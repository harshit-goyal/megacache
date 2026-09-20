"""RESP2 server and Redis-compatible command subset."""

import json
import logging
import math
import secrets
import socket
import socketserver
import time
from typing import Any, Dict, Iterable, List, Mapping, Optional, Sequence, Tuple

from .auth import AuthManager, Principal
from .config import Config
from .controlplane import (
    ControlPlaneCapacity,
    ControlPlaneError,
    ManagedControlPlane,
    TenantNotFound,
    TenantQuotaExceeded,
    TenantUnavailable,
)
from .engine import CacheResult
from .events import EventBackpressure, EventError, EventIngestionDisabled
from .origin import OriginError, OriginOverloaded
from .observability import valid_traceparent
from .storage import StorageBackend
from .transport import TLSRequestMixin

LOG = logging.getLogger("megacache.resp")
_PROCESS_EPOCH = secrets.token_urlsafe(18)


def _duration_milliseconds(value: float) -> int:
    return -1 if not math.isfinite(value) else max(0, int(value * 1000))


class RespCommandError(Exception):
    pass


class RespProtocolError(Exception):
    pass


class _SimpleString(str):
    pass


class MegaCacheRespServer(TLSRequestMixin, socketserver.ThreadingTCPServer):
    allow_reuse_address = True
    daemon_threads = False

    def __init__(self, address: tuple, config: Config, engine: StorageBackend):
        self.initialize_transport()
        self.config = config
        self.engine = engine
        self.invalidation_epoch = _PROCESS_EPOCH
        self.control_plane = (
            engine if isinstance(engine, ManagedControlPlane) else None
        )
        self.auth = AuthManager(
            config.api_key,
            config.users_file,
            default_tenant=(
                "default"
                if self.control_plane is None
                else self.control_plane.default_tenant
            ),
            allowed_tenants=(
                None
                if self.control_plane is None
                else self.control_plane.tenant_ids
            ),
        )
        super().__init__(address, MegaCacheRespHandler)

    def engine_for(self, principal: Principal) -> StorageBackend:
        if self.control_plane is None:
            return self.engine
        return self.control_plane.engine_for(principal)

class MegaCacheRespHandler(socketserver.StreamRequestHandler):
    server: MegaCacheRespServer

    def setup(self) -> None:
        super().setup()
        self.request.settimeout(30)
        self.principal = self.server.auth.anonymous()
        self.traceparent = None
        self._command_traceparent = None
        self._connection_tenant = None
        self._operation_tenant = None
        self._request_bytes = 0
        self._request_engine = None
        if self.principal is not None:
            try:
                self._bind_tenant(self.principal.tenant_id)
            except (TenantQuotaExceeded, TenantUnavailable):
                self.principal = None

    def finish(self) -> None:
        try:
            super().finish()
        finally:
            control = self.server.control_plane
            if control is not None and self._operation_tenant is not None:
                control.end_operation(self._operation_tenant)
                self._operation_tenant = None
            if control is not None and self._connection_tenant is not None:
                control.release_connection(self._connection_tenant)
                self._connection_tenant = None

    def handle(self) -> None:
        while True:
            started = None
            operation = "PROTOCOL"
            try:
                request = self._read_request()
                if request is None:
                    return
                self._request_bytes = sum(len(value) for value in request)
                self._request_engine = None
                self._operation_tenant = None
                started = time.perf_counter()
                operation = self._operation_label(request)
                response, close = self._execute(request)
                encoded = self._encode(response)
                self.wfile.write(encoded)
                self.wfile.flush()
                self._observe(operation, started, True, len(encoded))
                if close or self.server.is_draining:
                    return
            except RespCommandError as exc:
                encoded = self._error(str(exc))
                self.wfile.write(encoded)
                self.wfile.flush()
                if started is not None:
                    self._observe(operation, started, False, len(encoded))
            except RespProtocolError as exc:
                encoded = self._error("Protocol error: {}".format(exc))
                self.wfile.write(encoded)
                self.wfile.flush()
                if started is not None:
                    self._observe(operation, started, False, len(encoded))
                return
            except (ConnectionError, socket.timeout, TimeoutError):
                return
            except Exception:
                LOG.exception("unexpected RESP command failure")
                encoded = self._error("internal server error")
                self.wfile.write(encoded)
                self.wfile.flush()
                if started is not None:
                    self._observe(operation, started, False, len(encoded))
                return
            finally:
                if (
                    self.server.control_plane is not None
                    and self._operation_tenant is not None
                ):
                    self.server.control_plane.end_operation(
                        self._operation_tenant
                    )
                    self._operation_tenant = None
                self._command_traceparent = None

    def _observe(
        self,
        operation: str,
        started: float,
        success: bool,
        response_bytes: int,
    ) -> None:
        duration = time.perf_counter() - started
        engine = self._request_engine or self.server.engine
        engine.observe_request(
            "resp", operation, duration, success
        )
        control = self.server.control_plane
        tenant_id = self._operation_tenant or (
            None if self.principal is None else self.principal.tenant_id
        )
        if control is not None and tenant_id is not None:
            control.observe_tenant_request(
                tenant_id,
                "resp",
                operation,
                self._request_bytes,
                response_bytes,
                success,
                duration,
            )
        LOG.info(
            "command",
            extra={
                "protocol": "resp",
                "operation": operation,
                "status": "success" if success else "error",
                "duration_ms": round(duration * 1000, 3),
                "remote": self.client_address[0],
                "username": (
                    None if self.principal is None else self.principal.username
                ),
                "tenant_namespace": (
                    None
                    if control is None or tenant_id is None
                    else control.namespace_for(tenant_id)
                ),
                "traceparent": (
                    self._command_traceparent
                    if self._command_traceparent is not None
                    else self.traceparent
                ),
            },
        )

    @staticmethod
    def _operation_label(request: Sequence[bytes]) -> str:
        try:
            command = request[0].decode("ascii").upper()
        except UnicodeDecodeError:
            return "UNKNOWN"
        supported = {
            "AUTH",
            "PING",
            "ECHO",
            "GET",
            "SET",
            "MGET",
            "MSET",
            "DEL",
            "EXISTS",
            "EXPIRE",
            "TTL",
            "DBSIZE",
            "FLUSHDB",
            "INFO",
            "SELECT",
            "CLIENT",
            "COMMAND",
            "HELLO",
            "QUIT",
            "MC.SET",
            "MC.LEASE",
            "MC.INVALIDATE",
            "MC.TOPOLOGY",
            "MC.STATUS",
            "MC.FETCH",
            "MC.ORIGINS",
            "MC.EVENT",
            "MC.EVENT.STATUS",
            "MC.EVENT.RETRY",
            "MC.INVALIDATIONS",
            "MC.TRACEPARENT",
            "MC.EXPLAIN",
            "MC.RECOMMENDATIONS",
            "MC.POLICY.SIMULATE",
            "MC.EXPERIMENTS",
            "MC.IDENTITY",
            "MC.CONTROL.STATUS",
            "MC.CONTROL.TENANTS",
            "MC.CONTROL.OPERATION",
            "MC.CONTROL.ORCHESTRATOR",
            "MC.CONTROL.DEPLOYMENT",
            "MC.CONTROL.OBSERVE",
            "MC.BACKUP",
            "MC.DATA.EXPORT",
            "MC.RESTORE.VALIDATE",
            "MC.RESTORE",
            "MC.DR.DRILL",
            "MC.TENANT.DELETE.CHALLENGE",
            "MC.TENANT.DELETE",
            "MC.AUDIT",
            "MC.AUDIT.EXPORT",
            "MC.AUDIT.PRUNE",
            "MC.BILLING.EXPORT",
        }
        return command if command in supported else "UNKNOWN"

    def _read_request(self) -> Optional[List[bytes]]:
        first = self.rfile.read(1)
        if not first:
            return None
        if first != b"*":
            line = first + self._readline()
            parts = line.strip().split()
            if not parts:
                raise RespProtocolError("empty inline command")
            return parts

        count = self._parse_integer(self._readline(), "array length")
        if count <= 0 or count > 1024:
            raise RespProtocolError("command must contain 1 to 1024 arguments")
        parts = []
        total_length = 0
        for _ in range(count):
            if self.rfile.read(1) != b"$":
                raise RespProtocolError("command arguments must be bulk strings")
            length = self._parse_integer(self._readline(), "bulk string length")
            if length < 0 or length > self.server.config.max_body_bytes:
                raise RespProtocolError("bulk string exceeds configured limit")
            total_length += length
            if total_length > self.server.config.max_body_bytes:
                raise RespProtocolError("command exceeds configured limit")
            value = self.rfile.read(length)
            if len(value) != length or self.rfile.read(2) != b"\r\n":
                raise RespProtocolError("incomplete bulk string")
            parts.append(value)
        return parts

    def _readline(self) -> bytes:
        line = self.rfile.readline(65_537)
        if len(line) > 65_536 or not line.endswith(b"\r\n"):
            raise RespProtocolError("unterminated or oversized line")
        return line[:-2]

    @staticmethod
    def _parse_integer(value: bytes, field: str) -> int:
        try:
            return int(value)
        except ValueError as exc:
            raise RespProtocolError("invalid {}".format(field)) from exc

    def _execute(self, args: Sequence[bytes]) -> Tuple[Any, bool]:
        try:
            command = args[0].decode("ascii").upper()
        except UnicodeDecodeError as exc:
            raise RespCommandError("unknown command") from exc

        if command == "AUTH":
            return self._auth(args), False
        if self.principal is None:
            raise RespCommandError("NOAUTH Authentication required.")
        try:
            self._admit_operation(
                control_operation=command
                in {
                    "MC.IDENTITY",
                    "MC.CONTROL.STATUS",
                    "MC.CONTROL.TENANTS",
                    "MC.CONTROL.OPERATION",
                    "MC.CONTROL.ORCHESTRATOR",
                    "MC.CONTROL.DEPLOYMENT",
                    "MC.CONTROL.OBSERVE",
                    "MC.BACKUP",
                    "MC.DATA.EXPORT",
                    "MC.RESTORE.VALIDATE",
                    "MC.RESTORE",
                    "MC.DR.DRILL",
                    "MC.TENANT.DELETE.CHALLENGE",
                    "MC.TENANT.DELETE",
                    "MC.AUDIT",
                    "MC.AUDIT.EXPORT",
                    "MC.AUDIT.PRUNE",
                    "MC.BILLING.EXPORT",
                }
            )
        except TenantQuotaExceeded as exc:
            raise RespCommandError("BUSY {}".format(exc)) from exc
        except TenantUnavailable as exc:
            raise RespCommandError("TENANTUNAVAILABLE {}".format(exc)) from exc

        handlers = {
            "PING": self._ping,
            "ECHO": self._echo,
            "GET": self._get,
            "SET": self._set,
            "MGET": self._mget,
            "MSET": self._mset,
            "DEL": self._delete,
            "EXISTS": self._exists,
            "EXPIRE": self._expire,
            "TTL": self._ttl,
            "DBSIZE": self._dbsize,
            "FLUSHDB": self._flushdb,
            "INFO": self._info,
            "SELECT": self._select,
            "CLIENT": self._client,
            "COMMAND": self._command,
            "HELLO": self._hello,
            "MC.SET": self._mc_set,
            "MC.LEASE": self._mc_lease,
            "MC.INVALIDATE": self._mc_invalidate,
            "MC.TOPOLOGY": self._mc_topology,
            "MC.STATUS": self._mc_status,
            "MC.FETCH": self._mc_fetch,
            "MC.ORIGINS": self._mc_origins,
            "MC.EVENT": self._mc_event,
            "MC.EVENT.STATUS": self._mc_event_status,
            "MC.EVENT.RETRY": self._mc_event_retry,
            "MC.INVALIDATIONS": self._mc_invalidations,
            "MC.TRACEPARENT": self._mc_traceparent,
            "MC.EXPLAIN": self._mc_explain,
            "MC.RECOMMENDATIONS": self._mc_recommendations,
            "MC.POLICY.SIMULATE": self._mc_policy_simulate,
            "MC.EXPERIMENTS": self._mc_experiments,
            "MC.IDENTITY": self._mc_identity,
            "MC.CONTROL.STATUS": self._mc_control_status,
            "MC.CONTROL.TENANTS": self._mc_control_tenants,
            "MC.CONTROL.OPERATION": self._mc_control_operation,
            "MC.CONTROL.ORCHESTRATOR": self._mc_control_orchestrator,
            "MC.CONTROL.DEPLOYMENT": self._mc_control_deployment,
            "MC.CONTROL.OBSERVE": self._mc_control_observe,
            "MC.BACKUP": self._mc_backup,
            "MC.DATA.EXPORT": self._mc_data_export,
            "MC.RESTORE.VALIDATE": self._mc_restore_validate,
            "MC.RESTORE": self._mc_restore,
            "MC.DR.DRILL": self._mc_dr_drill,
            "MC.TENANT.DELETE.CHALLENGE": (
                self._mc_tenant_delete_challenge
            ),
            "MC.TENANT.DELETE": self._mc_tenant_delete,
            "MC.AUDIT": self._mc_audit,
            "MC.AUDIT.EXPORT": self._mc_audit_export,
            "MC.AUDIT.PRUNE": self._mc_audit_prune,
            "MC.BILLING.EXPORT": self._mc_billing_export,
        }
        if command == "QUIT":
            self._require_arity(args, 1)
            return _SimpleString("OK"), True
        handler = handlers.get(command)
        if handler is None:
            raise RespCommandError(
                "unknown command '{}', with args beginning with:".format(
                    command.lower()
                )
            )
        try:
            return handler(args), False
        except ValueError as exc:
            raise RespCommandError(str(exc)) from exc
        except OSError as exc:
            raise RespCommandError("control-plane state is unavailable") from exc

    def _auth(self, args: Sequence[bytes]) -> _SimpleString:
        if len(args) == 2:
            username = None
            password = args[1]
        elif len(args) == 3:
            username = args[1]
            password = args[2]
        else:
            raise RespCommandError(
                "wrong number of arguments for 'auth' command"
            )
        if not self.server.auth.required:
            raise RespCommandError(
                "AUTH called without any password configured"
            )
        try:
            decoded_username = (
                None if username is None else username.decode("utf-8")
            )
            decoded_password = password.decode("utf-8")
        except UnicodeDecodeError as exc:
            raise RespCommandError(
                "WRONGPASS invalid username-password pair"
            ) from exc
        principal = self.server.auth.authenticate(
            decoded_username, decoded_password
        )
        if principal is None:
            raise RespCommandError("WRONGPASS invalid username-password pair")
        try:
            self._bind_tenant(principal.tenant_id)
        except TenantQuotaExceeded as exc:
            raise RespCommandError("BUSY {}".format(exc)) from exc
        except TenantUnavailable as exc:
            raise RespCommandError("TENANTUNAVAILABLE {}".format(exc)) from exc
        self.principal = principal
        return _SimpleString("OK")

    def _engine(self) -> StorageBackend:
        if self._request_engine is not None:
            return self._request_engine
        if self.principal is None:
            raise RespCommandError("NOAUTH Authentication required.")
        try:
            self._request_engine = self.server.engine_for(self.principal)
        except TenantUnavailable as exc:
            raise RespCommandError("TENANTUNAVAILABLE {}".format(exc)) from exc
        return self._request_engine

    def _bind_tenant(self, tenant_id: str) -> None:
        control = self.server.control_plane
        if control is None:
            return
        if self._connection_tenant is not None:
            if self._connection_tenant != tenant_id:
                raise TenantUnavailable(
                    "one RESP connection cannot switch tenant identity"
                )
            return
        control.acquire_connection(tenant_id)
        self._connection_tenant = tenant_id

    def _admit_operation(self, *, control_operation: bool = False) -> None:
        control = self.server.control_plane
        if control is None or self._operation_tenant is not None:
            return
        assert self.principal is not None
        control.begin_operation(
            self.principal.tenant_id, control=control_operation
        )
        self._operation_tenant = self.principal.tenant_id

    def _ping(self, args: Sequence[bytes]) -> Any:
        if len(args) == 1:
            return _SimpleString("PONG")
        self._require_arity(args, 2)
        return args[1]

    def _echo(self, args: Sequence[bytes]) -> bytes:
        self._require_arity(args, 2)
        return args[1]

    def _get(self, args: Sequence[bytes]) -> Optional[bytes]:
        self._require_arity(args, 2)
        key = self._key(args[1])
        self._authorize("read", (key,))
        result = self._engine().get(key)
        return None if result.state == "miss" else self._value_bytes(result.value)

    def _set(self, args: Sequence[bytes]) -> _SimpleString:
        if len(args) not in (3, 5):
            raise RespCommandError("syntax error")
        key = self._key(args[1])
        self._authorize("write", (key,))
        ttl = None
        persistent = True
        if len(args) == 5:
            if args[3].upper() != b"EX":
                raise RespCommandError("only the EX expiration option is supported")
            ttl = self._positive_arg(args[4], "expire time")
            persistent = False
        self._engine().put(
            key,
            args[2],
            ttl_seconds=ttl,
            stale_seconds=0 if ttl is not None else None,
            persistent=persistent,
        )
        return _SimpleString("OK")

    def _mget(self, args: Sequence[bytes]) -> List[Optional[bytes]]:
        if len(args) < 2:
            raise RespCommandError("wrong number of arguments for 'mget' command")
        keys = self._keys(args[1:])
        self._authorize("read", keys)
        results = self._engine().mget(keys)
        return [
            None if result.state == "miss" else self._value_bytes(result.value)
            for result in results
        ]

    def _mset(self, args: Sequence[bytes]) -> _SimpleString:
        if len(args) < 3 or len(args) % 2 == 0:
            raise RespCommandError("wrong number of arguments for 'mset' command")
        pairs = [
            (self._key(args[index]), args[index + 1])
            for index in range(1, len(args), 2)
        ]
        self._authorize("write", (key for key, _ in pairs))
        self._engine().mset(pairs)
        return _SimpleString("OK")

    def _delete(self, args: Sequence[bytes]) -> int:
        if len(args) < 2:
            raise RespCommandError("wrong number of arguments for 'del' command")
        keys = self._keys(args[1:])
        self._authorize("write", keys)
        return self._engine().delete_many(keys)

    def _exists(self, args: Sequence[bytes]) -> int:
        if len(args) < 2:
            raise RespCommandError(
                "wrong number of arguments for 'exists' command"
            )
        keys = self._keys(args[1:])
        self._authorize("read", keys)
        return self._engine().exists(keys)

    def _expire(self, args: Sequence[bytes]) -> int:
        self._require_arity(args, 3)
        key = self._key(args[1])
        self._authorize("write", (key,))
        ttl = self._positive_arg(args[2], "expire time")
        return int(self._engine().expire(key, ttl))

    def _ttl(self, args: Sequence[bytes]) -> int:
        self._require_arity(args, 2)
        key = self._key(args[1])
        self._authorize("read", (key,))
        return self._engine().ttl(key)

    def _dbsize(self, args: Sequence[bytes]) -> int:
        self._require_arity(args, 1)
        self._authorize("admin")
        return self._engine().size()

    def _flushdb(self, args: Sequence[bytes]) -> _SimpleString:
        self._require_arity(args, 1)
        self._authorize("admin")
        self._engine().flush()
        return _SimpleString("OK")

    def _info(self, args: Sequence[bytes]) -> bytes:
        if len(args) > 2:
            raise RespCommandError("wrong number of arguments for 'info' command")
        self._authorize("admin")
        stats = self._engine().stats()
        lines = [
            "# Server",
            "redis_version:7.2.0",
            "megacache_version:1.0.0",
            "redis_mode:standalone",
            "# Keyspace",
            "db0:keys={}".format(self._engine().size()),
            "# MegaCache",
        ]
        lines.extend(
            "{}:{}".format(name, value) for name, value in sorted(stats.items())
        )
        return ("\r\n".join(lines) + "\r\n").encode("utf-8")

    def _select(self, args: Sequence[bytes]) -> _SimpleString:
        self._require_arity(args, 2)
        self._authorize("read")
        if args[1] != b"0":
            raise RespCommandError("DB index is out of range")
        return _SimpleString("OK")

    def _client(self, args: Sequence[bytes]) -> Any:
        self._authorize("read")
        if len(args) >= 2 and args[1].upper() == b"SETINFO":
            return _SimpleString("OK")
        if len(args) == 2 and args[1].upper() == b"GETNAME":
            return None
        raise RespCommandError("unsupported CLIENT subcommand")

    def _command(self, args: Sequence[bytes]) -> List[Any]:
        self._authorize("read")
        return []

    def _hello(self, args: Sequence[bytes]) -> List[Any]:
        self._authorize("read")
        if len(args) != 2 or args[1] != b"2":
            raise RespCommandError("only RESP2 is supported")
        return [
            b"server",
            b"megacache",
            b"version",
            b"1.0.0",
            b"proto",
            2,
            b"mode",
            b"standalone",
        ]

    def _mc_set(self, args: Sequence[bytes]) -> _SimpleString:
        if len(args) < 3:
            raise RespCommandError(
                "wrong number of arguments for 'mc.set' command"
            )
        key = self._key(args[1])
        self._authorize("write", (key,))
        ttl = None
        stale = None
        lease_token = None
        tags: List[str] = []
        index = 3
        while index < len(args):
            option = args[index].upper()
            if option in (b"TTL", b"STALE"):
                if index + 1 >= len(args):
                    raise RespCommandError("syntax error")
                value = self._non_negative_arg(args[index + 1], "duration")
                if option == b"TTL":
                    if value == 0:
                        raise RespCommandError("TTL must be greater than zero")
                    ttl = value
                else:
                    stale = value
                index += 2
            elif option == b"TAGS":
                if index + 1 >= len(args):
                    raise RespCommandError("syntax error")
                count = self._non_negative_arg(args[index + 1], "tag count")
                end = index + 2 + count
                if end > len(args):
                    raise RespCommandError("tag count exceeds supplied tags")
                tags.extend(
                    value.decode("utf-8") for value in args[index + 2 : end]
                )
                index = end
            elif option == b"LEASE":
                if index + 1 >= len(args):
                    raise RespCommandError("syntax error")
                try:
                    lease_token = args[index + 1].decode("ascii")
                except UnicodeDecodeError as exc:
                    raise RespCommandError("lease token must be ASCII") from exc
                index += 2
            else:
                raise RespCommandError("unknown MC.SET option")
        self._engine().put(
            key,
            args[2],
            ttl_seconds=ttl,
            stale_seconds=stale,
            tags=tags,
            lease_token=lease_token,
        )
        return _SimpleString("OK")

    def _mc_lease(self, args: Sequence[bytes]) -> List[Any]:
        if len(args) not in (2, 3):
            raise RespCommandError(
                "wrong number of arguments for 'mc.lease' command"
            )
        windows = len(args) == 3 and args[2].upper() == b"WINDOWS"
        if len(args) == 3 and not windows:
            raise RespCommandError("unknown MC.LEASE option")
        key = self._key(args[1])
        self._authorize("read", (key,))
        self._authorize("write", (key,))
        result = self._engine().acquire_lease(key)
        values: List[Any] = [result.state.encode("ascii")]
        if result.state == "fresh":
            values.append(self._value_bytes(result.value))
            if windows:
                values.extend(
                    [
                        _duration_milliseconds(result.expires_in_seconds),
                        _duration_milliseconds(result.stale_for_seconds),
                    ]
                )
        elif result.state == "stale":
            values.append(self._value_bytes(result.value))
            if windows:
                values.append(
                    _duration_milliseconds(result.stale_for_seconds)
                )
        elif result.state == "stale_lease":
            values.extend(
                [
                    self._value_bytes(result.value),
                    result.lease_token.encode("ascii"),
                ]
            )
            if windows:
                values.append(
                    _duration_milliseconds(result.stale_for_seconds)
                )
        elif result.state == "lease":
            values.append(result.lease_token.encode("ascii"))
        elif result.state == "loading":
            values.append(int(result.retry_after_seconds * 1000))
        return values

    def _mc_invalidate(self, args: Sequence[bytes]) -> int:
        if len(args) < 2:
            raise RespCommandError(
                "wrong number of arguments for 'mc.invalidate' command"
            )
        self._authorize("invalidate")
        try:
            tags = [value.decode("utf-8") for value in args[1:]]
        except UnicodeDecodeError as exc:
            raise RespCommandError("tags must be valid UTF-8") from exc
        return self._engine().invalidate_tags(tags)

    def _mc_topology(self, args: Sequence[bytes]) -> bytes:
        if len(args) not in (1, 2):
            raise RespCommandError(
                "wrong number of arguments for 'mc.topology' command"
            )
        self._authorize("admin")
        if len(args) == 2:
            key = self._key(args[1])
            ownership = getattr(self._engine(), "ownership", None)
            if ownership is None:
                value = {
                    "key": key,
                    "ring_version": 0,
                    "primary": "standalone",
                    "replicas": ["standalone"],
                }
            else:
                value = ownership(key)
        else:
            topology = getattr(self._engine(), "topology", None)
            if topology is None:
                value = {
                    "mode": "standalone",
                    "degraded": False,
                    "nodes": [{"node_id": "standalone", "status": "active"}],
                }
            else:
                value = topology()
        return json.dumps(value, separators=(",", ":")).encode("utf-8")

    def _mc_status(self, args: Sequence[bytes]) -> bytes:
        self._require_arity(args, 1)
        self._authorize("admin")
        status = getattr(self._engine(), "status", None)
        value = (
            {
                "healthy_nodes": 1,
                "total_nodes": 1,
                "degraded": False,
                "known_keys": self._engine().size(),
            }
            if status is None
            else status()
        )
        return json.dumps(value, separators=(",", ":")).encode("utf-8")

    def _mc_invalidations(self, args: Sequence[bytes]) -> bytes:
        if len(args) not in (1, 2):
            raise RespCommandError(
                "wrong number of arguments for 'mc.invalidations' command"
            )
        self._authorize("read")
        supplied = None
        if len(args) == 2:
            if len(args[1]) > 256:
                raise RespCommandError("invalidation cursor is too long")
            try:
                supplied = args[1].decode("ascii")
            except UnicodeDecodeError as exc:
                raise RespCommandError(
                    "invalidation cursor must be ASCII"
                ) from exc
        cursor_method = getattr(
            self._engine(), "invalidation_cursor", None
        )
        generation = 0 if cursor_method is None else int(cursor_method())
        epoch = self.server.invalidation_epoch
        cursor = "{}:{}".format(epoch, generation)
        return json.dumps(
            {
                "cursor": cursor,
                "epoch": epoch,
                "generation": generation,
                "changed": supplied is not None and cursor != supplied,
            },
            separators=(",", ":"),
        ).encode("utf-8")

    def _mc_traceparent(self, args: Sequence[bytes]) -> _SimpleString:
        self._require_arity(args, 2)
        self._authorize("read")
        try:
            value = args[1].decode("ascii").lower()
        except UnicodeDecodeError as exc:
            raise RespCommandError("traceparent must be ASCII") from exc
        if not valid_traceparent(value):
            raise RespCommandError("invalid W3C traceparent")
        self.traceparent = value
        return _SimpleString("OK")

    def _mc_fetch(self, args: Sequence[bytes]) -> bytes:
        if len(args) < 4:
            raise RespCommandError(
                "wrong number of arguments for 'mc.fetch' command"
            )
        key = self._key(args[1])
        self._authorize("read", (key,))
        self._authorize("write", (key,))
        fetch = getattr(self._engine(), "fetch", None)
        if fetch is None:
            raise RespCommandError(
                "HTTP origins are not configured on this server"
            )
        try:
            origin = args[2].decode("utf-8")
            path = args[3].decode("utf-8")
        except UnicodeDecodeError as exc:
            raise RespCommandError(
                "origin name and path must be valid UTF-8"
            ) from exc
        force_refresh = False
        traceparent = self.traceparent
        index = 4
        while index < len(args):
            option = args[index].upper()
            if option == b"REFRESH":
                force_refresh = True
                index += 1
            elif option == b"TRACEPARENT" and index + 1 < len(args):
                try:
                    candidate = args[index + 1].decode("ascii").lower()
                except UnicodeDecodeError as exc:
                    raise RespCommandError("traceparent must be ASCII") from exc
                if not valid_traceparent(candidate):
                    raise RespCommandError("invalid W3C traceparent")
                traceparent = candidate
                index += 2
            else:
                raise RespCommandError("unknown MC.FETCH option")
        self._command_traceparent = traceparent
        try:
            result = fetch(
                key,
                origin,
                path,
                force_refresh=force_refresh,
                traceparent=traceparent,
            )
        except OriginOverloaded as exc:
            raise RespCommandError("BUSY {}".format(exc)) from exc
        except OriginError as exc:
            raise RespCommandError(str(exc)) from exc
        return json.dumps(
            result.as_json(), separators=(",", ":")
        ).encode("utf-8")

    def _mc_origins(self, args: Sequence[bytes]) -> bytes:
        self._require_arity(args, 1)
        self._authorize("admin")
        origins = getattr(self._engine(), "origins", None)
        value = {} if origins is None else origins()
        return json.dumps(value, separators=(",", ":")).encode("utf-8")

    def _mc_explain(self, args: Sequence[bytes]) -> bytes:
        self._require_arity(args, 2)
        key = self._key(args[1])
        self._authorize("read", (key,))
        explain = getattr(self._engine(), "explain", None)
        if explain is None:
            raise RespCommandError("cache intelligence is not available")
        return json.dumps(
            explain(key), separators=(",", ":")
        ).encode("utf-8")

    def _mc_recommendations(self, args: Sequence[bytes]) -> bytes:
        if len(args) not in (1, 2):
            raise RespCommandError(
                "wrong number of arguments for 'mc.recommendations' command"
            )
        self._authorize("admin")
        limit = (
            100
            if len(args) == 1
            else self._positive_arg(args[1], "recommendation limit")
        )
        recommend = getattr(self._engine(), "recommendations", None)
        if recommend is None:
            raise RespCommandError("cache intelligence is not available")
        principal = self.principal
        assert principal is not None
        return json.dumps(
            recommend(
                limit,
                key_filter=lambda key: principal.allows("admin", (key,)),
            ),
            separators=(",", ":"),
        ).encode("utf-8")

    def _mc_policy_simulate(self, args: Sequence[bytes]) -> bytes:
        self._require_arity(args, 2)
        self._authorize("admin")
        try:
            document = json.loads(args[1].decode("utf-8"))
        except (UnicodeDecodeError, json.JSONDecodeError) as exc:
            raise RespCommandError("simulation input must be valid JSON") from exc
        if not isinstance(document, dict):
            raise RespCommandError("simulation input must be a JSON object")
        simulate = getattr(self._engine(), "simulate", None)
        if simulate is None:
            raise RespCommandError("cache intelligence is not available")
        return json.dumps(
            simulate(document), separators=(",", ":")
        ).encode("utf-8")

    def _mc_experiments(self, args: Sequence[bytes]) -> bytes:
        self._require_arity(args, 1)
        self._authorize("admin")
        status = getattr(self._engine(), "experiment_status", None)
        if status is None:
            raise RespCommandError("cache intelligence is not available")
        return json.dumps(
            status(), separators=(",", ":")
        ).encode("utf-8")

    def _mc_event(self, args: Sequence[bytes]) -> bytes:
        self._require_arity(args, 2)
        self._authorize("invalidate")
        ingest = getattr(self._engine(), "ingest_event", None)
        if ingest is None:
            raise RespCommandError("event ingestion is not configured")
        try:
            value = json.loads(args[1].decode("utf-8"))
        except (UnicodeDecodeError, json.JSONDecodeError) as exc:
            raise RespCommandError("event must be valid JSON") from exc
        if not isinstance(value, dict):
            raise RespCommandError("event must be a JSON object")
        try:
            result = ingest(value)
        except EventIngestionDisabled as exc:
            raise RespCommandError("events disabled: {}".format(exc)) from exc
        except EventBackpressure as exc:
            raise RespCommandError("BUSY {}".format(exc)) from exc
        except OSError as exc:
            raise RespCommandError("event state is unavailable") from exc
        except EventError as exc:
            raise RespCommandError(str(exc)) from exc
        return json.dumps(result, separators=(",", ":")).encode("utf-8")

    def _mc_event_status(self, args: Sequence[bytes]) -> bytes:
        self._require_arity(args, 1)
        self._authorize("admin")
        status = getattr(self._engine(), "event_status", None)
        if status is None:
            raise RespCommandError("event ingestion is not configured")
        try:
            value = status()
        except OSError as exc:
            raise RespCommandError("event state is unavailable") from exc
        return json.dumps(value, separators=(",", ":")).encode("utf-8")

    def _mc_event_retry(self, args: Sequence[bytes]) -> bytes:
        if len(args) not in (1, 2):
            raise RespCommandError(
                "wrong number of arguments for 'mc.event.retry' command"
            )
        self._authorize("admin")
        retry = getattr(self._engine(), "retry_events", None)
        if retry is None:
            raise RespCommandError("event ingestion is not configured")
        limit = (
            100
            if len(args) == 1
            else self._positive_arg(args[1], "retry limit")
        )
        try:
            value = retry(limit)
        except EventIngestionDisabled as exc:
            raise RespCommandError("events disabled: {}".format(exc)) from exc
        except EventBackpressure as exc:
            raise RespCommandError("BUSY {}".format(exc)) from exc
        except OSError as exc:
            raise RespCommandError("event state is unavailable") from exc
        except EventError as exc:
            raise RespCommandError(str(exc)) from exc
        return json.dumps(value, separators=(",", ":")).encode("utf-8")

    def _mc_identity(self, args: Sequence[bytes]) -> bytes:
        self._require_arity(args, 1)
        principal = self.principal
        assert principal is not None
        control = self._control_plane()
        return self._json_bytes(
            {
                "username": principal.username,
                "tenant_id": principal.tenant_id,
                "tenant_namespace": control.namespace_for(
                    principal.tenant_id
                ),
                "permissions": sorted(principal.permissions),
                "roles": sorted(principal.roles),
            }
        )

    def _mc_control_status(self, args: Sequence[bytes]) -> bytes:
        if len(args) not in (1, 2):
            raise RespCommandError(
                "wrong number of arguments for 'mc.control.status' command"
            )
        target = self._control_target(
            None if len(args) == 1 else self._text_arg(args[1], "tenant"),
            allow_operator=True,
        )
        return self._json_bytes(self._control_plane().tenant_status(target))

    def _mc_control_tenants(self, args: Sequence[bytes]) -> bytes:
        self._require_arity(args, 1)
        self._require_control_role("platform_admin", "operator")
        return self._json_bytes(self._control_plane().list_tenants())

    def _mc_control_operation(self, args: Sequence[bytes]) -> bytes:
        self._require_arity(args, 2)
        operation_id = self._text_arg(args[1], "operation id")
        principal = self.principal
        assert principal is not None
        control = self._control_plane()
        if principal.has_role("platform_admin") or principal.has_role(
            "operator"
        ):
            value = control.operation_status(operation_id)
        else:
            self._require_control_role("tenant_admin")
            value = control.operation_status(
                operation_id, principal.tenant_id
            )
        return self._json_bytes(value)

    def _mc_control_orchestrator(self, args: Sequence[bytes]) -> bytes:
        self._require_arity(args, 1)
        self._require_control_role("platform_admin", "operator")
        return self._json_bytes(self._control_plane().orchestrator_status())

    def _mc_control_deployment(self, args: Sequence[bytes]) -> bytes:
        self._require_arity(args, 3)
        self._require_control_role("platform_admin", "operator")
        tenant_id = self._control_target(
            self._text_arg(args[1], "tenant"), allow_operator=True
        )
        document = self._json_arg(args[2], "deployment")
        assert self.principal is not None
        return self._json_bytes(
            self._control_plane().set_desired_deployment(
                tenant_id, document, actor=self.principal.username
            )
        )

    def _mc_control_observe(self, args: Sequence[bytes]) -> bytes:
        self._require_arity(args, 3)
        self._require_control_role("platform_admin", "operator")
        tenant_id = self._control_target(
            self._text_arg(args[1], "tenant"), allow_operator=True
        )
        document = self._json_arg(args[2], "deployment observation")
        assert self.principal is not None
        return self._json_bytes(
            self._control_plane().report_observed_deployment(
                tenant_id, document, actor=self.principal.username
            )
        )

    def _mc_backup(self, args: Sequence[bytes]) -> bytes:
        if len(args) not in (1, 2):
            raise RespCommandError(
                "wrong number of arguments for 'mc.backup' command"
            )
        requested = None
        if len(args) > 1:
            candidate = self._text_arg(args[1], "tenant")
            requested = None if candidate == "-" else candidate
        tenant_id = self._control_target(requested)
        assert self.principal is not None
        return self._json_bytes(
            self._control_plane().request_backup(
                tenant_id, actor=self.principal.username
            )
        )

    def _mc_data_export(self, args: Sequence[bytes]) -> bytes:
        if len(args) not in (1, 2):
            raise RespCommandError(
                "wrong number of arguments for 'mc.data.export' command"
            )
        tenant_id = self._control_target(
            None if len(args) == 1 else self._text_arg(args[1], "tenant")
        )
        assert self.principal is not None
        return self._json_bytes(
            self._control_plane().request_data_export(
                tenant_id, actor=self.principal.username
            )
        )

    def _mc_restore_validate(self, args: Sequence[bytes]) -> bytes:
        if len(args) not in (2, 3):
            raise RespCommandError(
                "wrong number of arguments for 'mc.restore.validate' command"
            )
        if len(args) == 2:
            tenant = None
            backup = args[1]
        else:
            tenant = self._text_arg(args[1], "tenant")
            backup = args[2]
        tenant_id = self._control_target(tenant)
        assert self.principal is not None
        return self._json_bytes(
            self._control_plane().request_restore_validation(
                tenant_id,
                self._text_arg(backup, "backup id"),
                actor=self.principal.username,
            )
        )

    def _mc_restore(self, args: Sequence[bytes]) -> bytes:
        self._require_arity(args, 5)
        tenant_id = self._control_target(
            self._text_arg(args[1], "tenant")
        )
        assert self.principal is not None
        return self._json_bytes(
            self._control_plane().request_restore(
                tenant_id,
                self._text_arg(args[2], "backup id"),
                self._text_arg(args[3], "validation token"),
                self._text_arg(args[4], "tenant confirmation"),
                actor=self.principal.username,
            )
        )

    def _mc_dr_drill(self, args: Sequence[bytes]) -> bytes:
        if len(args) not in (2, 3):
            raise RespCommandError(
                "wrong number of arguments for 'mc.dr.drill' command"
            )
        if len(args) == 2:
            tenant = None
            backup = args[1]
        else:
            tenant = self._text_arg(args[1], "tenant")
            backup = args[2]
        tenant_id = self._control_target(tenant)
        assert self.principal is not None
        return self._json_bytes(
            self._control_plane().request_drill(
                tenant_id,
                self._text_arg(backup, "backup id"),
                actor=self.principal.username,
            )
        )

    def _mc_tenant_delete_challenge(
        self, args: Sequence[bytes]
    ) -> bytes:
        if len(args) not in (1, 2):
            raise RespCommandError(
                "wrong number of arguments for "
                "'mc.tenant.delete.challenge' command"
            )
        tenant_id = self._control_target(
            None if len(args) == 1 else self._text_arg(args[1], "tenant")
        )
        assert self.principal is not None
        return self._json_bytes(
            self._control_plane().create_deletion_challenge(
                tenant_id, actor=self.principal.username
            )
        )

    def _mc_tenant_delete(self, args: Sequence[bytes]) -> bytes:
        self._require_arity(args, 4)
        tenant_id = self._control_target(
            self._text_arg(args[1], "tenant")
        )
        assert self.principal is not None
        return self._json_bytes(
            self._control_plane().request_tenant_deletion(
                tenant_id,
                actor=self.principal.username,
                challenge=self._text_arg(args[2], "deletion challenge"),
                confirmation=self._text_arg(args[3], "tenant confirmation"),
            )
        )

    def _mc_audit(self, args: Sequence[bytes]) -> bytes:
        if len(args) > 4:
            raise RespCommandError(
                "wrong number of arguments for 'mc.audit' command"
            )
        principal = self.principal
        assert principal is not None
        control = self._control_plane()
        requested = None
        if len(args) >= 2:
            candidate = self._text_arg(args[1], "tenant")
            requested = None if candidate == "-" else candidate
        if principal.has_role("auditor") or principal.has_role(
            "platform_admin"
        ):
            tenant_id = (
                None
                if requested is None
                else control.resolve_tenant(principal, requested)
            )
        else:
            self._require_control_role("tenant_admin")
            tenant_id = control.resolve_tenant(principal, requested)
        after = (
            0
            if len(args) < 3
            else self._non_negative_arg(args[2], "after sequence")
        )
        limit = (
            1000
            if len(args) < 4
            else self._positive_arg(args[3], "audit limit")
        )
        return self._json_bytes(
            control.export_audit(
                tenant_id=tenant_id,
                after_sequence=after,
                limit=limit,
            )
        )

    def _mc_audit_export(self, args: Sequence[bytes]) -> bytes:
        if len(args) not in (1, 2, 3, 4):
            raise RespCommandError(
                "wrong number of arguments for 'mc.audit.export' command"
            )
        requested = None
        if len(args) > 1:
            candidate = self._text_arg(args[1], "tenant")
            requested = None if candidate == "-" else candidate
        principal = self.principal
        assert principal is not None
        if principal.has_role("auditor") or principal.has_role(
            "platform_admin"
        ):
            tenant_id = (
                principal.tenant_id
                if requested is None
                else self._control_plane().resolve_tenant(
                    principal, requested
                )
            )
        else:
            tenant_id = self._control_target(requested)
        after = (
            0
            if len(args) < 3
            else self._non_negative_arg(args[2], "after sequence")
        )
        limit = (
            1000
            if len(args) < 4
            else self._positive_arg(args[3], "audit limit")
        )
        return self._json_bytes(
            self._control_plane().request_audit_export(
                tenant_id,
                actor=principal.username,
                after_sequence=after,
                limit=limit,
            )
        )

    def _mc_billing_export(self, args: Sequence[bytes]) -> bytes:
        if len(args) > 4:
            raise RespCommandError(
                "wrong number of arguments for 'mc.billing.export' command"
            )
        self._require_control_role("billing_admin", "platform_admin")
        control = self._control_plane()
        tenant_id = None
        if len(args) >= 2:
            candidate = self._text_arg(args[1], "tenant")
            tenant_id = (
                None
                if candidate == "-"
                else control.resolve_tenant(self.principal, candidate)
            )
        start = (
            None
            if len(args) < 3 or args[2] == b"-"
            else self._text_arg(args[2], "start period")
        )
        end = (
            None
            if len(args) < 4 or args[3] == b"-"
            else self._text_arg(args[3], "end period")
        )
        assert self.principal is not None
        return self._json_bytes(
            control.billing_export(
                tenant_id=tenant_id,
                start_period=start,
                end_period=end,
                actor=self.principal.username,
            )
        )

    def _mc_audit_prune(self, args: Sequence[bytes]) -> bytes:
        self._require_arity(args, 3)
        self._require_control_role("auditor", "platform_admin")
        assert self.principal is not None
        return self._json_bytes(
            self._control_plane().prune_audit(
                through_sequence=self._positive_arg(
                    args[1], "audit prune sequence"
                ),
                expected_hash=self._text_arg(
                    args[2], "audit prune hash"
                ),
                actor=self.principal.username,
            )
        )

    def _control_plane(self) -> ManagedControlPlane:
        control = self.server.control_plane
        if control is None:
            raise RespCommandError("managed control plane is not configured")
        return control

    def _control_target(
        self, requested: Optional[str], allow_operator: bool = False
    ) -> str:
        principal = self.principal
        assert principal is not None
        control = self._control_plane()
        target = control.resolve_tenant(principal, requested)
        if principal.manages_tenant(target) or (
            allow_operator and principal.has_role("operator")
        ):
            return target
        raise RespCommandError(
            "NOPERM this user has no control permission for the tenant"
        )

    def _require_control_role(self, *roles: str) -> None:
        principal = self.principal
        assert principal is not None
        if not any(principal.has_role(role) for role in roles):
            raise RespCommandError(
                "NOPERM this user has no control permission"
            )

    @staticmethod
    def _text_arg(value: bytes, field: str) -> str:
        try:
            decoded = value.decode("utf-8")
        except UnicodeDecodeError as exc:
            raise RespCommandError(
                "{} must be valid UTF-8".format(field)
            ) from exc
        if not decoded:
            raise RespCommandError("{} must not be empty".format(field))
        return decoded

    @classmethod
    def _json_arg(cls, value: bytes, field: str) -> Dict[str, Any]:
        try:
            document = json.loads(value.decode("utf-8"))
        except (UnicodeDecodeError, json.JSONDecodeError) as exc:
            raise RespCommandError(
                "{} must be valid JSON".format(field)
            ) from exc
        if not isinstance(document, dict):
            raise RespCommandError("{} must be a JSON object".format(field))
        return document

    @staticmethod
    def _json_bytes(value: Mapping[str, Any]) -> bytes:
        return json.dumps(value, separators=(",", ":")).encode("utf-8")

    def _authorize(self, permission: str, keys: Iterable[str] = ()) -> None:
        assert self.principal is not None
        if not self.principal.allows(permission, keys):
            raise RespCommandError(
                "NOPERM this user has no permissions to run the command"
            )

    @staticmethod
    def _require_arity(args: Sequence[bytes], expected: int) -> None:
        if len(args) != expected:
            command = args[0].decode("ascii", "replace").lower()
            raise RespCommandError(
                "wrong number of arguments for '{}' command".format(command)
            )

    @staticmethod
    def _positive_arg(value: bytes, field: str) -> int:
        parsed = MegaCacheRespHandler._non_negative_arg(value, field)
        if parsed == 0:
            raise RespCommandError("{} must be greater than zero".format(field))
        return parsed

    @staticmethod
    def _non_negative_arg(value: bytes, field: str) -> int:
        try:
            parsed = int(value)
        except ValueError as exc:
            raise RespCommandError("{} is not an integer".format(field)) from exc
        if parsed < 0:
            raise RespCommandError("{} must not be negative".format(field))
        return parsed

    @staticmethod
    def _key(value: bytes) -> str:
        return value.decode("latin-1")

    @classmethod
    def _keys(cls, values: Sequence[bytes]) -> Tuple[str, ...]:
        return tuple(cls._key(value) for value in values)

    @staticmethod
    def _value_bytes(value: Any) -> bytes:
        if isinstance(value, bytes):
            return value
        if isinstance(value, str):
            return value.encode("utf-8")
        return json.dumps(value, separators=(",", ":")).encode("utf-8")

    @classmethod
    def _encode(cls, value: Any) -> bytes:
        if isinstance(value, _SimpleString):
            return b"+" + value.encode("utf-8") + b"\r\n"
        if value is None:
            return b"$-1\r\n"
        if isinstance(value, bytes):
            return b"$" + str(len(value)).encode("ascii") + b"\r\n" + value + b"\r\n"
        if isinstance(value, int):
            return b":" + str(value).encode("ascii") + b"\r\n"
        if isinstance(value, list):
            return (
                b"*"
                + str(len(value)).encode("ascii")
                + b"\r\n"
                + b"".join(cls._encode(item) for item in value)
            )
        raise TypeError("unsupported RESP response type")

    @staticmethod
    def _error(message: str) -> bytes:
        safe = message.replace("\r", " ").replace("\n", " ")
        prefix = b"-" if safe.startswith(("NOAUTH ", "WRONGPASS ")) else b"-ERR "
        return prefix + safe.encode("utf-8", "replace") + b"\r\n"
