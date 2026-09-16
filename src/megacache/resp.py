"""RESP2 server and Redis-compatible command subset."""

import hmac
import json
import logging
import socketserver
from typing import Any, List, Optional, Sequence, Tuple

from .config import Config
from .engine import CacheEngine, CacheResult

LOG = logging.getLogger("megacache.resp")


class RespCommandError(Exception):
    pass


class RespProtocolError(Exception):
    pass


class _SimpleString(str):
    pass


class MegaCacheRespServer(socketserver.ThreadingTCPServer):
    allow_reuse_address = True
    daemon_threads = True

    def __init__(self, address: tuple, config: Config, engine: CacheEngine):
        self.config = config
        self.engine = engine
        super().__init__(address, MegaCacheRespHandler)


class MegaCacheRespHandler(socketserver.StreamRequestHandler):
    server: MegaCacheRespServer

    def setup(self) -> None:
        super().setup()
        self.request.settimeout(30)
        self.authenticated = self.server.config.api_key is None

    def handle(self) -> None:
        while True:
            try:
                request = self._read_request()
                if request is None:
                    return
                response, close = self._execute(request)
                self.wfile.write(self._encode(response))
                self.wfile.flush()
                if close:
                    return
            except RespCommandError as exc:
                self.wfile.write(self._error(str(exc)))
                self.wfile.flush()
            except RespProtocolError as exc:
                self.wfile.write(self._error("Protocol error: {}".format(exc)))
                self.wfile.flush()
                return
            except (ConnectionError, TimeoutError):
                return
            except Exception:
                LOG.exception("unexpected RESP command failure")
                self.wfile.write(self._error("internal server error"))
                self.wfile.flush()

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
        if not self.authenticated:
            raise RespCommandError("NOAUTH Authentication required.")

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

    def _auth(self, args: Sequence[bytes]) -> _SimpleString:
        if len(args) == 2:
            supplied = args[1]
        elif len(args) == 3 and args[1] == b"default":
            supplied = args[2]
        else:
            raise RespCommandError(
                "wrong number of arguments for 'auth' command"
            )
        expected = self.server.config.api_key
        if expected is None:
            raise RespCommandError(
                "AUTH called without any password configured"
            )
        if not hmac.compare_digest(supplied, expected.encode("utf-8")):
            raise RespCommandError("WRONGPASS invalid username-password pair")
        self.authenticated = True
        return _SimpleString("OK")

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
        result = self.server.engine.get(self._key(args[1]))
        return None if result.state == "miss" else self._value_bytes(result.value)

    def _set(self, args: Sequence[bytes]) -> _SimpleString:
        if len(args) not in (3, 5):
            raise RespCommandError("syntax error")
        ttl = None
        persistent = True
        if len(args) == 5:
            if args[3].upper() != b"EX":
                raise RespCommandError("only the EX expiration option is supported")
            ttl = self._positive_arg(args[4], "expire time")
            persistent = False
        self.server.engine.put(
            self._key(args[1]),
            args[2],
            ttl_seconds=ttl,
            stale_seconds=0 if ttl is not None else None,
            persistent=persistent,
        )
        return _SimpleString("OK")

    def _mget(self, args: Sequence[bytes]) -> List[Optional[bytes]]:
        if len(args) < 2:
            raise RespCommandError("wrong number of arguments for 'mget' command")
        results = self.server.engine.mget(self._keys(args[1:]))
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
        self.server.engine.mset(pairs)
        return _SimpleString("OK")

    def _delete(self, args: Sequence[bytes]) -> int:
        if len(args) < 2:
            raise RespCommandError("wrong number of arguments for 'del' command")
        return self.server.engine.delete_many(self._keys(args[1:]))

    def _exists(self, args: Sequence[bytes]) -> int:
        if len(args) < 2:
            raise RespCommandError(
                "wrong number of arguments for 'exists' command"
            )
        return self.server.engine.exists(self._keys(args[1:]))

    def _expire(self, args: Sequence[bytes]) -> int:
        self._require_arity(args, 3)
        ttl = self._positive_arg(args[2], "expire time")
        return int(self.server.engine.expire(self._key(args[1]), ttl))

    def _ttl(self, args: Sequence[bytes]) -> int:
        self._require_arity(args, 2)
        return self.server.engine.ttl(self._key(args[1]))

    def _dbsize(self, args: Sequence[bytes]) -> int:
        self._require_arity(args, 1)
        return self.server.engine.size()

    def _flushdb(self, args: Sequence[bytes]) -> _SimpleString:
        self._require_arity(args, 1)
        self.server.engine.flush()
        return _SimpleString("OK")

    def _info(self, args: Sequence[bytes]) -> bytes:
        if len(args) > 2:
            raise RespCommandError("wrong number of arguments for 'info' command")
        stats = self.server.engine.stats()
        lines = [
            "# Server",
            "redis_version:7.2.0",
            "megacache_version:0.3.0",
            "redis_mode:standalone",
            "# Keyspace",
            "db0:keys={}".format(self.server.engine.size()),
            "# MegaCache",
        ]
        lines.extend(
            "{}:{}".format(name, value) for name, value in sorted(stats.items())
        )
        return ("\r\n".join(lines) + "\r\n").encode("utf-8")

    def _select(self, args: Sequence[bytes]) -> _SimpleString:
        self._require_arity(args, 2)
        if args[1] != b"0":
            raise RespCommandError("DB index is out of range")
        return _SimpleString("OK")

    def _client(self, args: Sequence[bytes]) -> Any:
        if len(args) >= 2 and args[1].upper() == b"SETINFO":
            return _SimpleString("OK")
        if len(args) == 2 and args[1].upper() == b"GETNAME":
            return None
        raise RespCommandError("unsupported CLIENT subcommand")

    def _command(self, args: Sequence[bytes]) -> List[Any]:
        return []

    def _hello(self, args: Sequence[bytes]) -> List[Any]:
        if len(args) != 2 or args[1] != b"2":
            raise RespCommandError("only RESP2 is supported")
        return [
            b"server",
            b"megacache",
            b"version",
            b"0.3.0",
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
        self.server.engine.put(
            self._key(args[1]),
            args[2],
            ttl_seconds=ttl,
            stale_seconds=stale,
            tags=tags,
            lease_token=lease_token,
        )
        return _SimpleString("OK")

    def _mc_lease(self, args: Sequence[bytes]) -> List[Any]:
        self._require_arity(args, 2)
        result = self.server.engine.acquire_lease(self._key(args[1]))
        values: List[Any] = [result.state.encode("ascii")]
        if result.state in ("fresh", "stale"):
            values.append(self._value_bytes(result.value))
        elif result.state == "stale_lease":
            values.extend(
                [
                    self._value_bytes(result.value),
                    result.lease_token.encode("ascii"),
                ]
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
        try:
            tags = [value.decode("utf-8") for value in args[1:]]
        except UnicodeDecodeError as exc:
            raise RespCommandError("tags must be valid UTF-8") from exc
        return self.server.engine.invalidate_tags(tags)

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
