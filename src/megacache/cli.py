"""Native MegaCache command-line interface."""

import argparse
import base64
import getpass
import json
import logging
import os
import signal
import sys
import threading
from dataclasses import replace
from typing import Any, List, Optional, Sequence

from .auth import hash_password, write_example_users_file
from .client import MegaCacheClient, MegaCacheClientError
from .cluster import ClusterNode, ClusterStorage
from .config import Config
from .controlplane import (
    ControlPlaneError,
    ManagedControlPlane,
    load_control_plane_definition,
    load_key_provider,
    tenant_namespace_id,
)
from .coordination import MutationClock
from .engine import CacheEngine
from .events import load_event_automation
from .intelligence import CacheIntelligence
from .observability import configure_logging
from .origin import OriginCache, load_origin_definitions
from .resp import MegaCacheRespServer
from .server import MegaCacheServer
from .transport import enable_server_tls, validate_tls_config


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="mc",
        description="Run and interact with MegaCache.",
    )
    parser.add_argument(
        "--version", action="version", version="MegaCache 1.0.0"
    )
    parser.add_argument(
        "--host",
        default=os.getenv("MEGACACHE_CLI_HOST", "127.0.0.1"),
        help="RESP server host (default: 127.0.0.1)",
    )
    parser.add_argument(
        "--port",
        type=int,
        default=int(os.getenv("MEGACACHE_CLI_PORT", "6380")),
        help="RESP server port (default: 6380)",
    )
    parser.add_argument(
        "--username",
        default=os.getenv("MEGACACHE_CLI_USERNAME"),
        help="named user; defaults to legacy API-key authentication",
    )
    parser.add_argument(
        "--password",
        default=os.getenv("MEGACACHE_CLI_PASSWORD")
        or os.getenv("MEGACACHE_API_KEY"),
        help="server password; prefer MEGACACHE_CLI_PASSWORD",
    )
    parser.add_argument(
        "--timeout",
        type=float,
        default=5,
        help="connection timeout in seconds (default: 5)",
    )
    parser.add_argument(
        "--tls",
        action="store_true",
        default=os.getenv("MEGACACHE_CLI_TLS", "").lower()
        in ("1", "true", "yes"),
        help="connect using TLS",
    )
    parser.add_argument(
        "--ca-file",
        default=os.getenv("MEGACACHE_CLI_CA_FILE"),
        help="CA certificate used to verify the server",
    )
    parser.add_argument(
        "--server-name",
        default=os.getenv("MEGACACHE_CLI_SERVER_NAME"),
        help="TLS server name override",
    )
    parser.add_argument(
        "--json",
        action="store_true",
        help="print machine-readable JSON",
    )
    commands = parser.add_subparsers(dest="command")

    serve = commands.add_parser("serve", help="start the MegaCache server")
    serve.add_argument("--http-host")
    serve.add_argument("--http-port", type=int)
    serve.add_argument("--resp-host")
    serve.add_argument("--resp-port", type=int)

    ping = commands.add_parser("ping", help="check server connectivity")
    ping.add_argument("message", nargs="?")

    put = commands.add_parser(
        "put", help="store a cache value with freshness policy"
    )
    put.add_argument("key")
    put.add_argument("value")
    put.add_argument("--ttl", type=int)
    put.add_argument("--stale", type=int)
    put.add_argument("--tag", action="append", default=[])
    put.add_argument("--lease", help="refresh lease token")

    set_command = commands.add_parser(
        "set", help="store a Redis-compatible string value"
    )
    set_command.add_argument("key")
    set_command.add_argument("value")
    set_command.add_argument("--expire", type=int)

    get = commands.add_parser("get", help="read a cached value")
    get.add_argument("key")

    explain = commands.add_parser(
        "explain", help="explain current and recommended policy for a key"
    )
    explain.add_argument("key")

    fetch = commands.add_parser(
        "fetch", help="read through a configured HTTP origin"
    )
    fetch.add_argument("key")
    fetch.add_argument("origin")
    fetch.add_argument(
        "path", help="allowed absolute path on the named origin"
    )
    fetch.add_argument(
        "--refresh", action="store_true", help="force an origin refresh"
    )

    delete = commands.add_parser("delete", aliases=["del"], help="delete keys")
    delete.add_argument("keys", nargs="+")

    exists = commands.add_parser("exists", help="count existing keys")
    exists.add_argument("keys", nargs="+")

    expire = commands.add_parser("expire", help="set a key TTL")
    expire.add_argument("key")
    expire.add_argument("seconds", type=int)

    ttl = commands.add_parser("ttl", help="read a key TTL")
    ttl.add_argument("key")

    mget = commands.add_parser("mget", help="read multiple values")
    mget.add_argument("keys", nargs="+")

    mset = commands.add_parser("mset", help="store key-value pairs")
    mset.add_argument("pairs", nargs="+", metavar="KEY_OR_VALUE")

    commands.add_parser("dbsize", help="count live entries")
    commands.add_parser("info", help="show server information")
    topology = commands.add_parser(
        "topology", help="show cluster topology and ownership"
    )
    topology.add_argument(
        "key", nargs="?", help="show owners for one cache key"
    )
    commands.add_parser(
        "status", help="show cluster health and replication status"
    )
    commands.add_parser(
        "identity", help="show the authenticated tenant identity"
    )
    control_status = commands.add_parser(
        "control-status", help="show the tenant control-plane dashboard"
    )
    control_status.add_argument("--tenant")
    commands.add_parser(
        "control-tenants",
        help="list tenant metadata (platform operator only)",
    )
    commands.add_parser(
        "deployment-status",
        help="show desired and observed deployment metadata",
    )
    deployment_set = commands.add_parser(
        "deployment-set",
        help="set desired rolling deployment metadata from JSON",
    )
    deployment_set.add_argument("tenant")
    deployment_set.add_argument("path", nargs="?", default="-")
    deployment_observe = commands.add_parser(
        "deployment-observe",
        help="report observed deployment metadata from JSON",
    )
    deployment_observe.add_argument("tenant")
    deployment_observe.add_argument("path", nargs="?", default="-")
    operation = commands.add_parser(
        "operation", help="show asynchronous control operation status"
    )
    operation.add_argument("operation_id")
    backup = commands.add_parser(
        "backup", help="queue an encrypted tenant backup"
    )
    backup.add_argument("--tenant")
    data_export = commands.add_parser(
        "data-export", help="queue an encrypted tenant data export"
    )
    data_export.add_argument("--tenant")
    restore_validate = commands.add_parser(
        "restore-validate", help="queue validation of an encrypted backup"
    )
    restore_validate.add_argument("backup_id")
    restore_validate.add_argument("--tenant")
    restore = commands.add_parser(
        "restore", help="queue a validated replacement restore"
    )
    restore.add_argument("tenant")
    restore.add_argument("backup_id")
    restore.add_argument("validation_token")
    restore.add_argument("--confirm", required=True)
    restore.add_argument("--yes", action="store_true")
    drill = commands.add_parser(
        "dr-drill", help="queue a non-mutating recovery drill"
    )
    drill.add_argument("backup_id")
    drill.add_argument("--tenant")
    delete_challenge = commands.add_parser(
        "tenant-delete-challenge",
        help="issue a short-lived irreversible deletion challenge",
    )
    delete_challenge.add_argument("--tenant")
    tenant_delete = commands.add_parser(
        "tenant-delete", help="irreversibly delete tenant data"
    )
    tenant_delete.add_argument("tenant")
    tenant_delete.add_argument("challenge")
    tenant_delete.add_argument("--confirm", required=True)
    tenant_delete.add_argument("--yes", action="store_true")
    audit = commands.add_parser(
        "audit", help="export verified audit records as JSON"
    )
    audit.add_argument("--tenant")
    audit.add_argument("--after", type=int, default=0)
    audit.add_argument("--limit", type=int, default=1000)
    audit_export = commands.add_parser(
        "audit-export", help="queue an encrypted audit export"
    )
    audit_export.add_argument("--tenant")
    audit_export.add_argument("--after", type=int, default=0)
    audit_export.add_argument("--limit", type=int, default=1000)
    audit_prune = commands.add_parser(
        "audit-prune",
        help="prune fully exported audit segments using the signed boundary",
    )
    audit_prune.add_argument("through_sequence", type=int)
    audit_prune.add_argument("expected_hash")
    audit_prune.add_argument("--yes", action="store_true")
    billing = commands.add_parser(
        "billing-export",
        help="prepare a deterministic idempotent usage batch",
    )
    billing.add_argument("--tenant")
    billing.add_argument("--start-period")
    billing.add_argument("--end-period")
    commands.add_parser(
        "origins", help="show configured origin health and breaker state"
    )
    recommendations = commands.add_parser(
        "recommendations", help="show bounded policy recommendations"
    )
    recommendations.add_argument("--limit", type=int, default=100)
    simulate = commands.add_parser(
        "policy-simulate", help="dry-run a policy against offline JSON records"
    )
    simulate.add_argument(
        "path", nargs="?", default="-", help="simulation JSON path (default: stdin)"
    )
    commands.add_parser(
        "experiments", help="show experiment allocation and guardrail status"
    )
    event = commands.add_parser(
        "event", help="ingest one freshness event from a JSON file or stdin"
    )
    event.add_argument(
        "path", nargs="?", default="-", help="JSON event path (default: stdin)"
    )
    commands.add_parser(
        "events-status", help="show event checkpoints, DLQ, and graph status"
    )
    retry_events = commands.add_parser(
        "events-retry", help="retry due dead-letter events"
    )
    retry_events.add_argument("--limit", type=int, default=100)

    flush = commands.add_parser("flush", help="delete every cache entry")
    flush.add_argument(
        "--yes", action="store_true", help="confirm destructive operation"
    )

    lease = commands.add_parser("lease", help="read and acquire refresh ownership")
    lease.add_argument("key")

    invalidate = commands.add_parser(
        "invalidate", help="invalidate entries by tag"
    )
    invalidate.add_argument("tags", nargs="+")

    commands.add_parser(
        "hash-password", help="prompt for and hash a password locally"
    )
    init_users = commands.add_parser(
        "init-users", help="create a protected administrator users file"
    )
    init_users.add_argument("path")
    init_users.add_argument("--username", default="admin")
    init_users.add_argument("--tenant", default="default")
    return parser


def run(argv: Optional[Sequence[str]] = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)
    if args.command in (None, "serve"):
        return _serve(args)
    if args.command == "hash-password":
        return _hash_password()
    if args.command == "init-users":
        return _init_users(args.path, args.username, args.tenant)

    if args.command == "flush" and not args.yes:
        parser.error("flush requires --yes")
    if args.command in ("restore", "tenant-delete", "audit-prune") and not args.yes:
        parser.error("{} requires --yes".format(args.command))
    if args.command == "mset" and len(args.pairs) % 2:
        parser.error("mset requires complete KEY VALUE pairs")

    try:
        with MegaCacheClient(
            host=args.host,
            port=args.port,
            username=args.username,
            password=args.password,
            timeout=args.timeout,
            tls=args.tls,
            ca_file=args.ca_file,
            server_name=args.server_name,
        ) as client:
            response = _execute(client, args)
    except (MegaCacheClientError, OSError, ValueError) as exc:
        print("mc: {}".format(exc), file=sys.stderr)
        return 1

    if response is None and args.command == "get":
        if args.json:
            print("null")
        else:
            print("(nil)")
        return 1
    _print_response(response, args.json)
    return 0


def main() -> None:
    raise SystemExit(run())


def _execute(client: MegaCacheClient, args: argparse.Namespace) -> Any:
    command = args.command
    if command == "ping":
        return client.command("PING", *([args.message] if args.message else []))
    if command == "put":
        parts: List[Any] = ["MC.SET", args.key, args.value]
        if args.ttl is not None:
            parts.extend(["TTL", args.ttl])
        if args.stale is not None:
            parts.extend(["STALE", args.stale])
        if args.tag:
            parts.extend(["TAGS", len(args.tag), *args.tag])
        if args.lease:
            parts.extend(["LEASE", args.lease])
        return client.command(*parts)
    if command == "set":
        parts = ["SET", args.key, args.value]
        if args.expire is not None:
            parts.extend(["EX", args.expire])
        return client.command(*parts)
    if command == "get":
        return client.command("GET", args.key)
    if command == "explain":
        return _decode_json_response(client.command("MC.EXPLAIN", args.key))
    if command == "fetch":
        parts = ["MC.FETCH", args.key, args.origin, args.path]
        if args.refresh:
            parts.append("REFRESH")
        return _decode_json_response(client.command(*parts))
    if command in ("delete", "del"):
        return client.command("DEL", *args.keys)
    if command == "exists":
        return client.command("EXISTS", *args.keys)
    if command == "expire":
        return client.command("EXPIRE", args.key, args.seconds)
    if command == "ttl":
        return client.command("TTL", args.key)
    if command == "mget":
        return client.command("MGET", *args.keys)
    if command == "mset":
        return client.command("MSET", *args.pairs)
    if command == "dbsize":
        return client.command("DBSIZE")
    if command == "info":
        return client.command("INFO")
    if command == "topology":
        parts = ["MC.TOPOLOGY"]
        if args.key is not None:
            parts.append(args.key)
        return _decode_json_response(client.command(*parts))
    if command == "status":
        return _decode_json_response(client.command("MC.STATUS"))
    if command == "identity":
        return _decode_json_response(client.command("MC.IDENTITY"))
    if command == "control-status":
        parts = ["MC.CONTROL.STATUS"]
        if args.tenant is not None:
            parts.append(args.tenant)
        return _decode_json_response(client.command(*parts))
    if command == "control-tenants":
        return _decode_json_response(client.command("MC.CONTROL.TENANTS"))
    if command == "deployment-status":
        return _decode_json_response(
            client.command("MC.CONTROL.ORCHESTRATOR")
        )
    if command == "deployment-set":
        return _decode_json_response(
            client.command(
                "MC.CONTROL.DEPLOYMENT",
                args.tenant,
                _read_json_document(args.path, "deployment"),
            )
        )
    if command == "deployment-observe":
        return _decode_json_response(
            client.command(
                "MC.CONTROL.OBSERVE",
                args.tenant,
                _read_json_document(args.path, "deployment observation"),
            )
        )
    if command == "operation":
        return _decode_json_response(
            client.command("MC.CONTROL.OPERATION", args.operation_id)
        )
    if command == "backup":
        parts = ["MC.BACKUP"]
        if args.tenant is not None:
            parts.append(args.tenant)
        return _decode_json_response(client.command(*parts))
    if command == "data-export":
        parts = ["MC.DATA.EXPORT"]
        if args.tenant is not None:
            parts.append(args.tenant)
        return _decode_json_response(client.command(*parts))
    if command == "restore-validate":
        parts = ["MC.RESTORE.VALIDATE"]
        if args.tenant is not None:
            parts.append(args.tenant)
        parts.append(args.backup_id)
        return _decode_json_response(client.command(*parts))
    if command == "restore":
        return _decode_json_response(
            client.command(
                "MC.RESTORE",
                args.tenant,
                args.backup_id,
                args.validation_token,
                args.confirm,
            )
        )
    if command == "dr-drill":
        parts = ["MC.DR.DRILL"]
        if args.tenant is not None:
            parts.append(args.tenant)
        parts.append(args.backup_id)
        return _decode_json_response(client.command(*parts))
    if command == "tenant-delete-challenge":
        parts = ["MC.TENANT.DELETE.CHALLENGE"]
        if args.tenant is not None:
            parts.append(args.tenant)
        return _decode_json_response(client.command(*parts))
    if command == "tenant-delete":
        return _decode_json_response(
            client.command(
                "MC.TENANT.DELETE",
                args.tenant,
                args.challenge,
                args.confirm,
            )
        )
    if command == "audit":
        return _decode_json_response(
            client.command(
                "MC.AUDIT",
                args.tenant or "-",
                args.after,
                args.limit,
            )
        )
    if command == "audit-export":
        parts = ["MC.AUDIT.EXPORT"]
        if args.tenant is not None:
            parts.append(args.tenant)
        elif args.after or args.limit != 1000:
            parts.append("-")
        if len(parts) > 1 or args.after or args.limit != 1000:
            parts.extend([args.after, args.limit])
        return _decode_json_response(client.command(*parts))
    if command == "audit-prune":
        return _decode_json_response(
            client.command(
                "MC.AUDIT.PRUNE",
                args.through_sequence,
                args.expected_hash,
            )
        )
    if command == "billing-export":
        return _decode_json_response(
            client.command(
                "MC.BILLING.EXPORT",
                args.tenant or "-",
                args.start_period or "-",
                args.end_period or "-",
            )
        )
    if command == "origins":
        return _decode_json_response(client.command("MC.ORIGINS"))
    if command == "recommendations":
        return _decode_json_response(
            client.command("MC.RECOMMENDATIONS", args.limit)
        )
    if command == "policy-simulate":
        return _decode_json_response(
            client.command(
                "MC.POLICY.SIMULATE",
                _read_json_document(args.path, "simulation"),
            )
        )
    if command == "experiments":
        return _decode_json_response(client.command("MC.EXPERIMENTS"))
    if args.command == "event":
        return _decode_json_response(
            client.command("MC.EVENT", _read_event(args.path))
        )
    if args.command == "events-status":
        return _decode_json_response(client.command("MC.EVENT.STATUS"))
    if args.command == "events-retry":
        return _decode_json_response(
            client.command("MC.EVENT.RETRY", args.limit)
        )
    if command == "flush":
        return client.command("FLUSHDB")
    if command == "lease":
        return client.command("MC.LEASE", args.key)
    if command == "invalidate":
        return client.command("MC.INVALIDATE", *args.tags)
    raise ValueError("unsupported command {}".format(command))


def _serve(args: argparse.Namespace) -> int:
    config = Config.from_env()
    configure_logging(config.log_format)
    validate_tls_config(config)
    if getattr(args, "http_host", None) is not None:
        config = replace(config, host=args.http_host)
    if getattr(args, "http_port", None) is not None:
        config = replace(config, port=args.http_port)
    if getattr(args, "resp_host", None) is not None:
        config = replace(config, resp_host=args.resp_host)
    if getattr(args, "resp_port", None) is not None:
        config = replace(config, resp_port=args.resp_port)

    if config.control_plane_file is None:
        engine = _build_data_plane(config)
    else:
        definition = load_control_plane_definition(
            config.control_plane_file,
            max_tenants=config.control_max_tenants,
            max_entries=config.max_entries,
            max_bytes=config.max_memory_bytes,
            max_entry_bytes=config.max_entry_bytes,
            max_usage_periods=config.control_max_usage_periods,
            max_operations=config.control_max_operations,
        )
        key_provider = load_key_provider(
            config.control_master_key, config.control_key_file
        )
        all_origins = (
            ()
            if config.origins_file is None
            else load_origin_definitions(config.origins_file)
        )
        origin_map = {origin.name: origin for origin in all_origins}
        engines = {}
        tenant_paths = {}
        try:
            for tenant in definition.tenants:
                missing_origins = set(tenant.origins) - set(origin_map)
                if missing_origins:
                    raise ControlPlaneError(
                        "tenant '{}' references unknown origins: {}".format(
                            tenant.tenant_id,
                            ", ".join(sorted(missing_origins)),
                        )
                    )
                namespace = tenant_namespace_id(
                    key_provider, tenant.tenant_id
                )
                tenant_directory = os.path.join(
                    os.path.abspath(config.control_state_directory),
                    "tenants",
                    namespace,
                )
                event_state = os.path.join(
                    tenant_directory, "events-state.json"
                )
                intelligence_state = os.path.join(
                    tenant_directory, "intelligence-state.json"
                )
                engines[tenant.tenant_id] = _build_data_plane(
                    config,
                    max_entries=tenant.quotas.max_entries,
                    max_memory_bytes=tenant.quotas.max_bytes,
                    max_entry_bytes=tenant.quotas.max_entry_bytes,
                    origins=tuple(origin_map[name] for name in tenant.origins),
                    origin_concurrency=tenant.quotas.origin_concurrency,
                    event_state_file=event_state,
                    intelligence_state_file=intelligence_state,
                    allowed_webhooks=tenant.webhooks,
                )
                tenant_paths[tenant.tenant_id] = (
                    event_state,
                    intelligence_state,
                )
            engine = ManagedControlPlane(
                definition,
                engines,
                state_directory=config.control_state_directory,
                key_provider=key_provider,
                state_max_bytes=config.control_state_max_bytes,
                max_usage_periods=config.control_max_usage_periods,
                max_operations=config.control_max_operations,
                audit_segment_bytes=config.control_audit_segment_bytes,
                audit_max_segments=config.control_audit_max_segments,
                artifact_max_bytes=config.control_artifact_max_bytes,
                scheduler_interval_seconds=(
                    config.control_scheduler_interval_seconds
                ),
                tenant_state_paths=tenant_paths,
            )
        except Exception:
            for candidate in engines.values():
                close = getattr(candidate, "close", None)
                if close is not None:
                    close(config.shutdown_grace_seconds)
            raise
    http_server = MegaCacheServer((config.host, config.port), config, engine)
    resp_server = MegaCacheRespServer(
        (config.resp_host, config.resp_port), config, engine
    )
    tls_enabled = enable_server_tls(http_server, config)
    enable_server_tls(resp_server, config)
    resp_thread = threading.Thread(target=resp_server.serve_forever, daemon=True)
    resp_thread.start()
    heartbeat_stopped = threading.Event()

    def send_heartbeats() -> None:
        while not heartbeat_stopped.wait(config.heartbeat_interval_seconds):
            for node_id in config.cluster_nodes:
                engine.heartbeat(node_id)

    heartbeat_thread = threading.Thread(target=send_heartbeats, daemon=True)
    heartbeat_thread.start()
    logger = logging.getLogger("megacache")
    logger.info("HTTP API listening on %s:%s", config.host, config.port)
    logger.info("RESP2 API listening on %s:%s", config.resp_host, config.resp_port)
    if tls_enabled:
        logger.info("TLS enabled for HTTP and RESP2")

    shutdown_started = threading.Event()
    shutdown_threads: List[threading.Thread] = []

    def begin_engine_shutdown() -> None:
        begin_shutdown = getattr(engine, "begin_shutdown", None)
        if begin_shutdown is not None:
            begin_shutdown()

    def shutdown() -> None:
        if shutdown_started.is_set():
            return
        shutdown_started.set()
        heartbeat_stopped.set()
        http_server.start_draining()
        resp_server.start_draining()
        begin_engine_shutdown()
        http_server.shutdown()
        resp_server.shutdown()
        drain_threads = [
            threading.Thread(
                target=server.drain_connections,
                args=(config.shutdown_grace_seconds,),
                daemon=True,
            )
            for server in (http_server, resp_server)
        ]
        for drain_thread in drain_threads:
            drain_thread.start()
        for drain_thread in drain_threads:
            drain_thread.join(config.shutdown_grace_seconds + 1)

    previous_handlers = {}
    if threading.current_thread() is threading.main_thread():
        for signum in (signal.SIGINT, signal.SIGTERM):
            previous_handlers[signum] = signal.getsignal(signum)

            def handle_signal(
                received: int, frame: Any, signal_number: int = signum
            ) -> None:
                logger.info("shutdown requested by signal %s", signal_number)
                shutdown_thread = threading.Thread(
                    target=shutdown, daemon=True
                )
                shutdown_threads.append(shutdown_thread)
                shutdown_thread.start()

            signal.signal(signum, handle_signal)
    try:
        http_server.serve_forever()
    except KeyboardInterrupt:
        shutdown_thread = threading.Thread(target=shutdown, daemon=True)
        shutdown_threads.append(shutdown_thread)
        shutdown_thread.start()
    finally:
        heartbeat_stopped.set()
        begin_engine_shutdown()
        if shutdown_threads:
            shutdown_threads[-1].join(
                timeout=config.shutdown_grace_seconds + 5
            )
        else:
            resp_server.shutdown()
        http_server.server_close()
        resp_server.server_close()
        resp_thread.join(timeout=5)
        heartbeat_thread.join(timeout=config.heartbeat_interval_seconds + 1)
        close_engine = getattr(engine, "close", None)
        if close_engine is not None:
            close_engine(config.shutdown_grace_seconds)
        for signum, previous in previous_handlers.items():
            signal.signal(signum, previous)
    return 0


def _build_data_plane(
    config: Config,
    *,
    max_entries: Optional[int] = None,
    max_memory_bytes: Optional[int] = None,
    max_entry_bytes: Optional[int] = None,
    origins: Optional[Sequence[Any]] = None,
    origin_concurrency: Optional[int] = None,
    event_state_file: Optional[str] = None,
    intelligence_state_file: Optional[str] = None,
    allowed_webhooks: Optional[Sequence[str]] = None,
) -> Any:
    entry_limit = config.max_entries if max_entries is None else max_entries
    memory_limit = (
        config.max_memory_bytes
        if max_memory_bytes is None
        else max_memory_bytes
    )
    per_entry_limit = (
        config.max_entry_bytes
        if max_entry_bytes is None
        else max_entry_bytes
    )
    nodes = [
        ClusterNode(
            node_id,
            CacheEngine(
                max_entries=entry_limit,
                max_memory_bytes=memory_limit,
                max_entry_bytes=per_entry_limit,
                default_ttl_seconds=config.default_ttl_seconds,
                default_stale_seconds=config.default_stale_seconds,
                lease_seconds=config.lease_seconds,
                eviction_policy=config.eviction_policy,
            ),
        )
        for node_id in config.cluster_nodes
    ]
    cluster = ClusterStorage(
        nodes,
        replica_count=config.replica_count,
        virtual_nodes=config.virtual_nodes,
        consistency=config.consistency,
        heartbeat_timeout_seconds=config.heartbeat_timeout_seconds,
        lease_seconds=config.lease_seconds,
        snapshot_payload_limit_bytes=min(
            config.snapshot_payload_limit_bytes, memory_limit
        ),
        snapshot_chunk_bytes=min(
            config.snapshot_chunk_bytes,
            config.snapshot_payload_limit_bytes,
            memory_limit,
        ),
        snapshot_max_in_flight=config.snapshot_max_in_flight,
        max_leases=entry_limit,
        max_lease_memory_bytes=memory_limit,
        max_retained_tombstones=min(
            config.max_retained_tombstones, max(1, entry_limit)
        ),
        max_hot_replica_keys=min(config.intelligence_max_keys, entry_limit),
    )
    engine: Any = CacheIntelligence(
        MutationClock(cluster),
        enabled=config.intelligence_enabled,
        adaptive_ttl=config.adaptive_ttl_enabled,
        min_ttl_seconds=config.intelligence_min_ttl_seconds,
        max_ttl_seconds=config.intelligence_max_ttl_seconds,
        telemetry_max_keys=min(config.intelligence_max_keys, entry_limit),
        telemetry_max_classes=config.intelligence_max_classes,
        hot_key_threshold=config.hot_key_threshold,
        hot_key_window_seconds=config.hot_key_window_seconds,
        hot_key_extra_replicas=config.hot_key_extra_replicas,
        experiment_enabled=config.experiment_enabled,
        experiment_id=config.experiment_id,
        experiment_allocation_percent=config.experiment_allocation_percent,
        experiment_min_samples=config.experiment_min_samples,
        experiment_max_miss_regression=(
            config.experiment_max_miss_regression
        ),
        state_file=(
            config.intelligence_state_file
            if intelligence_state_file is None
            else intelligence_state_file
        ),
        eviction_policy=config.eviction_policy,
    )
    selected_origins = origins
    if selected_origins is None and config.origins_file is not None:
        selected_origins = load_origin_definitions(config.origins_file)
    if selected_origins:
        engine = OriginCache(
            engine,
            selected_origins,
            worker_threads=min(
                config.origin_worker_threads,
                origin_concurrency or config.origin_global_max_concurrency,
            ),
            refresh_queue_size=config.origin_refresh_queue_size,
            global_max_concurrency=min(
                config.origin_global_max_concurrency,
                origin_concurrency or config.origin_global_max_concurrency,
            ),
            global_max_queue=config.origin_global_max_queue,
        )
    if config.events_file is not None:
        engine = load_event_automation(
            engine,
            config.events_file,
            config.event_state_file
            if event_state_file is None
            else event_state_file,
            max_seen_events=config.event_max_seen,
            max_replay_tokens=config.event_max_replay_tokens,
            max_dead_letters=config.event_max_dead_letters,
            max_dead_letter_bytes=config.event_max_dead_letter_bytes,
            max_streams=config.event_max_streams,
            max_state_bytes=config.event_max_state_bytes,
            max_payload_bytes=config.event_max_payload_bytes,
            max_cursor_bytes=config.event_max_cursor_bytes,
            max_error_bytes=config.event_max_error_bytes,
            graph_max_nodes=config.event_graph_max_nodes,
            graph_max_edges=config.event_graph_max_edges,
            graph_max_fanout=config.event_graph_max_fanout,
            graph_max_depth=config.event_graph_max_depth,
            graph_max_invalidation_nodes=(
                config.event_graph_max_invalidation_nodes
            ),
            allowed_webhooks=allowed_webhooks,
        )
    return engine


def _hash_password() -> int:
    password = getpass.getpass("Password: ")
    confirmation = getpass.getpass("Confirm password: ")
    if password != confirmation:
        print("mc: passwords do not match", file=sys.stderr)
        return 1
    try:
        print(hash_password(password))
    except ValueError as exc:
        print("mc: {}".format(exc), file=sys.stderr)
        return 1
    return 0


def _init_users(path: str, username: str, tenant_id: str = "default") -> int:
    password = getpass.getpass("Password for {}: ".format(username))
    confirmation = getpass.getpass("Confirm password: ")
    if password != confirmation:
        print("mc: passwords do not match", file=sys.stderr)
        return 1
    try:
        write_example_users_file(path, username, password, tenant_id)
    except (OSError, ValueError) as exc:
        print("mc: {}".format(exc), file=sys.stderr)
        return 1
    print(path)
    return 0


def _print_response(value: Any, as_json: bool) -> None:
    if as_json:
        print(json.dumps(_json_value(value), separators=(",", ":")))
        return
    if isinstance(value, bytes):
        sys.stdout.buffer.write(value + b"\n")
        return
    if isinstance(value, list):
        for index, item in enumerate(value, 1):
            rendered = "(nil)" if item is None else _text_value(item)
            print("{}) {}".format(index, rendered))
        return
    if isinstance(value, dict):
        print(json.dumps(_json_value(value), indent=2, sort_keys=True))
        return
    print(value)


def _text_value(value: Any) -> str:
    if isinstance(value, bytes):
        return value.decode("utf-8", "backslashreplace")
    return str(value)


def _json_value(value: Any) -> Any:
    if isinstance(value, bytes):
        try:
            return value.decode("utf-8")
        except UnicodeDecodeError:
            return {
                "$binary": base64.b64encode(value).decode("ascii"),
                "$encoding": "base64",
            }
    if isinstance(value, list):
        return [_json_value(item) for item in value]
    return value


def _decode_json_response(value: Any) -> Any:
    if not isinstance(value, bytes):
        raise ValueError("server returned an invalid JSON response")
    try:
        decoded = json.loads(value.decode("utf-8"))
    except (UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise ValueError("server returned invalid JSON") from exc
    if not isinstance(decoded, dict):
        raise ValueError("server returned invalid JSON")
    return decoded


def _read_event(path: str) -> bytes:
    return _read_json_document(path, "event")


def _read_json_document(path: str, kind: str) -> bytes:
    try:
        if path == "-":
            value = json.load(sys.stdin)
        else:
            with open(path, "r", encoding="utf-8") as source:
                value = json.load(source)
    except (OSError, json.JSONDecodeError) as exc:
        raise ValueError(
            "unable to read {} JSON: {}".format(kind, exc)
        ) from exc
    if not isinstance(value, dict):
        raise ValueError("{} JSON must be an object".format(kind))
    return json.dumps(value, separators=(",", ":")).encode("utf-8")
