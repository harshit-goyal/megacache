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
from .engine import CacheEngine
from .observability import configure_logging
from .origin import OriginCache
from .resp import MegaCacheRespServer
from .server import MegaCacheServer
from .transport import enable_server_tls, validate_tls_config


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="mc",
        description="Run and interact with MegaCache.",
    )
    parser.add_argument(
        "--version", action="version", version="MegaCache 0.6.0"
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
        "origins", help="show configured origin health and breaker state"
    )

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
    return parser


def run(argv: Optional[Sequence[str]] = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)
    if args.command in (None, "serve"):
        return _serve(args)
    if args.command == "hash-password":
        return _hash_password()
    if args.command == "init-users":
        return _init_users(args.path, args.username)

    if args.command == "flush" and not args.yes:
        parser.error("flush requires --yes")
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
    if command == "origins":
        return _decode_json_response(client.command("MC.ORIGINS"))
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

    nodes = [
        ClusterNode(
            node_id,
            CacheEngine(
                max_entries=config.max_entries,
                max_memory_bytes=config.max_memory_bytes,
                max_entry_bytes=config.max_entry_bytes,
                default_ttl_seconds=config.default_ttl_seconds,
                default_stale_seconds=config.default_stale_seconds,
                lease_seconds=config.lease_seconds,
            ),
        )
        for node_id in config.cluster_nodes
    ]
    engine = ClusterStorage(
        nodes,
        replica_count=config.replica_count,
        virtual_nodes=config.virtual_nodes,
        consistency=config.consistency,
        heartbeat_timeout_seconds=config.heartbeat_timeout_seconds,
        lease_seconds=config.lease_seconds,
        snapshot_payload_limit_bytes=config.snapshot_payload_limit_bytes,
        snapshot_chunk_bytes=config.snapshot_chunk_bytes,
        snapshot_max_in_flight=config.snapshot_max_in_flight,
        max_leases=config.max_entries,
        max_lease_memory_bytes=config.max_memory_bytes,
        max_retained_tombstones=config.max_retained_tombstones,
    )
    if config.origins_file is not None:
        engine = OriginCache.from_file(
            engine,
            config.origins_file,
            worker_threads=config.origin_worker_threads,
            refresh_queue_size=config.origin_refresh_queue_size,
            global_max_concurrency=config.origin_global_max_concurrency,
            global_max_queue=config.origin_global_max_queue,
        )
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


def _init_users(path: str, username: str) -> int:
    password = getpass.getpass("Password for {}: ".format(username))
    confirmation = getpass.getpass("Confirm password: ")
    if password != confirmation:
        print("mc: passwords do not match", file=sys.stderr)
        return 1
    try:
        write_example_users_file(path, username, password)
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
