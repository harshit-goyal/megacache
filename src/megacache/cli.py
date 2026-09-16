"""Native MegaCache command-line interface."""

import argparse
import base64
import json
import logging
import os
import sys
import threading
from dataclasses import replace
from typing import Any, List, Optional, Sequence

from .client import MegaCacheClient, MegaCacheClientError
from .config import Config
from .engine import CacheEngine
from .resp import MegaCacheRespServer
from .server import MegaCacheServer


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="mc",
        description="Run and interact with MegaCache.",
    )
    parser.add_argument(
        "--version", action="version", version="MegaCache 0.3.1"
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
        "--password",
        default=os.getenv("MEGACACHE_API_KEY"),
        help="server password; prefer MEGACACHE_API_KEY",
    )
    parser.add_argument(
        "--timeout",
        type=float,
        default=5,
        help="connection timeout in seconds (default: 5)",
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
    return parser


def run(argv: Optional[Sequence[str]] = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)
    if args.command in (None, "serve"):
        return _serve(args)

    if args.command == "flush" and not args.yes:
        parser.error("flush requires --yes")
    if args.command == "mset" and len(args.pairs) % 2:
        parser.error("mset requires complete KEY VALUE pairs")

    try:
        with MegaCacheClient(
            host=args.host,
            port=args.port,
            password=args.password,
            timeout=args.timeout,
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
    if command == "flush":
        return client.command("FLUSHDB")
    if command == "lease":
        return client.command("MC.LEASE", args.key)
    if command == "invalidate":
        return client.command("MC.INVALIDATE", *args.tags)
    raise ValueError("unsupported command {}".format(command))


def _serve(args: argparse.Namespace) -> int:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s %(message)s",
    )
    config = Config.from_env()
    if getattr(args, "http_host", None) is not None:
        config = replace(config, host=args.http_host)
    if getattr(args, "http_port", None) is not None:
        config = replace(config, port=args.http_port)
    if getattr(args, "resp_host", None) is not None:
        config = replace(config, resp_host=args.resp_host)
    if getattr(args, "resp_port", None) is not None:
        config = replace(config, resp_port=args.resp_port)

    engine = CacheEngine(
        max_entries=config.max_entries,
        default_ttl_seconds=config.default_ttl_seconds,
        default_stale_seconds=config.default_stale_seconds,
        lease_seconds=config.lease_seconds,
    )
    http_server = MegaCacheServer((config.host, config.port), config, engine)
    resp_server = MegaCacheRespServer(
        (config.resp_host, config.resp_port), config, engine
    )
    resp_thread = threading.Thread(target=resp_server.serve_forever, daemon=True)
    resp_thread.start()
    logger = logging.getLogger("megacache")
    logger.info("HTTP API listening on %s:%s", config.host, config.port)
    logger.info("RESP2 API listening on %s:%s", config.resp_host, config.resp_port)
    try:
        http_server.serve_forever()
    except KeyboardInterrupt:
        pass
    finally:
        http_server.server_close()
        resp_server.shutdown()
        resp_server.server_close()
        resp_thread.join(timeout=5)
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
