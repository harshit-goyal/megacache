#!/usr/bin/env python3
"""Run one or all SDK conformance suites against a real MegaCache server."""

import argparse
import os
import shutil
import subprocess
import sys
import threading
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))

from megacache.config import Config
from megacache.coordination import MutationClock
from megacache.engine import CacheEngine
from megacache.origin import HTTPOrigin, OriginCache, OriginResponse
from megacache.resp import MegaCacheRespServer


def config():
    return Config(
        host="127.0.0.1",
        port=0,
        resp_host="127.0.0.1",
        resp_port=0,
        max_entries=100,
        max_memory_bytes=1_000_000,
        max_entry_bytes=100_000,
        max_body_bytes=100_000,
        default_ttl_seconds=60,
        default_stale_seconds=60,
        lease_seconds=5,
        shutdown_grace_seconds=1,
        api_key=None,
        tls_cert_file=None,
        tls_key_file=None,
        users_file=None,
        log_format="text",
    )


def command_for(name):
    if name == "python":
        return [sys.executable, "-m", "unittest", "sdk.python.test_conformance", "-v"]
    if name == "node":
        return ["npm", "test", "--silent", "--prefix", "sdk/node"]
    if name == "go":
        return ["go", "test", "./..."]
    if name == "java":
        output = ROOT / "sdk" / "java" / "build" / "conformance"
        output.mkdir(parents=True, exist_ok=True)
        sources = [
            str(path)
            for path in (ROOT / "sdk" / "java" / "src").rglob("*.java")
        ]
        return [
            ["javac", "-d", str(output)] + sources,
            ["java", "-cp", str(output), "io.megacache.ConformanceTest"],
        ]
    raise ValueError(name)


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--sdk",
        choices=("all", "python", "node", "go", "java"),
        default="all",
    )
    args = parser.parse_args()
    selected = (
        ("python", "node", "go", "java")
        if args.sdk == "all"
        else (args.sdk,)
    )
    traces = []

    def transport(origin, path, addresses, headers):
        traces.append(headers.get("traceparent"))
        return OriginResponse(200, path.encode("utf-8"))

    storage = MutationClock(CacheEngine(max_entries=100))
    engine = OriginCache(
        storage,
        [
            HTTPOrigin.from_dict(
                {
                    "name": "catalog",
                    "base_url": "https://origin.example",
                    "allowed_hosts": ["origin.example"],
                    "allowed_ports": [443],
                    "allowed_path_prefixes": ["/v1/"],
                    "retry_attempts": 0,
                }
            )
        ],
        resolver=lambda host, port: ["93.184.216.34"],
        transport=transport,
    )
    server = MegaCacheRespServer(("127.0.0.1", 0), config(), engine)
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    environment = dict(os.environ)
    environment.update(
        {
            "PYTHONPATH": str(ROOT / "src"),
            "MEGACACHE_CONFORMANCE_HOST": "127.0.0.1",
            "MEGACACHE_CONFORMANCE_PORT": str(server.server_address[1]),
            "MEGACACHE_FIXTURES": str(
                ROOT / "conformance" / "fixtures" / "commands.tsv"
            ),
        }
    )
    failed = False
    try:
        for name in selected:
            executable = {
                "python": sys.executable,
                "node": "node",
                "go": "go",
                "java": "javac",
            }[name]
            if shutil.which(executable) is None:
                print("{}: SKIPPED (toolchain unavailable)".format(name))
                continue
            commands = command_for(name)
            if commands and isinstance(commands[0], str):
                commands = [commands]
            for command in commands:
                result = subprocess.run(
                    command,
                    cwd=str(
                        ROOT / "sdk" / "go" if name == "go" else ROOT
                    ),
                    env=environment,
                    check=False,
                )
                if result.returncode:
                    failed = True
                    break
            print("{}: {}".format(name, "FAILED" if failed else "passed"))
            if failed:
                break
        if not failed and selected != ("python",) and not any(traces):
            print("warning: no conformance fetch propagated traceparent")
    finally:
        server.shutdown()
        server.server_close()
        thread.join(timeout=2)
        engine.close(1)
        shutil.rmtree(ROOT / "sdk" / "java" / "build", ignore_errors=True)
    return int(failed)


if __name__ == "__main__":
    raise SystemExit(main())
