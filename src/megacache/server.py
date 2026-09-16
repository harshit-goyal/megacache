"""Dependency-free HTTP API for MegaCache."""

import hmac
import json
import logging
from dataclasses import asdict
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from typing import Any, Dict, Optional
from urllib.parse import unquote, urlsplit

from .config import Config
from .engine import CacheEngine, CacheResult

LOG = logging.getLogger("megacache")


class MegaCacheServer(ThreadingHTTPServer):
    daemon_threads = True

    def __init__(self, address: tuple, config: Config, engine: CacheEngine):
        super().__init__(address, MegaCacheHandler)
        self.config = config
        self.engine = engine


class MegaCacheHandler(BaseHTTPRequestHandler):
    server: MegaCacheServer
    protocol_version = "HTTP/1.1"

    def do_GET(self) -> None:
        path = urlsplit(self.path).path
        if path in ("/healthz", "/readyz"):
            self._json(200, {"status": "ok"})
            return
        if path == "/metrics":
            payload = self.server.engine.prometheus_metrics().encode("utf-8")
            self._send(200, payload, "text/plain; version=0.0.4")
            return
        if not self._authorized():
            return
        if path == "/v1/stats":
            self._json(200, self.server.engine.stats())
            return
        key = self._key_from(path, "/v1/cache/")
        if key is not None:
            try:
                result = self.server.engine.get(key)
                self._result(result)
            except ValueError as exc:
                self._json(400, {"error": "invalid_request", "message": str(exc)})
            return
        self._json(404, {"error": "route_not_found"})

    def do_PUT(self) -> None:
        if not self._authorized():
            return
        path = urlsplit(self.path).path
        key = self._key_from(path, "/v1/cache/")
        if key is None:
            self._json(404, {"error": "route_not_found"})
            return
        try:
            body = self._read_json()
            if "value" not in body:
                raise ValueError("value is required")
            result = self.server.engine.put(
                key=key,
                value=body["value"],
                ttl_seconds=body.get("ttl_seconds"),
                stale_seconds=body.get("stale_seconds"),
                tags=body.get("tags", ()),
                lease_token=body.get("lease_token"),
            )
            self._result(result, status=201)
        except (ValueError, TypeError) as exc:
            self._json(400, {"error": "invalid_request", "message": str(exc)})

    def do_POST(self) -> None:
        if not self._authorized():
            return
        path = urlsplit(self.path).path
        key = self._key_from(path, "/v1/lease/")
        if key is not None:
            try:
                self._result(self.server.engine.acquire_lease(key))
            except ValueError as exc:
                self._json(400, {"error": "invalid_request", "message": str(exc)})
            return
        if path == "/v1/invalidate":
            try:
                body = self._read_json()
                if "tags" not in body:
                    raise ValueError("tags is required")
                count = self.server.engine.invalidate_tags(body["tags"])
                self._json(200, {"invalidated": count})
            except (ValueError, TypeError) as exc:
                self._json(400, {"error": "invalid_request", "message": str(exc)})
            return
        self._json(404, {"error": "route_not_found"})

    def do_DELETE(self) -> None:
        if not self._authorized():
            return
        path = urlsplit(self.path).path
        key = self._key_from(path, "/v1/cache/")
        if key is None:
            self._json(404, {"error": "route_not_found"})
            return
        try:
            deleted = self.server.engine.delete(key)
            self._json(200 if deleted else 404, {"deleted": deleted})
        except ValueError as exc:
            self._json(400, {"error": "invalid_request", "message": str(exc)})

    def log_message(self, message: str, *args: Any) -> None:
        LOG.info("%s %s", self.address_string(), message % args)

    def _authorized(self) -> bool:
        expected = self.server.config.api_key
        if expected is None:
            return True
        supplied = self.headers.get("Authorization", "")
        valid = supplied.startswith("Bearer ") and hmac.compare_digest(
            supplied[7:], expected
        )
        if not valid:
            self._json(401, {"error": "unauthorized"})
        return valid

    def _read_json(self) -> Dict[str, Any]:
        raw_length = self.headers.get("Content-Length")
        if raw_length is None:
            raise ValueError("Content-Length is required")
        try:
            length = int(raw_length)
        except ValueError as exc:
            raise ValueError("Content-Length must be an integer") from exc
        if length < 0 or length > self.server.config.max_body_bytes:
            raise ValueError("request body exceeds configured limit")
        try:
            value = json.loads(self.rfile.read(length))
        except (json.JSONDecodeError, UnicodeDecodeError) as exc:
            raise ValueError("body must be valid JSON") from exc
        if not isinstance(value, dict):
            raise ValueError("body must be a JSON object")
        return value

    @staticmethod
    def _key_from(path: str, prefix: str) -> Optional[str]:
        if not path.startswith(prefix):
            return None
        return unquote(path[len(prefix) :])

    def _result(self, result: CacheResult, status: Optional[int] = None) -> None:
        code = status or {
            "fresh": 200,
            "stale": 200,
            "stale_lease": 200,
            "miss": 404,
            "lease": 201,
            "loading": 202,
        }[result.state]
        self._json(code, asdict(result))

    def _json(self, status: int, value: Any) -> None:
        self._send(
            status,
            json.dumps(value, separators=(",", ":")).encode("utf-8"),
            "application/json",
        )

    def _send(self, status: int, payload: bytes, content_type: str) -> None:
        self.send_response(status)
        self.send_header("Content-Type", content_type)
        self.send_header("Content-Length", str(len(payload)))
        self.send_header("Cache-Control", "no-store")
        self.end_headers()
        self.wfile.write(payload)
