"""Dependency-free HTTP API for MegaCache."""

import base64
import json
import logging
import time
from dataclasses import asdict
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from typing import Any, Dict, Optional
from urllib.parse import unquote, urlsplit

from .auth import AuthManager, Principal
from .cluster import QuorumError
from .config import Config
from .engine import CacheResult
from .events import (
    EventBackpressure,
    EventError,
    EventIngestionDisabled,
    WebhookAuthError,
)
from .origin import (
    OriginOverloaded,
    OriginPolicyError,
    OriginUnavailable,
)
from .observability import valid_traceparent
from .storage import StorageBackend
from .transport import TLSRequestMixin

LOG = logging.getLogger("megacache")


class MegaCacheServer(TLSRequestMixin, ThreadingHTTPServer):
    daemon_threads = False

    def __init__(self, address: tuple, config: Config, engine: StorageBackend):
        self.initialize_transport()
        super().__init__(address, MegaCacheHandler)
        self.config = config
        self.engine = engine
        self.auth = AuthManager(config.api_key, config.users_file)

class MegaCacheHandler(BaseHTTPRequestHandler):
    server: MegaCacheServer
    protocol_version = "HTTP/1.1"

    def handle_one_request(self) -> None:
        self._request_observed = False
        self._principal = None
        self._traceparent = None
        super().handle_one_request()

    def do_GET(self) -> None:
        self._request_started = time.perf_counter()
        if self._has_request_body():
            self.close_connection = True
            self._json(
                400,
                {
                    "error": "invalid_request",
                    "message": "GET requests must not contain a body",
                },
            )
            return
        path = urlsplit(self.path).path
        if path == "/healthz":
            self._json(200, {"status": "ok"})
            return
        if path == "/readyz":
            status = getattr(self.server.engine, "status", None)
            degraded = False if status is None else bool(status()["degraded"])
            self._json(
                503 if degraded else 200,
                {"status": "degraded" if degraded else "ok"},
            )
            return
        if path == "/metrics":
            payload = self.server.engine.prometheus_metrics().encode("utf-8")
            self._send(200, payload, "text/plain; version=0.0.4")
            return
        if path == "/v1/stats":
            if not self._authorized("admin"):
                return
            self._json(200, self.server.engine.stats())
            return
        if path == "/v1/origins":
            if not self._authorized("admin"):
                return
            origins = getattr(self.server.engine, "origins", None)
            self._json(200, {} if origins is None else origins())
            return
        if path == "/v1/events/status":
            if not self._authorized("admin"):
                return
            status = getattr(self.server.engine, "event_status", None)
            if status is None:
                self._json(
                    503,
                    {
                        "error": "events_not_configured",
                        "message": "event ingestion is not configured",
                    },
                )
            else:
                self._json(200, status())
            return
        if path == "/v1/policies/recommendations":
            if not self._authorized("admin"):
                return
            recommendations = getattr(
                self.server.engine, "recommendations", None
            )
            if recommendations is None:
                self._json(
                    503,
                    {
                        "error": "intelligence_not_configured",
                        "message": "cache intelligence is not available",
                    },
                )
            else:
                principal = self._principal
                assert principal is not None
                self._json(
                    200,
                    recommendations(
                        key_filter=lambda key: principal.allows(
                            "admin", (key,)
                        )
                    ),
                )
            return
        if path == "/v1/experiments":
            if not self._authorized("admin"):
                return
            status = getattr(self.server.engine, "experiment_status", None)
            if status is None:
                self._json(
                    503,
                    {
                        "error": "intelligence_not_configured",
                        "message": "cache intelligence is not available",
                    },
                )
            else:
                self._json(200, status())
            return
        key = self._key_from(path, "/v1/explain/")
        if key is not None:
            if not self._authorized("read", (key,)):
                return
            explain = getattr(self.server.engine, "explain", None)
            if explain is None:
                self._json(
                    503,
                    {
                        "error": "intelligence_not_configured",
                        "message": "cache intelligence is not available",
                    },
                )
                return
            try:
                self._json(200, explain(key))
            except QuorumError as exc:
                self._json(
                    503,
                    {"error": "quorum_unavailable", "message": str(exc)},
                )
            except ValueError as exc:
                self._json(400, {"error": "invalid_request", "message": str(exc)})
            return
        key = self._key_from(path, "/v1/cache/")
        if key is not None:
            if not self._authorized("read", (key,)):
                return
            try:
                result = self.server.engine.get(key)
                self._result(result)
            except QuorumError as exc:
                self._json(
                    503,
                    {"error": "quorum_unavailable", "message": str(exc)},
                )
            except ValueError as exc:
                self._json(400, {"error": "invalid_request", "message": str(exc)})
            return
        self._json(404, {"error": "route_not_found"})

    def do_PUT(self) -> None:
        self._request_started = time.perf_counter()
        self.close_connection = True
        path = urlsplit(self.path).path
        key = self._key_from(path, "/v1/cache/")
        if key is None:
            self._json(404, {"error": "route_not_found"})
            return
        if not self._authorized("write", (key,)):
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
        except QuorumError as exc:
            self._json(
                503,
                {"error": "quorum_unavailable", "message": str(exc)},
            )
        except (ValueError, TypeError) as exc:
            self._json(400, {"error": "invalid_request", "message": str(exc)})

    def do_POST(self) -> None:
        self._request_started = time.perf_counter()
        self.close_connection = True
        path = urlsplit(self.path).path
        if path == "/v1/events":
            if not self._authorized("invalidate"):
                return
            ingest = getattr(self.server.engine, "ingest_event", None)
            if ingest is None:
                self._json(
                    503,
                    {
                        "error": "events_not_configured",
                        "message": "event ingestion is not configured",
                    },
                )
                return
            try:
                self._json(200, ingest(self._read_json()))
            except EventIngestionDisabled as exc:
                self._json(
                    503,
                    {"error": "events_disabled", "message": str(exc)},
                )
            except EventBackpressure as exc:
                self._json(
                    429,
                    {"error": "event_backpressure", "message": str(exc)},
                )
            except OSError:
                self._json(
                    503,
                    {
                        "error": "event_state_unavailable",
                        "message": "event state could not be persisted",
                    },
                )
            except (EventError, ValueError, TypeError) as exc:
                self._json(
                    400, {"error": "invalid_event", "message": str(exc)}
                )
            return
        if path == "/v1/events/retry":
            if not self._authorized("admin"):
                return
            retry = getattr(self.server.engine, "retry_events", None)
            if retry is None:
                self._json(
                    503,
                    {
                        "error": "events_not_configured",
                        "message": "event ingestion is not configured",
                    },
                )
                return
            try:
                body = self._read_json()
                limit = body.get("limit", 100)
                if isinstance(limit, bool) or not isinstance(limit, int):
                    raise ValueError("limit must be an integer")
                self._json(200, retry(limit))
            except EventIngestionDisabled as exc:
                self._json(
                    503,
                    {"error": "events_disabled", "message": str(exc)},
                )
            except EventBackpressure as exc:
                self._json(
                    429,
                    {"error": "event_backpressure", "message": str(exc)},
                )
            except OSError:
                self._json(
                    503,
                    {
                        "error": "event_state_unavailable",
                        "message": "event state could not be persisted",
                    },
                )
            except (EventError, ValueError) as exc:
                self._json(
                    400, {"error": "invalid_request", "message": str(exc)}
                )
            return
        if path == "/v1/policies/simulate":
            if not self._authorized("admin"):
                return
            simulate = getattr(self.server.engine, "simulate", None)
            if simulate is None:
                self._json(
                    503,
                    {
                        "error": "intelligence_not_configured",
                        "message": "cache intelligence is not available",
                    },
                )
                return
            try:
                self._json(200, simulate(self._read_json()))
            except (ValueError, TypeError) as exc:
                self._json(
                    400, {"error": "invalid_request", "message": str(exc)}
                )
            return
        webhook = self._key_from(path, "/v1/events/webhook/")
        if webhook is not None:
            ingest_webhook = getattr(
                self.server.engine, "ingest_webhook", None
            )
            if ingest_webhook is None:
                self._json(
                    503,
                    {
                        "error": "events_not_configured",
                        "message": "event ingestion is not configured",
                    },
                )
                return
            try:
                body = self._read_body()
                self._json(
                    200,
                    ingest_webhook(webhook, self.headers, body),
                )
            except EventIngestionDisabled as exc:
                self._json(
                    503,
                    {"error": "events_disabled", "message": str(exc)},
                )
            except EventBackpressure as exc:
                self._json(
                    429,
                    {"error": "event_backpressure", "message": str(exc)},
                )
            except WebhookAuthError:
                self._json(401, {"error": "invalid_webhook"})
            except OSError:
                self._json(
                    503,
                    {
                        "error": "event_state_unavailable",
                        "message": "event state could not be persisted",
                    },
                )
            except (EventError, ValueError, TypeError) as exc:
                self._json(
                    400, {"error": "invalid_event", "message": str(exc)}
                )
            return
        key = self._key_from(path, "/v1/fetch/")
        if key is not None:
            if not self._authorized("read", (key,)):
                return
            if not self._authorized("write", (key,)):
                return
            fetch = getattr(self.server.engine, "fetch", None)
            if fetch is None:
                self._json(
                    503,
                    {
                        "error": "origins_not_configured",
                        "message": "HTTP origins are not configured",
                    },
                )
                return
            try:
                body = self._read_json()
                if "origin" not in body or "path" not in body:
                    raise ValueError("origin and path are required")
                refresh = body.get("refresh", False)
                if not isinstance(refresh, bool):
                    raise ValueError("refresh must be a boolean")
                traceparent = self.headers.get("traceparent")
                if traceparent is not None:
                    traceparent = traceparent.lower()
                    if not valid_traceparent(traceparent):
                        raise ValueError("traceparent must be a valid W3C value")
                self._traceparent = traceparent
                result = fetch(
                    key,
                    body["origin"],
                    body["path"],
                    force_refresh=refresh,
                    traceparent=traceparent,
                )
                self._json(200, result.as_json())
            except OriginOverloaded as exc:
                self._json(
                    429,
                    {"error": "origin_overloaded", "message": str(exc)},
                )
            except OriginPolicyError as exc:
                self._json(
                    400,
                    {"error": "origin_policy", "message": str(exc)},
                )
            except OriginUnavailable as exc:
                self._json(
                    503,
                    {"error": "origin_unavailable", "message": str(exc)},
                )
            except QuorumError as exc:
                self._json(
                    503,
                    {"error": "quorum_unavailable", "message": str(exc)},
                )
            except (ValueError, TypeError) as exc:
                self._json(400, {"error": "invalid_request", "message": str(exc)})
            return
        key = self._key_from(path, "/v1/lease/")
        if key is not None:
            if not self._authorized("read", (key,)):
                return
            if not self._authorized("write", (key,)):
                return
            try:
                self._result(self.server.engine.acquire_lease(key))
            except QuorumError as exc:
                self._json(
                    503,
                    {"error": "quorum_unavailable", "message": str(exc)},
                )
            except ValueError as exc:
                self._json(400, {"error": "invalid_request", "message": str(exc)})
            return
        if path == "/v1/invalidate":
            if not self._authorized("invalidate"):
                return
            try:
                body = self._read_json()
                if "tags" not in body:
                    raise ValueError("tags is required")
                count = self.server.engine.invalidate_tags(body["tags"])
                self._json(200, {"invalidated": count})
            except QuorumError as exc:
                self._json(
                    503,
                    {"error": "quorum_unavailable", "message": str(exc)},
                )
            except (ValueError, TypeError) as exc:
                self._json(400, {"error": "invalid_request", "message": str(exc)})
            return
        self._json(404, {"error": "route_not_found"})

    def do_DELETE(self) -> None:
        self._request_started = time.perf_counter()
        self.close_connection = True
        path = urlsplit(self.path).path
        key = self._key_from(path, "/v1/cache/")
        if key is None:
            self._json(404, {"error": "route_not_found"})
            return
        if not self._authorized("write", (key,)):
            return
        try:
            deleted = self.server.engine.delete(key)
            self._json(200 if deleted else 404, {"deleted": deleted})
        except QuorumError as exc:
            self._json(
                503,
                {"error": "quorum_unavailable", "message": str(exc)},
            )
        except ValueError as exc:
            self._json(400, {"error": "invalid_request", "message": str(exc)})

    def log_message(self, message: str, *args: Any) -> None:
        return

    def _authorized(self, permission: str, keys: tuple = ()) -> bool:
        principal = self._authenticate()
        if principal is None:
            self._json(401, {"error": "unauthorized"})
            return False
        if not principal.allows(permission, keys):
            self._json(403, {"error": "forbidden"})
            return False
        self._principal = principal
        return True

    def _authenticate(self) -> Optional[Principal]:
        anonymous = self.server.auth.anonymous()
        if anonymous is not None:
            return anonymous
        supplied = self.headers.get("Authorization", "")
        if supplied.startswith("Bearer "):
            return self.server.auth.authenticate(None, supplied[7:])
        if supplied.startswith("Basic "):
            try:
                decoded = base64.b64decode(
                    supplied[6:], validate=True
                ).decode("utf-8")
                username, password = decoded.split(":", 1)
            except (ValueError, UnicodeDecodeError):
                return None
            return self.server.auth.authenticate(username, password)
        return None

    def _read_json(self) -> Dict[str, Any]:
        raw = self._read_body()
        try:
            value = json.loads(raw)
        except (json.JSONDecodeError, UnicodeDecodeError) as exc:
            raise ValueError("body must be valid JSON") from exc
        if not isinstance(value, dict):
            raise ValueError("body must be a JSON object")
        return value

    def _read_body(self) -> bytes:
        raw_length = self.headers.get("Content-Length")
        if raw_length is None:
            raise ValueError("Content-Length is required")
        try:
            length = int(raw_length)
        except ValueError as exc:
            raise ValueError("Content-Length must be an integer") from exc
        if length < 0 or length > self.server.config.max_body_bytes:
            raise ValueError("request body exceeds configured limit")
        return self.rfile.read(length)

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
            json.dumps(
                self._json_compatible(value), separators=(",", ":")
            ).encode("utf-8"),
            "application/json",
        )

    @classmethod
    def _json_compatible(cls, value: Any) -> Any:
        if isinstance(value, bytes):
            return {
                "$binary": base64.b64encode(value).decode("ascii"),
                "$encoding": "base64",
            }
        if isinstance(value, dict):
            return {key: cls._json_compatible(item) for key, item in value.items()}
        if isinstance(value, (list, tuple)):
            return [cls._json_compatible(item) for item in value]
        return value

    def _send(self, status: int, payload: bytes, content_type: str) -> None:
        duration = time.perf_counter() - self._request_started
        if not self._request_observed:
            self._request_observed = True
            operation = self._operation_name()
            success = status < 400
            self.server.engine.observe_request(
                "http", operation, duration, success
            )
            LOG.info(
                "request",
                extra={
                    "protocol": "http",
                    "operation": operation,
                    "status": status,
                    "duration_ms": round(duration * 1000, 3),
                    "remote": self.client_address[0],
                    "username": getattr(
                        getattr(self, "_principal", None), "username", None
                    ),
                    "traceparent": self._traceparent,
                },
            )
        self.send_response(status)
        self.send_header("Content-Type", content_type)
        self.send_header("Content-Length", str(len(payload)))
        self.send_header("Cache-Control", "no-store")
        if self.server.is_draining:
            self.close_connection = True
        if self.close_connection:
            self.send_header("Connection", "close")
        self.end_headers()
        self.wfile.write(payload)

    def _has_request_body(self) -> bool:
        transfer_encoding = self.headers.get("Transfer-Encoding")
        content_length = self.headers.get("Content-Length")
        if transfer_encoding is not None:
            return True
        if content_length is None:
            return False
        try:
            return int(content_length) != 0
        except ValueError:
            return True

    def _operation_name(self) -> str:
        path = urlsplit(self.path).path
        if path in (
            "/healthz",
            "/readyz",
            "/metrics",
            "/v1/stats",
            "/v1/origins",
            "/v1/events",
            "/v1/events/status",
            "/v1/events/retry",
            "/v1/policies/recommendations",
            "/v1/policies/simulate",
            "/v1/experiments",
        ):
            return "{} {}".format(self.command, path)
        for prefix, route in (
            ("/v1/cache/", "/v1/cache/{key}"),
            ("/v1/lease/", "/v1/lease/{key}"),
            ("/v1/fetch/", "/v1/fetch/{key}"),
            ("/v1/explain/", "/v1/explain/{key}"),
        ):
            if path.startswith(prefix):
                return "{} {}".format(self.command, route)
        if path == "/v1/invalidate":
            return "{} /v1/invalidate".format(self.command)
        if path.startswith("/v1/events/webhook/"):
            return "{} /v1/events/webhook/{{source}}".format(self.command)
        return "{} unknown".format(self.command)
