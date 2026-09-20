"""Dependency-free HTTP API for MegaCache."""

import base64
import json
import logging
import time
from dataclasses import asdict
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from typing import Any, Dict, Mapping, Optional
from urllib.parse import parse_qs, unquote, urlsplit

from .auth import AuthManager, Principal
from .cluster import QuorumError
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

    def engine_for(self, principal: Principal) -> StorageBackend:
        if self.control_plane is None:
            return self.engine
        return self.control_plane.engine_for(principal)

class MegaCacheHandler(BaseHTTPRequestHandler):
    server: MegaCacheServer
    protocol_version = "HTTP/1.1"

    def setup(self) -> None:
        super().setup()
        self._connection_tenant = None

    def handle_one_request(self) -> None:
        self._request_observed = False
        self._principal = None
        self._traceparent = None
        self._request_engine = None
        self._request_body_bytes = 0
        self._tenant_operation = None
        self._auth_failure = None
        super().handle_one_request()

    def finish(self) -> None:
        try:
            super().finish()
        finally:
            control = self.server.control_plane
            if control is not None and getattr(
                self, "_tenant_operation", None
            ) is not None:
                control.end_operation(self._tenant_operation)
                self._tenant_operation = None
            if control is not None and getattr(
                self, "_connection_tenant", None
            ) is not None:
                control.release_connection(self._connection_tenant)
                self._connection_tenant = None

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
        if path.startswith("/v1/control/"):
            self._control_get(path)
            return
        if path == "/v1/stats":
            if not self._authorized("admin"):
                return
            self._json(200, self._engine().stats())
            return
        if path == "/v1/origins":
            if not self._authorized("admin"):
                return
            origins = getattr(self._engine(), "origins", None)
            self._json(200, {} if origins is None else origins())
            return
        if path == "/v1/events/status":
            if not self._authorized("admin"):
                return
            status = getattr(self._engine(), "event_status", None)
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
                self._engine(), "recommendations", None
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
            status = getattr(self._engine(), "experiment_status", None)
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
            explain = getattr(self._engine(), "explain", None)
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
                result = self._engine().get(key)
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
        if path.startswith("/v1/control/"):
            self._control_put(path)
            return
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
            result = self._engine().put(
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
        if path.startswith("/v1/control/"):
            self._control_post(path)
            return
        if path == "/v1/events":
            if not self._authorized("invalidate"):
                return
            ingest = getattr(self._engine(), "ingest_event", None)
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
            retry = getattr(self._engine(), "retry_events", None)
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
            simulate = getattr(self._engine(), "simulate", None)
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
            webhook_engine = self.server.engine
            if self.server.control_plane is not None:
                try:
                    tenant_id, webhook_engine = (
                        self.server.control_plane.engine_for_webhook(webhook)
                    )
                    self._bind_tenant(tenant_id)
                    self._admit_tenant_operation(tenant_id)
                    self._request_engine = webhook_engine
                except (TenantNotFound, TenantUnavailable):
                    self._json(401, {"error": "invalid_webhook"})
                    return
                except TenantQuotaExceeded as exc:
                    self._json(
                        429,
                        {"error": "tenant_quota", "message": str(exc)},
                    )
                    return
            ingest_webhook = getattr(webhook_engine, "ingest_webhook", None)
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
            fetch = getattr(self._engine(), "fetch", None)
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
                self._result(self._engine().acquire_lease(key))
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
                count = self._engine().invalidate_tags(body["tags"])
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
            deleted = self._engine().delete(key)
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

    def _engine(self) -> StorageBackend:
        if self._request_engine is not None:
            return self._request_engine
        principal = self._principal
        if principal is None:
            raise TenantUnavailable("request has no authenticated tenant")
        self._request_engine = self.server.engine_for(principal)
        return self._request_engine

    def _bind_tenant(self, tenant_id: str) -> None:
        control = self.server.control_plane
        if control is None:
            return
        if self._connection_tenant is not None:
            if self._connection_tenant != tenant_id:
                raise TenantUnavailable(
                    "one HTTP connection cannot switch tenant identity"
                )
            return
        control.acquire_connection(tenant_id)
        self._connection_tenant = tenant_id

    def _admit_tenant_operation(
        self, tenant_id: str, *, control_operation: bool = False
    ) -> None:
        control = self.server.control_plane
        if control is None or self._tenant_operation is not None:
            return
        control.begin_operation(
            tenant_id, control=control_operation
        )
        self._tenant_operation = tenant_id

    def _control_get(self, path: str) -> None:
        control = self.server.control_plane
        if control is None:
            self._json(404, {"error": "control_plane_not_configured"})
            return
        parsed = urlsplit(self.path)
        query = parse_qs(parsed.query, keep_blank_values=False)
        try:
            if path == "/v1/control/identity":
                principal = self._authenticate()
                if principal is None:
                    self._control_auth_failure()
                    return
                self._admit_tenant_operation(
                    principal.tenant_id, control_operation=True
                )
                self._json(
                    200,
                    {
                        "username": principal.username,
                        "tenant_id": principal.tenant_id,
                        "tenant_namespace": control.namespace_for(
                            principal.tenant_id
                        ),
                        "permissions": sorted(principal.permissions),
                        "roles": sorted(principal.roles),
                    },
                )
                return
            if path == "/v1/control/status":
                requested = self._query_one(query, "tenant")
                target = self._authorize_control(
                    "status", requested, allow_operator=True
                )
                if target is None:
                    return
                self._json(200, control.tenant_status(target))
                return
            if path == "/v1/control/tenants":
                if not self._authorize_global_control(
                    ("platform_admin", "operator")
                ):
                    return
                self._json(200, control.list_tenants())
                return
            if path == "/v1/control/orchestrator":
                if not self._authorize_global_control(
                    ("platform_admin", "operator")
                ):
                    return
                self._json(200, control.orchestrator_status())
                return
            operation_id = self._key_from(
                path, "/v1/control/operations/"
            )
            if operation_id is not None:
                principal = self._authenticate()
                if principal is None:
                    self._control_auth_failure()
                    return
                if principal.has_role("platform_admin") or principal.has_role(
                    "operator"
                ):
                    self._admit_tenant_operation(
                        principal.tenant_id, control_operation=True
                    )
                    value = control.operation_status(operation_id)
                else:
                    if "tenant_admin" not in principal.roles:
                        self._json(403, {"error": "forbidden"})
                        return
                    self._admit_tenant_operation(
                        principal.tenant_id, control_operation=True
                    )
                    value = control.operation_status(
                        operation_id, principal.tenant_id
                    )
                self._json(200, value)
                return
            if path == "/v1/control/audit":
                principal = self._authenticate()
                if principal is None:
                    self._control_auth_failure()
                    return
                requested = self._query_one(query, "tenant")
                if principal.has_role("auditor") or principal.has_role(
                    "platform_admin"
                ):
                    target = (
                        None
                        if requested is None
                        else control.resolve_tenant(principal, requested)
                    )
                elif "tenant_admin" in principal.roles:
                    target = control.resolve_tenant(principal, requested)
                else:
                    self._json(403, {"error": "forbidden"})
                    return
                self._admit_tenant_operation(
                    principal.tenant_id, control_operation=True
                )
                after = int(self._query_one(query, "after") or "0")
                limit = int(self._query_one(query, "limit") or "1000")
                self._json(
                    200,
                    control.export_audit(
                        tenant_id=target,
                        after_sequence=after,
                        limit=limit,
                    ),
                )
                return
            self._json(404, {"error": "route_not_found"})
        except (ValueError, TypeError, OSError) as exc:
            self._control_exception(exc)

    def _control_put(self, path: str) -> None:
        control = self.server.control_plane
        if control is None:
            self._json(404, {"error": "control_plane_not_configured"})
            return
        parts = self._control_tenant_path(path)
        if parts is None or parts[1] != "deployment":
            self._json(404, {"error": "route_not_found"})
            return
        tenant_id = parts[0]
        try:
            principal = self._authenticate()
            if principal is None:
                self._control_auth_failure()
                return
            if not (
                principal.has_role("platform_admin")
                or principal.has_role("operator")
            ):
                self._json(403, {"error": "forbidden"})
                return
            target = control.resolve_tenant(principal, tenant_id)
            self._admit_tenant_operation(
                principal.tenant_id, control_operation=True
            )
            self._json(
                200,
                control.set_desired_deployment(
                    target,
                    self._read_json(),
                    actor=principal.username,
                ),
            )
        except (ValueError, TypeError, OSError) as exc:
            self._control_exception(exc)

    def _control_post(self, path: str) -> None:
        control = self.server.control_plane
        if control is None:
            self._json(404, {"error": "control_plane_not_configured"})
            return
        try:
            principal = self._authenticate()
            if principal is None:
                self._control_auth_failure()
                return
            if path == "/v1/control/billing/exports":
                if not (
                    principal.has_role("billing_admin")
                    or principal.has_role("platform_admin")
                ):
                    self._json(403, {"error": "forbidden"})
                    return
                self._admit_tenant_operation(
                    principal.tenant_id, control_operation=True
                )
                body = self._read_json()
                requested = body.get("tenant_id")
                target = (
                    None
                    if requested is None
                    else control.resolve_tenant(principal, requested)
                )
                self._json(
                    200,
                    control.billing_export(
                        tenant_id=target,
                        start_period=body.get("start_period"),
                        end_period=body.get("end_period"),
                        actor=principal.username,
                    ),
                )
                return
            if path == "/v1/control/audit/prune":
                if not (
                    principal.has_role("auditor")
                    or principal.has_role("platform_admin")
                ):
                    self._json(403, {"error": "forbidden"})
                    return
                self._admit_tenant_operation(
                    principal.tenant_id, control_operation=True
                )
                body = self._read_json()
                if body.get("irreversible") is not True:
                    raise ControlPlaneError(
                        "audit pruning requires irreversible=true"
                    )
                self._json(
                    200,
                    control.prune_audit(
                        through_sequence=body.get("through_sequence"),
                        expected_hash=body.get("expected_hash"),
                        actor=principal.username,
                    ),
                )
                return
            parts = self._control_tenant_path(path)
            if parts is None:
                self._json(404, {"error": "route_not_found"})
                return
            requested, action = parts
            if action == "observations":
                if not (
                    principal.has_role("platform_admin")
                    or principal.has_role("operator")
                ):
                    self._json(403, {"error": "forbidden"})
                    return
            elif action == "audit-exports":
                if not (
                    principal.has_role("platform_admin")
                    or principal.has_role("auditor")
                    or principal.manages_tenant(requested)
                ):
                    self._json(403, {"error": "forbidden"})
                    return
            elif not principal.manages_tenant(requested):
                self._json(403, {"error": "forbidden"})
                return
            target = control.resolve_tenant(principal, requested)
            self._admit_tenant_operation(
                principal.tenant_id, control_operation=True
            )
            body = self._read_json() if self._has_request_body() else {}
            idempotency = body.get("idempotency_key")
            if action == "backups":
                value = control.request_backup(
                    target,
                    actor=principal.username,
                    reason=body.get("reason", "manual"),
                    idempotency_key=idempotency,
                )
                status = 202
            elif action == "exports":
                value = control.request_data_export(
                    target,
                    actor=principal.username,
                    idempotency_key=idempotency,
                )
                status = 202
            elif action == "restore-validations":
                value = control.request_restore_validation(
                    target,
                    body.get("backup_id"),
                    actor=principal.username,
                    idempotency_key=idempotency,
                )
                status = 202
            elif action == "restores":
                if body.get("irreversible") is not True:
                    raise ControlPlaneError(
                        "restore requires irreversible=true"
                    )
                value = control.request_restore(
                    target,
                    body.get("backup_id"),
                    body.get("validation_token"),
                    body.get("confirm_tenant_id"),
                    actor=principal.username,
                    idempotency_key=idempotency,
                )
                status = 202
            elif action == "drills":
                value = control.request_drill(
                    target,
                    body.get("backup_id"),
                    actor=principal.username,
                    idempotency_key=idempotency,
                )
                status = 202
            elif action == "deletion-challenge":
                value = control.create_deletion_challenge(
                    target, actor=principal.username
                )
                status = 200
            elif action == "delete":
                if body.get("irreversible") is not True:
                    raise ControlPlaneError(
                        "tenant deletion requires irreversible=true"
                    )
                value = control.request_tenant_deletion(
                    target,
                    actor=principal.username,
                    challenge=body.get("challenge"),
                    confirmation=body.get("confirm_tenant_id"),
                    idempotency_key=idempotency,
                )
                status = 202
            elif action == "observations":
                value = control.report_observed_deployment(
                    target, body, actor=principal.username
                )
                status = 200
            elif action == "audit-exports":
                value = control.request_audit_export(
                    target,
                    actor=principal.username,
                    after_sequence=body.get("after_sequence", 0),
                    limit=body.get("limit", 1000),
                    idempotency_key=idempotency,
                )
                status = 202
            else:
                self._json(404, {"error": "route_not_found"})
                return
            self._json(status, value)
        except (ValueError, TypeError, OSError) as exc:
            self._control_exception(exc)

    def _authorize_global_control(self, roles: tuple) -> bool:
        principal = self._authenticate()
        if principal is None:
            self._control_auth_failure()
            return False
        if not any(principal.has_role(role) for role in roles):
            self._json(403, {"error": "forbidden"})
            return False
        try:
            self._admit_tenant_operation(
                principal.tenant_id, control_operation=True
            )
        except (TenantQuotaExceeded, TenantUnavailable) as exc:
            self._control_exception(exc)
            return False
        return True

    def _authorize_control(
        self,
        action: str,
        requested: Optional[str],
        *,
        allow_operator: bool = False,
    ) -> Optional[str]:
        principal = self._authenticate()
        if principal is None:
            self._control_auth_failure()
            return None
        control = self.server.control_plane
        assert control is not None
        target = control.resolve_tenant(principal, requested)
        if not principal.manages_tenant(target) and not (
            allow_operator and principal.has_role("operator")
        ):
            self._json(403, {"error": "forbidden"})
            return None
        self._admit_tenant_operation(
            principal.tenant_id, control_operation=True
        )
        return target

    def _control_auth_failure(self) -> None:
        if self._auth_failure == "quota":
            self._json(429, {"error": "tenant_connection_quota"})
        elif self._auth_failure == "tenant":
            self._json(409, {"error": "tenant_unavailable"})
        else:
            self._json(401, {"error": "unauthorized"})

    def _control_exception(self, exc: BaseException) -> None:
        if isinstance(exc, TenantNotFound):
            self._json(404, {"error": "not_found"})
        elif isinstance(exc, TenantQuotaExceeded):
            self._json(429, {"error": "tenant_quota", "message": str(exc)})
        elif isinstance(exc, TenantUnavailable):
            self._json(409, {"error": "tenant_unavailable", "message": str(exc)})
        elif isinstance(exc, ControlPlaneCapacity):
            self._json(
                429, {"error": "control_plane_capacity", "message": str(exc)}
            )
        elif isinstance(exc, OSError):
            self._json(503, {"error": "control_plane_unavailable"})
        else:
            self._json(
                400, {"error": "invalid_control_request", "message": str(exc)}
            )

    @staticmethod
    def _query_one(query: Mapping[str, list], name: str) -> Optional[str]:
        values = query.get(name)
        if not values:
            return None
        if len(values) != 1:
            raise ControlPlaneError(
                "query parameter '{}' must appear once".format(name)
            )
        return values[0]

    @staticmethod
    def _control_tenant_path(path: str) -> Optional[tuple]:
        prefix = "/v1/control/tenants/"
        if not path.startswith(prefix):
            return None
        remainder = path[len(prefix) :]
        parts = remainder.split("/")
        if len(parts) != 2 or not all(parts):
            return None
        return unquote(parts[0]), parts[1]

    def _authorized(self, permission: str, keys: tuple = ()) -> bool:
        principal = self._authenticate()
        if principal is None:
            self._control_auth_failure()
            return False
        if not principal.allows(permission, keys):
            self._json(403, {"error": "forbidden"})
            return False
        try:
            self._admit_tenant_operation(principal.tenant_id)
            self._engine()
        except (TenantQuotaExceeded, TenantUnavailable) as exc:
            self._control_exception(exc)
            return False
        self._principal = principal
        return True

    def _authenticate(self) -> Optional[Principal]:
        if self._principal is not None:
            return self._principal
        anonymous = self.server.auth.anonymous()
        if anonymous is not None:
            principal = anonymous
        else:
            supplied = self.headers.get("Authorization", "")
            if supplied.startswith("Bearer "):
                principal = self.server.auth.authenticate(None, supplied[7:])
            elif supplied.startswith("Basic "):
                try:
                    decoded = base64.b64decode(
                        supplied[6:], validate=True
                    ).decode("utf-8")
                    username, password = decoded.split(":", 1)
                except (ValueError, UnicodeDecodeError):
                    return None
                principal = self.server.auth.authenticate(username, password)
            else:
                return None
        if principal is None:
            return None
        try:
            self._bind_tenant(principal.tenant_id)
        except TenantQuotaExceeded:
            self._auth_failure = "quota"
            return None
        except TenantUnavailable:
            self._auth_failure = "tenant"
            return None
        self._principal = principal
        return principal

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
        self._request_body_bytes = length
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
            observation_engine = self._request_engine or self.server.engine
            observation_engine.observe_request(
                "http", operation, duration, success
            )
            control = self.server.control_plane
            tenant_id = (
                self._tenant_operation
                or getattr(self._principal, "tenant_id", None)
                or self._connection_tenant
            )
            if control is not None and tenant_id is not None:
                control.observe_tenant_request(
                    tenant_id,
                    "http",
                    operation,
                    self._request_body_bytes,
                    len(payload),
                    success,
                    duration,
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
                    "tenant_namespace": (
                        None
                        if control is None or tenant_id is None
                        else control.namespace_for(tenant_id)
                    ),
                    "traceparent": self._traceparent,
                },
            )
            if control is not None and self._tenant_operation is not None:
                control.end_operation(self._tenant_operation)
                self._tenant_operation = None
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
            "/v1/control/identity",
            "/v1/control/status",
            "/v1/control/tenants",
            "/v1/control/orchestrator",
            "/v1/control/audit",
            "/v1/control/billing/exports",
            "/v1/control/audit/prune",
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
        if path.startswith("/v1/control/operations/"):
            return "{} /v1/control/operations/{{id}}".format(self.command)
        if path.startswith("/v1/control/tenants/"):
            return "{} /v1/control/tenants/{{tenant}}/{{action}}".format(
                self.command
            )
        return "{} unknown".format(self.command)
