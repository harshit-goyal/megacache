# HTTP API

Clients may send a W3C `traceparent` header to `POST /v1/fetch/{key}`.
MegaCache validates it, includes it in request observations, and forwards it
to the configured HTTP origin. Invalid values return `400`. No tracing
dependency is required.

This document covers the HTTP protocol. For `redis-cli` and RESP2 clients, see
the [RESP command reference](resp.md).

All `/v1/*` endpoints except configured webhook routes require application
authentication when an API key or users file is configured. Legacy API keys use `Authorization: Bearer <key>`. Named users use
HTTP Basic authentication over TLS. Named-user permissions and key prefixes
are enforced for every operation. Webhook routes use their own HMAC
credentials. Keys are URL-path components and should be percent encoded.

In managed mode, each named identity is bound to a `tenant_id`. There is no
tenant-selection header: all cache, tag, origin, event, cursor, and
intelligence operations are routed to that identity's independent tenant data
plane. Platform roles can inspect control metadata but do not implicitly
change the tenant used for cache commands.

## Cache entries

### `GET /v1/cache/{key}`

Returns `200` for fresh or stale values and `404` for a miss.

Values written as binary strings through RESP are represented over HTTP as:

```json
{"$binary": "AAEC", "$encoding": "base64"}
```

```json
{
  "state": "fresh",
  "value": {"id": 123},
  "expires_in_seconds": 299.8,
  "stale_for_seconds": 0,
  "lease_token": null,
  "retry_after_seconds": 0
}
```

### `PUT /v1/cache/{key}`

Creates or replaces an entry. `value` is required and may contain any JSON
value. `ttl_seconds` must be positive; `stale_seconds` may be zero.

```json
{
  "value": {"id": 123},
  "ttl_seconds": 300,
  "stale_seconds": 900,
  "tags": ["product:123", "catalog"],
  "lease_token": "optional-refresh-lease"
}
```

Returns `201`. A supplied lease must be current and match the key. When the
server uses `ClusterStorage`, completion also means the configured write
consistency profile was satisfied; otherwise the request returns an
`invalid_request` error describing the unavailable quorum.

### `DELETE /v1/cache/{key}`

Returns `200` when deleted and `404` when the key did not exist.

## Refresh coordination

### `POST /v1/fetch/{key}`

Reads through a named declarative HTTP origin. Both `read` and `write`
permission for the key are required.

```json
{
  "origin": "catalog",
  "path": "/v1/products/123",
  "refresh": false
}
```

`origin` must name an entry loaded from `MEGACACHE_ORIGINS_FILE`. `path` must
be an absolute path inside that origin's explicit path-prefix allowlist;
schemes, authorities, fragments, traversal, and network-path references are
rejected. Clients cannot submit a URL. `refresh` defaults to false.

Successful states are:

- `fresh`: returned from the fresh cache window;
- `refreshed`: one protected origin request populated the cache;
- `stale`: stale-while-revalidate returned immediately and queued refresh;
- `stale_if_error`: a failed refresh returned stale data inside its configured
  safety window;
- `negative`: a configured 4xx response was negatively cached.

The response includes the origin name, origin status when available, attempts,
and value. Origin bodies include `value_encoding` (`utf-8` or `base64`) when
represented in JSON. Overload returns `429`, origin or breaker failure returns
`503`, and policy violations return `400`.

### `POST /v1/lease/{key}`

Possible responses:

- `200 fresh`: use the returned cached value.
- `200 stale_lease`: serve the stale value and refresh it using the included
  `lease_token`.
- `200 stale`: another caller is refreshing; serve the stale value.
- `201 lease`: this caller owns refresh until `expires_in_seconds`; regenerate
  and write the value with `lease_token`.
- `202 loading`: another caller owns refresh; retry after
  `retry_after_seconds`.

## Invalidation

### `POST /v1/invalidate`

Removes every entry associated with at least one supplied tag. Local
`CacheEngine` invalidation is atomic. Cluster invalidation preflights quorum for
the complete matched key set and commits all versioned tombstones atomically;
no matched key is removed when any key cannot satisfy quorum.

```json
{"tags": ["tenant:42", "catalog"]}
```

Returns `{"invalidated": 12}`.

## Cache intelligence

### `GET /v1/explain/{key}`

Requires `read` permission for the key, including named-user key-prefix
restrictions. Returns current freshness windows, estimated size, eviction
policy and score, origin lineage, bounded access/load evidence, reasons, and
the recommended policy. Cached values are never included.

### `GET /v1/policies/recommendations`

Administrator-only bounded recommendations generated from deterministic
counters and thresholds. For administrators with configured key prefixes,
both recommendation rows and `tracked_keys` are limited to authorized keys.
The response identifies that no statistical or ML model is used.

### `POST /v1/policies/simulate`

Administrator-only offline dry run. It accepts:

```json
{
  "policy": {
    "min_ttl_seconds": 5,
    "max_ttl_seconds": 600,
    "eviction_policy": "cost",
    "capacity_entries": 1000
  },
  "records": [
    {
      "key": "catalog:product:123",
      "base_ttl_seconds": 300,
      "accesses": 1200,
      "loads": 20,
      "changes": 2,
      "size_bytes": 4096,
      "load_latency_ms": 180,
      "last_access_age_seconds": 2
    }
  ]
}
```

The bounded response compares base and simulated TTL, estimates relative
origin-load change, and counts configured freshness-bound violations. It never
changes live configuration or evaluates caller-supplied expressions.

### `GET /v1/experiments`

Administrator-only experiment allocation, sample counts, miss-rate guardrail,
status, rollback reason, and bounded decision audit.

## Freshness events

### `POST /v1/events`

Submits one normalized event. The caller needs `invalidate` permission.

```json
{
  "event_id": "catalog-1042",
  "source": "postgres",
  "stream": "catalog-slot",
  "position": 1042,
  "cursor": "0/16B6C50",
  "operation": "update",
  "schema_id": "catalog.product@2",
  "payload": {"id": 42, "tenant_id": "acme"}
}
```

The response reports `processed`, `duplicate`, `replayed`, or `dead_letter`
and includes the durable checkpoint and invalidation counts. Capacity pressure
returns `429 event_backpressure` without advancing the checkpoint. If no event
file is configured, or the configured file has no rules, ingestion returns a
clear `503` disabled/not-configured response.

### `POST /v1/events/webhook/{source}`

Webhook routes use their configured source secret rather than normal HTTP
authentication. Required headers are:

```text
X-MegaCache-Timestamp: UNIX_SECONDS
X-MegaCache-Delivery: UNIQUE_DELIVERY_ID
X-MegaCache-Signature: sha256=HEX_HMAC
```

The HMAC-SHA256 input is `megacache-webhook-v2|` followed by four
length-framed fields in order: source, timestamp header, delivery header, and
the exact body. Each frame is the ASCII decimal byte length, `:`, then the
unmodified bytes. Signatures use constant-time comparison. Timestamp tolerance
and durable delivery claims prevent replay. Live claims are never evicted;
claim-capacity pressure returns `429`. Authentication failures return a
deliberately generic `401`.
In managed mode, every configured webhook source may belong to only one
tenant, and that authenticated source is routed to the tenant's independent
event state.

### `GET /v1/events/status`

Administrator-only status containing checkpoints and native cursors, retained
deduplication and replay counts, DLQ depth, rule/schema/namespace configuration,
graph/state bounds, current state and DLQ bytes, event metrics, and an explicit
`enabled` boolean. Checkpoint map keys are opaque v2 length-framed identifiers;
each checkpoint value also contains its source and stream.

### `POST /v1/events/retry`

Administrator-only retry of due retryable dead letters:

```json
{"limit": 100}
```

See [Freshness events and CDC adapters](events.md) for the normalized envelope,
state semantics, adapters, and configuration.

## Operations

- `GET /healthz`: process liveness.
- `GET /readyz`: readiness; returns `503` with `degraded` when the configured
  cluster consistency profile cannot be met.
- `GET /metrics`: Prometheus text exposition.
- `GET /v1/stats`: JSON metric snapshot.
- `GET /v1/origins`: administrator-only origin health and breaker state.
- `GET /v1/events/status`: administrator-only freshness ingestion status.
- `GET /v1/explain/{key}`: authorized per-key policy explanation.
- `GET /v1/policies/recommendations`: administrator-only recommendations.
- `POST /v1/policies/simulate`: administrator-only dry-run simulation.
- `GET /v1/experiments`: administrator-only guardrail and rollback status.

Errors are JSON objects with a stable `error` identifier and, for invalid
requests, a human-readable `message`.

## Managed control plane

These routes exist only when `MEGACACHE_CONTROL_PLANE_FILE` is configured.
They return JSON; no browser dashboard is bundled.

| Method and route | Role | Behavior |
|---|---|---|
| `GET /v1/control/identity` | authenticated | Bound tenant, opaque namespace, permissions, and roles |
| `GET /v1/control/status[?tenant=ID]` | tenant/platform admin or operator | Tenant dashboard, quotas, usage, desired/observed state, backup/DR status, operations, and alerts |
| `GET /v1/control/tenants` | platform admin/operator | Bounded tenant metadata list |
| `GET /v1/control/orchestrator` | platform admin/operator | Desired/observed reconciliation document |
| `GET /v1/control/operations/{id}` | owning tenant admin or platform/operator | Durable asynchronous operation status |
| `GET /v1/control/audit?tenant=ID&after=N&limit=N` | tenant admin/auditor/platform admin | Verified bounded audit export |
| `PUT /v1/control/tenants/{id}/deployment` | platform admin/operator | Set generation-numbered desired rolling/drain state |
| `POST /v1/control/tenants/{id}/observations` | platform admin/operator | Report bounded observed deployment state |
| `POST /v1/control/tenants/{id}/backups` | tenant/platform admin | Queue encrypted backup |
| `POST /v1/control/tenants/{id}/exports` | tenant/platform admin | Queue encrypted tenant-data export |
| `POST /v1/control/tenants/{id}/restore-validations` | tenant/platform admin | Queue decrypt/schema/quota validation |
| `POST /v1/control/tenants/{id}/restores` | tenant/platform admin | Queue replacement restore |
| `POST /v1/control/tenants/{id}/drills` | tenant/platform admin | Queue non-mutating local DR drill |
| `POST /v1/control/tenants/{id}/deletion-challenge` | tenant/platform admin | Issue short-lived deletion challenge |
| `POST /v1/control/tenants/{id}/delete` | tenant/platform admin | Queue irreversible deletion |
| `POST /v1/control/tenants/{id}/audit-exports` | tenant admin/auditor/platform admin | Queue encrypted audit artifact |
| `POST /v1/control/audit/prune` | auditor/platform admin | Prune fully exported segments using an exact sequence/hash boundary |
| `POST /v1/control/billing/exports` | billing/platform admin | Prepare deterministic provider-neutral usage batch |

Asynchronous responses return `202` and an `operation_id`. Poll the operation
route until `succeeded` or `failed`. An optional `idempotency_key` makes
submission retry-safe while the bounded operation record is retained.

Restore bodies require `backup_id`, `validation_token`,
`confirm_tenant_id`, and `"irreversible": true`. Deletion bodies require the
issued `challenge`, exact `confirm_tenant_id`, and `"irreversible": true`.
Audit pruning requires `through_sequence`, `expected_hash`, and
`"irreversible": true`; only whole segments fully covered by that verified
global export boundary are removed.
Billing export returns raw usage units and a deterministic `batch_id`; it does
not submit charges to a provider.

Tenant throughput/connection exhaustion returns `429`; maintenance, suspended,
or deletion-pending tenants return `409`. Unknown or unauthorized cross-tenant
identifiers use non-enumerating `404`/`403` responses.
