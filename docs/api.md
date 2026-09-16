# HTTP API

This document covers the HTTP protocol. For `redis-cli` and RESP2 clients, see
the [RESP command reference](resp.md).

All `/v1/*` endpoints require `Authorization: Bearer <key>` when
`MEGACACHE_API_KEY` is configured. Keys are URL-path components and should be
percent encoded.

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

Returns `201`. A supplied lease must be current and match the key.

### `DELETE /v1/cache/{key}`

Returns `200` when deleted and `404` when the key did not exist.

## Refresh coordination

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

Atomically removes every entry associated with at least one supplied tag.

```json
{"tags": ["tenant:42", "catalog"]}
```

Returns `{"invalidated": 12}`.

## Operations

- `GET /healthz`: process liveness.
- `GET /readyz`: readiness.
- `GET /metrics`: Prometheus text exposition.
- `GET /v1/stats`: JSON metric snapshot.

Errors are JSON objects with a stable `error` identifier and, for invalid
requests, a human-readable `message`.
