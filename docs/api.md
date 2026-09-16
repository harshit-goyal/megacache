# HTTP API

This document covers the HTTP protocol. For `redis-cli` and RESP2 clients, see
the [RESP command reference](resp.md).

All `/v1/*` endpoints require authentication when an API key or users file is
configured. Legacy API keys use `Authorization: Bearer <key>`. Named users use
HTTP Basic authentication over TLS. Named-user permissions and key prefixes
are enforced for every operation. Keys are URL-path components and should be
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

## Operations

- `GET /healthz`: process liveness.
- `GET /readyz`: readiness; returns `503` with `degraded` when the configured
  cluster consistency profile cannot be met.
- `GET /metrics`: Prometheus text exposition.
- `GET /v1/stats`: JSON metric snapshot.
- `GET /v1/origins`: administrator-only origin health and breaker state.

Errors are JSON objects with a stable `error` identifier and, for invalid
requests, a human-readable `message`.
