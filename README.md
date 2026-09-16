# MegaCache

**Caching that stays fresh.**

MegaCache is a correctness-aware, in-memory cache for database-backed APIs and
internal services. It provides the primitives applications usually have to
build around Redis or Memcached themselves: stale reads, request coalescing,
lease-protected refreshes, tag invalidation, bounded memory, and useful
operational metrics.

> MegaCache is an alpha release. This version stores data in one process and is
> intended for evaluation, development, and single-node deployments.

## Why MegaCache?

A conventional cache-aside implementation can overload its origin when a
popular key expires, serve incompatible values after deployments, and leave
related keys stale. MegaCache makes the safe path explicit:

- **Fresh and stale windows** support stale-while-revalidate and
  stale-if-error strategies.
- **Refresh leases** ensure only one client regenerates a missing value.
- **Singleflight loading** coalesces concurrent in-process misses.
- **Tag invalidation** expires related records without key scans.
- **Bounded LRU storage** prevents unbounded process growth.
- **Bearer authentication**, body limits, health endpoints, and Prometheus
  metrics provide a secure operational baseline.
- **Zero runtime dependencies** keeps deployment and auditing simple.

## Quick start

Requires Python 3.9 or newer.

```bash
make test
make run
```

MegaCache listens on `http://localhost:8080`.

```bash
# Store a value for 5 minutes, retain it as stale for another 15 minutes.
curl -X PUT http://localhost:8080/v1/cache/product%3A123 \
  -H 'Content-Type: application/json' \
  -d '{
    "value":{"id":123,"name":"Desk"},
    "ttl_seconds":300,
    "stale_seconds":900,
    "tags":["product:123","catalog"]
  }'

curl http://localhost:8080/v1/cache/product%3A123

curl -X POST http://localhost:8080/v1/invalidate \
  -H 'Content-Type: application/json' \
  -d '{"tags":["catalog"]}'
```

For Docker:

```bash
docker compose up --build
```

## Safe refresh protocol

On a cache miss, ask MegaCache for a refresh lease:

```bash
curl -X POST http://localhost:8080/v1/lease/product%3A123
```

The first caller receives `state: "lease"` and a `lease_token`. Other callers
receive `state: "loading"` and a retry interval. The lease holder computes the
value and writes it with the token:

```bash
curl -X PUT http://localhost:8080/v1/cache/product%3A123 \
  -H 'Content-Type: application/json' \
  -d '{"value":{"id":123},"lease_token":"TOKEN_FROM_LEASE"}'
```

This prevents a cache stampede without trusting a client-side distributed lock.

## Configuration

| Variable | Default | Purpose |
|---|---:|---|
| `MEGACACHE_HOST` | `0.0.0.0` | Bind address |
| `MEGACACHE_PORT` | `8080` | HTTP port |
| `MEGACACHE_MAX_ENTRIES` | `10000` | Maximum entries before LRU eviction |
| `MEGACACHE_MAX_BODY_BYTES` | `1048576` | Maximum JSON request size |
| `MEGACACHE_DEFAULT_TTL_SECONDS` | `300` | Default fresh duration |
| `MEGACACHE_DEFAULT_STALE_SECONDS` | `900` | Default stale duration |
| `MEGACACHE_LEASE_SECONDS` | `30` | Refresh lease duration |
| `MEGACACHE_API_KEY` | unset | Bearer token for `/v1/*` routes |

Set `MEGACACHE_API_KEY` for every network-accessible deployment. Health and
metrics endpoints remain public so infrastructure probes can reach them;
restrict `/metrics` at the network or reverse-proxy layer when necessary.

## Documentation

- [HTTP API](docs/api.md)
- [Architecture and guarantees](docs/architecture.md)
- [Operations and deployment](docs/operations.md)
- [Contributing](CONTRIBUTING.md)
- [Security policy](SECURITY.md)

## Roadmap

- Pluggable Valkey and Redis storage
- Distributed tag indexes and refresh leases
- Event-driven invalidation and database CDC connectors
- Background revalidation and negative-cache policy
- OpenTelemetry traces and language SDKs
- Multi-tier local plus distributed caching

## License

Licensed under the [MIT License](LICENSE).
