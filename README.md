# MegaCache

**Caching that stays fresh.**

MegaCache is a correctness-aware, in-memory cache for database-backed APIs and
internal services. It provides the primitives applications usually have to
build around Redis or Memcached themselves: stale reads, request coalescing,
lease-protected refreshes, tag invalidation, bounded memory, and useful
operational metrics. Clients can use either Redis-compatible RESP2 commands or
the HTTP API.

> MegaCache is an alpha release. Version 0.5 adds a deterministic, testable
> in-process cluster coordinator. It does not yet include authenticated
> node-to-node RPC, so separately deployed processes do not form a cluster.

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
- **Byte-aware admission and eviction** enforce total and per-entry limits.
- **RESP2 compatibility** supports common Redis clients and `redis-cli`.
- **TLS and scoped users** protect HTTP and RESP traffic and restrict commands
  and key prefixes.
- **Structured logs and latency histograms** provide a production observability
  baseline.
- **Versioned consistent hashing and replication** provide explicit `one`,
  `majority`, and `all` consistency profiles across coordinator-managed nodes.
- **Fenced leadership, membership heartbeats, and online rebalancing** make
  logical node failure and topology changes deterministic.
- **Checksummed snapshots with bounded chunks** bootstrap replicas without
  publishing partially transferred ownership.
- **Zero runtime dependencies** keeps deployment and auditing simple.

## Quick start

Requires Python 3.9 or newer.

```bash
make test
python3 -m pip install .
mc serve
```

MegaCache listens for RESP2 on port `6380` and HTTP on port `8080`.

Use the native MegaCache CLI:

```bash
mc ping
mc put product:123 '{"id":123,"name":"Desk"}' \
  --ttl 300 --stale 900 --tag product:123 --tag catalog
mc get product:123
mc ttl product:123
mc invalidate catalog
mc topology
mc topology product:123
mc status
```

Redis clients remain supported through RESP2:

```bash
redis-cli -p 6380 SET compatibility:key value EX 300
redis-cli -p 6380 GET compatibility:key
```

The HTTP API is also available:

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
mc lease product:123
```

The first caller receives `lease` and a token. Other callers receive `loading`
and a retry interval. The lease holder computes the value and writes it with
the token:

```bash
mc put product:123 '{"id":123}' \
  --ttl 300 --stale 900 --lease TOKEN_FROM_LEASE
```

This prevents a cache stampede without trusting a client-side distributed lock.

## Configuration

| Variable | Default | Purpose |
|---|---:|---|
| `MEGACACHE_HOST` | `0.0.0.0` | Bind address |
| `MEGACACHE_PORT` | `8080` | HTTP port |
| `MEGACACHE_RESP_HOST` | `0.0.0.0` | RESP2 bind address |
| `MEGACACHE_RESP_PORT` | `6380` | RESP2 port |
| `MEGACACHE_CLI_HOST` | `127.0.0.1` | Native CLI target host |
| `MEGACACHE_CLI_PORT` | `6380` | Native CLI target port |
| `MEGACACHE_MAX_ENTRIES` | `10000` | Maximum entries before LRU eviction |
| `MEGACACHE_MAX_MEMORY_BYTES` | `67108864` | Estimated total cache memory limit |
| `MEGACACHE_MAX_ENTRY_BYTES` | `1048576` | Maximum estimated size of one entry |
| `MEGACACHE_MAX_BODY_BYTES` | `1048576` | Maximum JSON request size |
| `MEGACACHE_DEFAULT_TTL_SECONDS` | `300` | Default fresh duration |
| `MEGACACHE_DEFAULT_STALE_SECONDS` | `900` | Default stale duration |
| `MEGACACHE_LEASE_SECONDS` | `30` | Refresh lease duration |
| `MEGACACHE_SHUTDOWN_GRACE_SECONDS` | `10` | Maximum handler drain period |
| `MEGACACHE_API_KEY` | unset | Legacy HTTP/RESP administrator secret |
| `MEGACACHE_USERS_FILE` | unset | Named users, permissions and key prefixes |
| `MEGACACHE_TLS_CERT_FILE` | unset | PEM certificate for HTTP and RESP TLS |
| `MEGACACHE_TLS_KEY_FILE` | unset | PEM private key for HTTP and RESP TLS |
| `MEGACACHE_LOG_FORMAT` | `json` | `json` or `text` server logs |
| `MEGACACHE_NODE_ID` | system hostname | Stable identity of this process |
| `MEGACACHE_CLUSTER_NODES` | local node ID | Comma-separated in-process node IDs |
| `MEGACACHE_REPLICA_COUNT` | `1` | Desired replicas per key |
| `MEGACACHE_VIRTUAL_NODES` | `128` | Consistent-hash points per node |
| `MEGACACHE_CONSISTENCY` | `majority` | `one`, `majority`, or `all` |
| `MEGACACHE_HEARTBEAT_INTERVAL_SECONDS` | `2` | Local heartbeat interval |
| `MEGACACHE_HEARTBEAT_TIMEOUT_SECONDS` | `10` | Failure-detection timeout |
| `MEGACACHE_SNAPSHOT_PAYLOAD_LIMIT_BYTES` | `67108864` | Maximum snapshot session payload |
| `MEGACACHE_SNAPSHOT_CHUNK_BYTES` | `262144` | Maximum transfer chunk |
| `MEGACACHE_SNAPSHOT_MAX_IN_FLIGHT` | `2` | Unacknowledged chunk limit |
| `MEGACACHE_MAX_RETAINED_TOMBSTONES` | `10000` | Backpressure limit for deletes awaiting unavailable replicas |

The native client also reads `MEGACACHE_CLI_HOST`,
`MEGACACHE_CLI_PORT`, `MEGACACHE_CLI_USERNAME`,
`MEGACACHE_CLI_PASSWORD`, `MEGACACHE_CLI_TLS`,
`MEGACACHE_CLI_CA_FILE`, and `MEGACACHE_CLI_SERVER_NAME`.

Use `mc init-users users.json` and configure `MEGACACHE_USERS_FILE` for every
network-accessible deployment. `MEGACACHE_API_KEY` remains available for
legacy administrator access. Health and metrics endpoints remain public so
infrastructure probes can reach them; restrict `/metrics` at the network or
reverse-proxy layer.

## Documentation

- [Command reference](docs/commands.md)
- [RESP2 and Redis command reference](docs/resp.md)
- [HTTP API](docs/api.md)
- [Architecture and guarantees](docs/architecture.md)
- [Operations and deployment](docs/operations.md)
- [Implementation roadmap](docs/roadmap.md)
- [Contributing](CONTRIBUTING.md)
- [Security policy](SECURITY.md)

## Roadmap

Phase 1 production foundations are available in version 0.4. Version 0.5
delivers the Phase 2 storage and in-process cluster coordination interfaces.
Secure inter-process transport remains a documented boundary rather than a
simulated guarantee. Origin protection, freshness automation, SDKs, cache
intelligence, and the managed control plane remain on the
[implementation roadmap](docs/roadmap.md).

## License

Licensed under the [MIT License](LICENSE).
