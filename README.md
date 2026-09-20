# MegaCache

**Caching that stays fresh.**

MegaCache is a correctness-aware, in-memory cache for database-backed APIs and
internal services. It provides the primitives applications usually have to
build around Redis or Memcached themselves: stale reads, request coalescing,
lease-protected refreshes, tag invalidation, bounded memory, and useful
operational metrics. Clients can use either Redis-compatible RESP2 commands or
the HTTP API.

> MegaCache is an alpha release. Version 0.8 adds supported dependency-free
> Python, Node.js, Go, and Java SDKs, a shared conformance suite, bounded L1
> caching with L2 lease coordination, invalidation polling, and W3C
> `traceparent` propagation hooks.
> Cluster coordination remains in-process: separately deployed processes do
> not form a cluster.

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
- **Declarative HTTP origins** provide SSRF-safe read-through fetching without
  accepting arbitrary URLs.
- **Bounded origin work** combines coordinator-wide singleflight, refresh
  workers, concurrency and queue limits, retry budgets, and circuit breakers.
- **Freshness policies** support refresh-ahead, stale-while-revalidate,
  stale-if-error, and negative caching.
- **Durable freshness events** provide replay checkpoints, idempotency,
  bounded dead letters, key/tag transformations, and dependency invalidation.
- **Honest CDC integration boundaries** provide externally-fed Kafka,
  PostgreSQL, MySQL, and MongoDB adapters without pretending to bundle their
  network clients.
- **Zero runtime dependencies** keeps deployment and auditing simple.
- **Four supported SDKs** share RESP2 semantics, typed errors, bounded local
  caches, request coalescing, and trace-context hooks.

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
mc fetch product:123 catalog /v1/products/123
mc ttl product:123
mc invalidate catalog
mc topology
mc topology product:123
mc status
mc event change.json
mc events-status
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

## SDKs

Supported clients live in [`sdk/`](sdk/README.md):

```python
from megacache import CachePolicy, MegaCacheClient

with MegaCacheClient() as cache:
    user = cache.get_or_load(
        "user:42",
        lambda: b'{"id":42}',
        CachePolicy(ttl_seconds=60, stale_seconds=300, tags=("users",)),
    )
```

Node.js 18+, Go 1.20+, and Java 11+ clients expose the same RESP2 and
MegaCache operation set. Run all locally available conformance suites with
`make conformance`.

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
| `MEGACACHE_ORIGINS_FILE` | unset | JSON file containing declarative HTTP origins |
| `MEGACACHE_ORIGIN_WORKER_THREADS` | `2` | Background refresh worker count |
| `MEGACACHE_ORIGIN_REFRESH_QUEUE_SIZE` | `1000` | Bounded background refresh queue |
| `MEGACACHE_ORIGIN_GLOBAL_MAX_CONCURRENCY` | `64` | Process-wide active origin-request limit |
| `MEGACACHE_ORIGIN_GLOBAL_MAX_QUEUE` | `256` | Process-wide waiting origin-request limit |
| `MEGACACHE_EVENTS_FILE` | unset | Event rules, schemas, namespaces, dependencies, and webhooks |
| `MEGACACHE_EVENT_STATE_FILE` | `megacache-events-state.json` | Durable checkpoint, deduplication, replay, and DLQ state |
| `MEGACACHE_EVENT_MAX_SEEN` | `10000` | Retained event idempotency identifiers |
| `MEGACACHE_EVENT_MAX_REPLAY_TOKENS` | `10000` | Retained webhook delivery claims |
| `MEGACACHE_EVENT_MAX_DEAD_LETTERS` | `1000` | Maximum dead-letter entries |
| `MEGACACHE_EVENT_MAX_DEAD_LETTER_BYTES` | `8388608` | Maximum serialized bytes across unresolved dead letters |
| `MEGACACHE_EVENT_MAX_STREAMS` | `1000` | Maximum distinct checkpointed source/stream pairs |
| `MEGACACHE_EVENT_MAX_STATE_BYTES` | `16777216` | Maximum serialized durable event-state size |
| `MEGACACHE_EVENT_MAX_PAYLOAD_BYTES` | `1048576` | Maximum serialized payload bytes per event |
| `MEGACACHE_EVENT_MAX_CURSOR_BYTES` | `4096` | Maximum UTF-8 bytes in a native source cursor |
| `MEGACACHE_EVENT_MAX_ERROR_BYTES` | `4096` | Maximum retained UTF-8 bytes per DLQ error |
| `MEGACACHE_EVENT_GRAPH_MAX_NODES` | `10000` | Dependency graph node limit |
| `MEGACACHE_EVENT_GRAPH_MAX_EDGES` | `50000` | Dependency graph edge limit |
| `MEGACACHE_EVENT_GRAPH_MAX_FANOUT` | `100` | Dependents allowed per key |
| `MEGACACHE_EVENT_GRAPH_MAX_DEPTH` | `16` | Invalidation traversal depth |
| `MEGACACHE_EVENT_GRAPH_MAX_INVALIDATION_NODES` | `10000` | Keys visited per event |

The native client also reads `MEGACACHE_CLI_HOST`,
`MEGACACHE_CLI_PORT`, `MEGACACHE_CLI_USERNAME`,
`MEGACACHE_CLI_PASSWORD`, `MEGACACHE_CLI_TLS`,
`MEGACACHE_CLI_CA_FILE`, and `MEGACACHE_CLI_SERVER_NAME`.

Use `mc init-users users.json` and configure `MEGACACHE_USERS_FILE` for every
network-accessible deployment. `MEGACACHE_API_KEY` remains available for
legacy administrator access. Health and metrics endpoints remain public so
infrastructure probes can reach them; restrict `/metrics` at the network or
reverse-proxy layer.

To enable read-through fetching, copy `origins.example.json`, replace its
authority and policy, and set `MEGACACHE_ORIGINS_FILE`. Every origin requires
exact host, port, and path-prefix allowlists. Non-public, multicast,
reserved, NAT64, IPv4-mapped/translatable, and transition addresses are
rejected unless covered by an explicit `allowed_ip_networks` CIDR; embedded
IPv4 addresses follow the same policy. `mc fetch` accepts only a configured
origin name and an allowed absolute path—not a URL.

Configure freshness automation with `events.example.json`; see
[Freshness events and CDC adapters](docs/events.md). MegaCache provides
dependency-free adapter contracts and does not ship Kafka or database wire
clients.

## Documentation

- [Command reference](docs/commands.md)
- [RESP2 and Redis command reference](docs/resp.md)
- [HTTP API](docs/api.md)
- [HTTP origin configuration](docs/origins.md)
- [Freshness events and CDC adapters](docs/events.md)
- [Architecture and guarantees](docs/architecture.md)
- [Operations and deployment](docs/operations.md)
- [Implementation roadmap](docs/roadmap.md)
- [SDKs and framework integrations](docs/sdks.md)
- [Versioning policy](docs/versioning.md)
- [Contributing](CONTRIBUTING.md)
- [Security policy](SECURITY.md)

## Roadmap

Phase 1 production foundations are available in version 0.4, Phase 2
coordinator behavior in 0.5, Phase 3 HTTP origin protection in 0.6, Phase 4
freshness automation in 0.7, and Phase 5 developer tooling in 0.8.
Secure inter-process transport remains a documented boundary rather than a
simulated guarantee. Cache intelligence and the managed control plane remain on the
[implementation roadmap](docs/roadmap.md).

## License

Licensed under the [MIT License](LICENSE).
