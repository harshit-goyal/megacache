# MegaCache

**Caching that stays fresh.**

MegaCache is a correctness-aware, in-memory cache for database-backed APIs and
internal services. It provides the primitives applications usually have to
build around Redis or Memcached themselves: stale reads, request coalescing,
lease-protected refreshes, tag invalidation, bounded memory, and useful
operational metrics. Clients can use either Redis-compatible RESP2 commands or
the HTTP API.

> MegaCache 1.0 adds an opt-in self-hosted managed control-plane foundation:
> cryptographically bound tenant identities, isolated tenant data planes,
> hard quotas, durable usage and audit records, encrypted backup/restore,
> billing export contracts, deployment/DR metadata, and privacy workflows.
> It does not include a hosted dashboard, external orchestration, a billing
> provider, a cloud KMS, or a multi-process data plane.

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
- **Optional cache intelligence** uses bounded counters and transparent
  formulas—no ML claims, runtime dependencies, untrusted expressions, or
  unbounded key labels.
- **Tenant-isolated data planes** keep keys, tags, origins, events,
  intelligence, eviction, and invalidation cursors out of other tenants.
- **Managed operations metadata** covers quotas, usage, append-only
  tamper-evident audit, encrypted backups, restore drills, rolling desired
  state, billing export, and privacy deletion without pretending to provision
  hosted infrastructure.

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
mc explain product:123
mc recommendations
mc policy-simulate policy-simulation.example.json
mc experiments
mc event change.json
mc events-status
mc identity
mc control-status
mc backup
mc audit
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
| `MEGACACHE_INTELLIGENCE_ENABLED` | `false` | Enable bounded telemetry, explanations, hot-key handling, and policy controls |
| `MEGACACHE_ADAPTIVE_TTL_ENABLED` | `false` | Adapt positive origin TTLs without exceeding their configured TTL |
| `MEGACACHE_INTELLIGENCE_MIN_TTL_SECONDS` | `5` | Lower adaptive TTL clamp |
| `MEGACACHE_INTELLIGENCE_MAX_TTL_SECONDS` | `3600` | Upper adaptive TTL clamp; origin TTL remains the hard ceiling |
| `MEGACACHE_INTELLIGENCE_MAX_KEYS` | `10000` | Maximum retained per-key telemetry records |
| `MEGACACHE_INTELLIGENCE_MAX_CLASSES` | `128` | Maximum retained class aggregates |
| `MEGACACHE_EVICTION_POLICY` | `lru` | `lru` or opt-in deterministic `cost` |
| `MEGACACHE_HOT_KEY_THRESHOLD` | `100` | Accesses required to classify a key as hot |
| `MEGACACHE_HOT_KEY_WINDOW_SECONDS` | `60` | Idle gap that resets hot-key access count |
| `MEGACACHE_HOT_KEY_EXTRA_REPLICAS` | `1` | Best-effort extra in-process logical copies |
| `MEGACACHE_EXPERIMENT_ENABLED` | `false` | Enable deterministic policy experiment allocation |
| `MEGACACHE_EXPERIMENT_ID` | `adaptive-ttl-v1` | Stable experiment allocation identifier |
| `MEGACACHE_EXPERIMENT_ALLOCATION_PERCENT` | `0` | Candidate allocation from 0 through 100 |
| `MEGACACHE_EXPERIMENT_MIN_SAMPLES` | `100` | Required hit/miss observations per arm |
| `MEGACACHE_EXPERIMENT_MAX_MISS_REGRESSION` | `0.05` | Candidate miss-rate regression guardrail |
| `MEGACACHE_INTELLIGENCE_STATE_FILE` | `megacache-intelligence-state.json` | Local atomic experiment decision audit |
| `MEGACACHE_CONTROL_PLANE_FILE` | unset | Opt-in v1 tenant/control-plane definition |
| `MEGACACHE_CONTROL_STATE_DIRECTORY` | `megacache-control-state` | Protected single-owner state, audit, backup, and export directory |
| `MEGACACHE_CONTROL_MASTER_KEY` | unset | Base64 32–64 byte local master key |
| `MEGACACHE_CONTROL_KEY_FILE` | unset | Protected versioned local keyring; mutually exclusive with master key |
| `MEGACACHE_CONTROL_STATE_MAX_BYTES` | `16777216` | Maximum atomic control-state bytes |
| `MEGACACHE_CONTROL_MAX_TENANTS` | `100` | Startup tenant-count bound |
| `MEGACACHE_CONTROL_MAX_USAGE_PERIODS` | `744` | Maximum retained hourly usage periods per tenant |
| `MEGACACHE_CONTROL_MAX_OPERATIONS` | `1000` | Maximum durable asynchronous operations |
| `MEGACACHE_CONTROL_AUDIT_SEGMENT_BYTES` | `1048576` | Audit segment rotation threshold |
| `MEGACACHE_CONTROL_AUDIT_MAX_SEGMENTS` | `32` | Audit segments retained before control mutations fail closed |
| `MEGACACHE_CONTROL_ARTIFACT_MAX_BYTES` | `134217728` | Maximum encrypted backup/export artifact |
| `MEGACACHE_CONTROL_SCHEDULER_INTERVAL_SECONDS` | `30` | Backup and retention scheduler interval |

The native client also reads `MEGACACHE_CLI_HOST`,
`MEGACACHE_CLI_PORT`, `MEGACACHE_CLI_USERNAME`,
`MEGACACHE_CLI_PASSWORD`, `MEGACACHE_CLI_TLS`,
`MEGACACHE_CLI_CA_FILE`, and `MEGACACHE_CLI_SERVER_NAME`.

Use `mc init-users users.json` and configure `MEGACACHE_USERS_FILE` for every
network-accessible deployment. `MEGACACHE_API_KEY` remains available for
legacy administrator access. Health and metrics endpoints remain public so
infrastructure probes can reach them; restrict `/metrics` at the network or
reverse-proxy layer. Managed-mode metrics are aggregate and never use tenant
IDs or cache keys as labels.

Managed mode requires named users, `control-plane.example.json`, a protected
state directory, and exactly one local key source. Run
`mc init-users users.json --tenant default`, then see the
[self-hosted control-plane guide](docs/control-plane.md). Tenant identity comes
from authentication and cannot be selected with a request header or cache-key
prefix.

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
- [Cache intelligence](docs/intelligence.md)
- [Self-hosted managed control plane](docs/control-plane.md)
- [Versioning policy](docs/versioning.md)
- [Contributing](CONTRIBUTING.md)
- [Security policy](SECURITY.md)

## Roadmap

Phase 1 production foundations are available in version 0.4, Phase 2
coordinator behavior in 0.5, Phase 3 HTTP origin protection in 0.6, Phase 4
freshness automation in 0.7, Phase 5 developer tooling in 0.8, and Phase 6
cache intelligence in 0.9. Phase 7's self-hosted control-plane foundation is
complete in 1.0. Secure inter-process transport, hosted UI/service operation,
external orchestration, billing providers, cloud KMS integrations, and
compliance certification remain explicit external boundaries.

## License

Licensed under the [MIT License](LICENSE).
