# Implementation roadmap

MegaCache is developed in compatibility-preserving increments. A phase is
complete only when its behavior is implemented, tested, documented, packaged,
and observable. Later phases build on—not bypass—the guarantees of earlier
phases.

## Phase 1: production foundation — complete in 0.4

- Entry-count, estimated total-byte, and per-entry limits
- Bounded, byte-accounted refresh leases
- LRU eviction under either capacity limit
- Ordered RESP command pipelining
- TLS 1.2+ for HTTP and RESP and certificate verification in `mc`
- Named users with slow password hashes
- Bounded password-verification concurrency and successful-auth caching
- Read, write, invalidation, and administrator permissions
- Optional key-prefix restrictions
- Structured JSON request logs without sensitive arguments
- Prometheus request counters and latency histograms
- Coordinated `SIGINT` and `SIGTERM` shutdown with a bounded handler drain

Remaining validation before declaring the alpha production-ready still
includes sustained load testing, fuzzing, external security review, and
published performance envelopes.

## Phase 2: distributed operation — coordinator complete in 0.5

Deliverables:

- A storage interface separating protocol behavior from local memory
- Node identities, membership heartbeats, and failure detection
- Versioned consistent-hash rings with virtual nodes
- Replication with configurable replica count
- Quorum reads and writes with explicit consistency profiles
- Fenced leadership and automatic failover
- Distributed refresh leases and tag indexes
- Online node addition, removal, draining, and rebalancing
- Snapshot/bootstrap transfer with checksums and backpressure

Version 0.5 implements these behaviors behind `StorageBackend` and
`ClusterStorage` as a deterministic in-process coordinator. It includes
versioned rings, logical membership failure detection, consistency profiles,
fencing, distributed leases/tag invalidation, online rebalance plans, bounded
snapshot transfer, observability, and native topology/status commands.

Authenticated encrypted node RPC, discovery, and independent-process failure
domains are intentionally not simulated and remain required before MegaCache
can claim a multi-host distributed deployment.

Acceptance criteria:

- A three-logical-node coordinator survives one modeled node loss without
  acknowledged-write loss under quorum mode.
- Rebalancing does not expose partially transferred entries.
- Missing-heartbeat behavior is deterministic and documented; real network
  partitions are outside version 0.5 because there is no node RPC transport.
- The CLI reports topology, ownership, replication lag, and degraded state.

## Phase 3: HTTP origin protection — complete in 0.6

Deliverables:

- Declarative HTTP origin definitions with exact authority, path, and network
  allowlists
- Read-through `mc fetch` operation
- Single-coordinator singleflight and refresh ownership
- Refresh-ahead and stale-while-revalidate workers
- Stale-if-error and negative-cache policies
- Per-origin concurrency budgets, queues, and timeouts
- Retry budgets with exponential backoff and jitter
- Circuit breakers and origin health metrics
- Admission control and load shedding

Version 0.6 implements these behaviors in `OriginCache`, which wraps either
`CacheEngine` or `ClusterStorage` without changing version 0.5 constructors or
storage operations. HTTP origins are startup-loaded declarations. Clients can
select a named origin and an allowed path but can never supply a URL.

SSRF controls require exact host and port allowlists, normalized path prefixes,
and explicit CIDRs for non-public addresses. DNS is checked on every attempt,
connections are pinned to validated addresses, HTTPS preserves hostname
verification, and redirects are not followed.

Singleflight is cluster-wide only within the current in-process
`ClusterStorage` coordinator boundary. Independently deployed processes do not
coordinate flights, breakers, retry tokens, or concurrency budgets. Database
origin adapters are intentionally not claimed in 0.6; database change-stream
integration remains part of Phase 4 rather than being simulated through an
unsafe generic connector.

Acceptance criteria:

- Concurrent misses for one key produce one origin request across all logical
  nodes sharing one coordinator.
- Origin failure serves stale data only within configured safety windows.
- Cache failure cannot create unbounded origin concurrency.

Tests cover concurrent coordinator miss coalescing, refresh-ahead,
stale-while-revalidate, stale-if-error boundaries, negative responses, retry
budgets and backoff, concurrency and queue shedding, breaker transitions,
timeouts, response limits, redirect handling, SSRF policy, and shutdown.

## Phase 4: freshness automation — complete in 0.7

Deliverables:

- Kafka and webhook event ingestion
- PostgreSQL logical replication connector
- MySQL binlog and MongoDB change-stream connectors
- Event-to-key and event-to-tag transformation rules
- Dependency graph for derived cached values
- Schema identifiers and compatible reader ranges
- Versioned namespaces and rolling-deployment migration policies
- Replay checkpoints, dead-letter handling, and idempotency

Version 0.7 implements a normalized event envelope, declarative key/tag
transforms, bounded cycle-safe dependency invalidation, compatible schema
readers and explicit migrations, rolling versioned namespaces, durable atomic
checkpoints, bounded deduplication, and a retryable bounded dead-letter queue.
Authenticated HTTP/RESP ingestion and HMAC-SHA256 webhooks share the same core.

Kafka integration is an implementable `RecordConsumer` contract and record
adapter. PostgreSQL logical replication, MySQL binlog, and MongoDB change-stream
integrations are externally-fed adapter interfaces. MegaCache intentionally
does not claim native wire clients, consumer-group coordination, replication
slot management, or database-driver behavior because version 0.7 retains zero
runtime dependencies.

Acceptance criteria:

- Connector restart resumes without silently skipping committed changes.
- Duplicate events do not cause incorrect state.
- Dependency invalidation is bounded, observable, and cycle-safe.

Tests cover restart resume, duplicates, replay and out-of-order input, webhook
authentication/replay, schema compatibility and migrations, dependency graph
bounds, connector record mapping, and dead-letter retry metadata.

## Phase 5: developer platform — complete in 0.8

Deliverables:

- Supported Python, Node.js, Go, and Java SDKs
- Shared conformance suite across every SDK
- Local L1 plus distributed L2 cache coordination
- Request coalescing and stale policies in each SDK
- OpenTelemetry traces, metrics, and context propagation
- Framework integrations for common HTTP and database stacks
- Typed policy and error models with semantic versioning

Acceptance criteria:

- SDK behavior matches the protocol conformance suite.
- L1 invalidation reaches healthy clients within a documented bound.
- Trace context follows client, MegaCache, and origin operations.

Version 0.8 ships dependency-free Python 3.9+, Node.js 18+, Go 1.20+, and Java
11+ SDKs. Their common surface covers RESP2 string operations and native
fetch, lease, invalidate, status, invalidation-cursor, and trace-context
commands. Each SDK has bounded entry/byte L1 storage, per-key in-process
coalescing, stale-if-error, and distributed lease completion.

Active clients poll `MC.INVALIDATIONS` before an L1 read once the configured
interval has elapsed and clear L1 when the monotonic server mutation cursor
changes. The next-read bound is the default one-second interval plus one
successful network round trip; applications may lower the interval or
explicitly invalidate after local writes. This deliberately bounded,
metadata-free mechanism favors correctness over selective eviction.

The shared conformance runner starts a real server and exercises every SDK.
W3C `traceparent` is accepted without an OpenTelemetry dependency, attached to
server observations, and forwarded to configured HTTP origins.

## Phase 6: cache intelligence — complete in 0.9

Deliverables:

- Adaptive TTL based on mutation and access history
- Cost-aware eviction using size, popularity, origin cost, and latency
- Hot-key detection and selective replication
- Refresh prioritization under constrained origin budgets
- `mc explain` with freshness, lineage, policy, and eviction reasoning
- Safe recommendations with simulation before activation
- Per-policy experiments and measurable outcome comparison

Acceptance criteria:

- Automated policies can be disabled and rolled back immediately.
- Every automated decision has an explanation and supporting metrics.
- Simulations demonstrate no configured freshness-bound violations.

Version 0.9 implements these features with bounded counters and deterministic
formulas rather than an ML model. LRU and all automated policies remain the
defaults unless explicitly enabled. Adaptive TTL applies to positive HTTP
origin refreshes and can shorten, but never extend, the origin's declared TTL.
Cost-aware eviction is an opt-in engine policy using estimated size, idle time,
access count, and measured origin/load latency.

Hot-key detection can create bounded extra copies only across logical nodes in
the current in-process `ClusterStorage`; those copies do not count toward
quorum or imply multi-host replication. Origin background work uses a bounded
priority queue. Native CLI, HTTP, and RESP explain surfaces return reasons,
evidence, lineage, current policy, and recommendations without cache values.

Offline simulation accepts only a fixed JSON schema and never activates a
policy. Experiments use stable SHA-256 key allocation, explicit sample and
miss-regression guardrails, a bounded local audit, and automatic rollback.
Per-key/class telemetry, simulations, recommendations, and audit history all
have explicit limits, and keys/classes are not exported as metric labels.

## Phase 7: managed control plane

Deliverables:

- Tenant namespaces and cryptographic identity boundaries
- Per-tenant memory, throughput, connection, and origin quotas
- Noisy-neighbor isolation and tenant-aware eviction
- Usage metering with an immutable audit trail
- Billing-provider integration behind a replaceable interface
- Regional placement, backups, upgrades, and disaster recovery
- Control-plane API, web dashboard, alerting, and audit logs
- Data retention, deletion, export, and compliance controls

Acceptance criteria:

- Tenant isolation is covered by independent security testing.
- Metering is reconcilable and idempotent.
- Regional recovery objectives are continuously exercised.
- Control-plane failure does not interrupt healthy data-plane traffic.
