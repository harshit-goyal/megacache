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

## Phase 3: origin protection

Deliverables:

- Declarative HTTP and database origin definitions
- Read-through `mc fetch` operation
- Cluster-wide singleflight and refresh ownership
- Refresh-ahead and stale-while-revalidate workers
- Stale-if-error and negative-cache policies
- Per-origin concurrency budgets, queues, and timeouts
- Retry budgets with exponential backoff and jitter
- Circuit breakers and origin health metrics
- Admission control and load shedding

Acceptance criteria:

- Concurrent misses for one key produce one origin request across the cluster.
- Origin failure serves stale data only within configured safety windows.
- Cache failure cannot create unbounded origin concurrency.

## Phase 4: freshness automation

Deliverables:

- Kafka and webhook event ingestion
- PostgreSQL logical replication connector
- MySQL binlog and MongoDB change-stream connectors
- Event-to-key and event-to-tag transformation rules
- Dependency graph for derived cached values
- Schema identifiers and compatible reader ranges
- Versioned namespaces and rolling-deployment migration policies
- Replay checkpoints, dead-letter handling, and idempotency

Acceptance criteria:

- Connector restart resumes without silently skipping committed changes.
- Duplicate events do not cause incorrect state.
- Dependency invalidation is bounded, observable, and cycle-safe.

## Phase 5: developer platform

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

## Phase 6: cache intelligence

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
