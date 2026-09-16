# Architecture and guarantees

## Components

The HTTP and RESP2 servers depend on the structural `StorageBackend` contract,
not on local-memory implementation details. `CacheEngine` remains the public,
backward-compatible bounded local implementation. `ClusterStorage` implements
the same contract and coordinates one or more `ClusterNode` instances. The
native `mc` CLI connects through RESP2 using the packaged `MegaCacheClient`.

`CacheEngine` owns an ordered entry map, reverse tag index, lease table, flight
table, and counters under one reentrant lock. Existing callers can continue to
construct and call it exactly as in version 0.4. Portable `StorageEntry`
snapshots allow cluster coordination without protocol adapters reaching into
engine internals.

## Distributed coordination

`ClusterStorage` is a well-defined in-process coordinator. Every node has a
stable, validated identity, incarnation, status, storage backend, and most
recent heartbeat. A node becomes unavailable after the configured heartbeat
timeout. Recovery requires a heartbeat with a current or newer incarnation.

The ring uses SHA-256 and a fixed namespace. Node IDs are sorted before
creating virtual-node points, point collisions have deterministic tie-breaks,
and every ring has a monotonically increasing version plus a content
fingerprint. Owners are selected clockwise, once per distinct node.

Each mutation receives an ordered `(term, sequence, leader_id)` version. The
initial leader and failover leader are the lexicographically smallest healthy
eligible node. A healthy leader is not preempted when an earlier node returns.
Leader loss increments the term and invalidates leases. Mutations or rebalance
plans carrying an earlier `FenceToken` are rejected.

The replica count is configurable. Required acknowledgements are:

| Profile | Required responses |
|---|---:|
| `one` | 1 |
| `majority` | floor(replica count / 2) + 1 |
| `all` | every owner |

Reads under `majority` and `all` select the highest version, prefer a live copy
when equal non-tombstone versions disagree physically, and repair older or
physically missing healthy replicas. Writes preflight deterministic admission
on every participating replica before mutation. Common single-key mutations
use targeted journals that capture the affected entry, LRU position, evictions,
leases, accounting, and metrics instead of copying the complete cache. Failed
mutations restore every attempted replica exactly. Validation failures remain
validation errors rather than being reported as unavailable quorum.

Refresh leases and the reverse tag index are owned by the cluster coordinator.
Lease state is fenced by the current leadership term. Tag invalidation
preflights the complete matched key set, then commits versioned tombstones as
one coordinator transaction; `delete_many` has the same transaction-wide
preflight and rollback guarantee.

Tombstones are collected once every current owner is healthy and acknowledges
the deletion. A missing current owner retains the tombstone so stale data
cannot be resurrected. `MEGACACHE_MAX_RETAINED_TOMBSTONES` bounds that backlog;
at the limit, additional deletes that would need retained tombstones fail until
the unavailable owner returns and is repaired or is removed through a completed
rebalance. Compaction advances a version watermark so older staged snapshots
cannot resurrect a collected deletion. Rebalance planning converts
quorum-confirmed physical absence of an expired or fully evicted catalog entry
into a versioned deletion before moving keys.

## Topology changes and snapshots

Joining nodes are excluded from routing until a rebalance completes. Draining
and removal plans calculate a target ring and immutable key moves against a
base ring version and fence. Each move transfers and verifies data first; the
planner requires a healthy source holding the latest metadata version, and old
owners are cleaned only after every target owner verifies that latest version.
The target ring is published only after every move succeeds. Failed plans leave
the old routing table active, so partially transferred entries are not exposed.

Snapshots encode remaining freshness windows, tags, binary values, mutation
versions, and tombstones. SHA-256 protects every chunk and the complete
payload. Configured per-session and per-chunk limits bound memory, and the
pull/ack session refuses more chunks when its in-flight window is full.
Receivers stage and validate all chunks before changing storage. A snapshot is
bound to its leadership fence, and records older than current target or cluster
metadata are skipped rather than overwriting newer state.

The HTTP server listens on port `8080`. The RESP2 server listens on port `6380`
and accepts a documented subset of Redis commands plus `MC.*` extensions.
RESP3, Redis Cluster, Redis replication protocols, Lua, transactions, and
Redis data structures are outside the current compatibility scope.

Each entry has two deadlines:

1. Before `fresh_until`, reads return `fresh`.
2. Between `fresh_until` and `stale_until`, reads return `stale`.
3. At or after `stale_until`, the entry is removed and reads return `miss`.

The entry map is access ordered. Inserting beyond `max_entries` removes the
least recently accessed entry and its tag-index references. Every entry also
has an estimated byte cost covering its encoded key, value, tags, and fixed
metadata. Entries above the per-entry limit are rejected before mutation;
otherwise LRU entries are removed until both entry-count and byte limits are
satisfied.

Plain RESP `SET` creates a persistent entry, matching Redis expiration
semantics, although it remains subject to LRU capacity eviction and process
restart. `SET ... EX`, `EXPIRE`, and MegaCache `MC.SET` create deadlines.

RESP clients may pipeline commands. MegaCache reads and executes them in
connection order and returns responses in that same order.

## Transport and authorization

When a certificate and key are configured, one TLS 1.2-or-newer context wraps
both HTTP and RESP listeners. The native CLI verifies server certificates
against the system trust store or a supplied CA.

Named users are loaded once at server startup. Passwords use salted
PBKDF2-HMAC-SHA256 hashes with at least 100,000 iterations. Permissions are
`read`, `write`, `invalidate`, and `admin`; optional key prefixes further limit
read and write operations. Administrator permission implies every permission.
The legacy API key authenticates an unrestricted administrator for migration.

## Observability

HTTP routes and RESP command names are recorded as bounded operation labels.
The engine exports request counts and Prometheus latency histograms. Logs are
JSON by default and include protocol, operation, status, duration, remote
address, and authenticated username without recording keys, values, passwords,
or command arguments.

## Stampede prevention

Embedded users can call `CacheEngine.get_or_load`. One caller executes the
loader while followers wait for the same result. Exceptions are delivered to
all waiters and are never cached.

Remote clients use refresh leases. Lease ownership is represented by a
cryptographically random token, has a short deadline, and is consumed by a
successful write. The first client requesting a stale key receives both the
stale value and a refresh token as `stale_lease`; concurrent clients continue
receiving the stale value without blocking. A missing key gives one client a
lease while followers receive a retry interval.

## Guarantees

- Engine methods are thread-safe.
- Writes, deletes, tag-index updates, and LRU eviction are atomic in-process.
- A valid lease allows one guarded refresh write per key.
- RESP values are binary-safe bulk strings. HTTP values retain JSON types.
- RESP reads of HTTP-created structured values return compact JSON.
- HTTP reads of RESP-created binary values return a base64-marked object.
- Loader failures are propagated and do not produce success-shaped entries.

## Explicit non-guarantees

Version 0.5 does not ship a node discovery service or authenticated,
encrypted node-to-node RPC transport. `MEGACACHE_CLUSTER_NODES` creates
multiple logical stores inside one process; loss of that process loses every
logical node and all cache data. The coordinator interfaces can model missing
heartbeats and unavailable nodes, but they do not detect or resolve real
network partitions between processes. Do not infer linearizability,
cross-process durability, split-brain prevention, or partition tolerance from
the in-process tests.

Entries are not persisted. Restarting the process empties the cache. Use the
coordinator as an embedded/testable distributed state machine until a secure
transport implements the same interfaces.

Clients remain responsible for deciding whether stale data is safe for their
domain. Never cache authorization decisions, secrets, or correctness-critical
mutable data without a domain-specific invalidation strategy.
