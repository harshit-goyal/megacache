# Architecture and guarantees

## Components

The HTTP and RESP2 servers are protocol adapters over one shared `CacheEngine`.
The native `megacache` CLI connects through RESP2 using the packaged
`MegaCacheClient`. The engine owns an ordered entry map, reverse tag index,
lease table, flight table, and counters under one reentrant lock. This makes
compound operations atomic within the process and keeps behavior consistent
across interfaces.

The HTTP server listens on port `8080`. The RESP2 server listens on port `6380`
and accepts a documented subset of Redis commands plus `MC.*` extensions.
RESP3, Redis Cluster, replication, Lua, transactions, and Redis data structures
are outside the current compatibility scope.

Each entry has two deadlines:

1. Before `fresh_until`, reads return `fresh`.
2. Between `fresh_until` and `stale_until`, reads return `stale`.
3. At or after `stale_until`, the entry is removed and reads return `miss`.

The entry map is access ordered. Inserting beyond `max_entries` removes the
least recently accessed entry and its tag-index references.

Plain RESP `SET` creates a persistent entry, matching Redis expiration
semantics, although it remains subject to LRU capacity eviction and process
restart. `SET ... EX`, `EXPIRE`, and MegaCache `MC.SET` create deadlines.

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

Version 0.3 is a single-process cache. It does not replicate data, persist
entries, coordinate multiple MegaCache nodes, encrypt transport, or enforce
durability. Restarting the process empties the cache. Deploy one instance per
isolated workload or put it behind a single-target service until a distributed
backend is available.

Clients remain responsible for deciding whether stale data is safe for their
domain. Never cache authorization decisions, secrets, or correctness-critical
mutable data without a domain-specific invalidation strategy.
