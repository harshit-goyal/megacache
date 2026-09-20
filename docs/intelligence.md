# Cache intelligence

Version 0.9 adds an optional, dependency-free policy layer based on bounded
counters and deterministic formulas. It is not machine learning. The default
configuration keeps intelligence disabled and preserves LRU eviction.

Enable it explicitly:

```bash
export MEGACACHE_INTELLIGENCE_ENABLED=true
export MEGACACHE_ADAPTIVE_TTL_ENABLED=true
export MEGACACHE_EVICTION_POLICY=lru
```

The complete set of bounds and experiment settings is shown in
[`.env.example`](../.env.example) and the README configuration table.

## Safety model

- Per-key telemetry is LRU-bounded by `MEGACACHE_INTELLIGENCE_MAX_KEYS`.
- Per-class aggregates are LRU-bounded by
  `MEGACACHE_INTELLIGENCE_MAX_CLASSES`.
- Classes are an origin name for origin loads and otherwise the bounded key
  prefix before the first `:`. No key or class is used as a Prometheus label.
- Explanations never contain cached values. Key-bearing explain requests need
  `read` permission for that key, including the user's configured prefixes.
- Recommendation, simulation, and experiment summaries require `admin`.
  Recommendation keys are additionally filtered through the principal's
  configured key prefixes. Explain remains authorized against its requested
  key.
- Policies use fixed arithmetic only. Configuration does not accept source
  code, expressions, callbacks, or templates.

## Adaptive TTL

Adaptive TTL applies only to successful positive `OriginCache` refreshes when
both intelligence and adaptive TTL are enabled. Explicit application writes
and negative-cache TTLs are unchanged.

For each key, MegaCache retains bounded load count, content-change count, an
EWMA of load latency, and access outcomes. Before a positive origin value is
stored, its declared origin TTL is multiplied by a deterministic factor:

| Observed change rate | TTL factor |
|---:|---:|
| at least 0.50 | 0.25 |
| at least 0.25 | 0.50 |
| at least 0.10 | 0.75 |
| below 0.10 | 1.00 |

Fewer than two loads keep the configured TTL. The result is clamped to the
configured intelligence bounds, but the origin's declared TTL is always the
hard ceiling. Automated policy therefore cannot extend a configured freshness
bound. Low observed reuse can conservatively reduce the factor to `0.75` or
`0.50`; it never lengthens it. Content identity is represented only by an
in-memory SHA-256 digest.

## Cost-aware eviction

`MEGACACHE_EVICTION_POLICY=lru` is the default and retains previous behavior.
The opt-in `cost` policy evicts the entry with the lowest deterministic score:

```text
(1 + log(1 + access_count)) * (1 + log(1 + load_latency_ms))
----------------------------------------------------------------
         sqrt(estimated_entry_bytes) * (1 + idle_seconds)
```

Size, reuse, frequency, recency, and measured origin/load cost therefore
contribute without unbounded history. Ties are resolved by key. Existing
entry-count, total-byte, per-entry, quorum, rollback, and atomicity checks still
apply.

## Hot keys and refresh priority

A key is hot after the configured number of accesses without an idle gap
longer than `MEGACACHE_HOT_KEY_WINDOW_SECONDS`. At most the configured number
of extra healthy logical nodes receive a best-effort copy. Extra copies never
count toward write or read quorum and are used only when their version matches
the latest coordinator metadata.

This replication is strictly inside one `ClusterStorage` object. It creates no
cross-process traffic, independent failure domain, or durability. A
single-node deployment has nowhere to create an extra copy.

Origin refresh-ahead and stale-while-revalidate jobs use a bounded priority
queue. Recent, frequently accessed, and expensive-to-load keys receive lower
priority numbers and run first. A queued job that waits one second is promoted
ahead of newer work, bounding starvation during sustained high-priority load.
Queue capacity, per-origin admission, retries, leases, and shutdown semantics
remain unchanged.

## Explain and recommendations

```bash
mc explain catalog:product:123
mc recommendations --limit 50
mc experiments
```

Equivalent protocol surfaces are:

- `GET /v1/explain/{key}`
- `GET /v1/policies/recommendations`
- `GET /v1/experiments`
- `MC.EXPLAIN key`
- `MC.RECOMMENDATIONS [limit]`
- `MC.EXPERIMENTS`

`explain` reports current fresh/stale/miss state, remaining windows, estimated
size, access/load evidence, origin lineage without the value, active eviction
mode, reasons, and a recommended policy. Origin requests retain their complete
query string for the outbound request and cache identity, but lineage stores
and reports only the path so query credentials cannot appear in explanations.
Recommendations are generated from the same bounded evidence and fixed
thresholds.

## Offline simulation

Simulation is dry-run only and never mutates cache or policy:

```bash
mc policy-simulate policy-simulation.example.json
cat policy-simulation.example.json | mc policy-simulate
```

HTTP uses `POST /v1/policies/simulate`; RESP uses
`MC.POLICY.SIMULATE json`. Input contains a `policy` object with TTL bounds and
a bounded `records` array containing `key`, `base_ttl_seconds`, `accesses`,
`loads`, `changes`, and optional size/load-cost/idle evidence. A policy may
also select `lru` or `cost` eviction and an entry capacity. Record order is the
offline LRU order, oldest first. Output lists simulated TTLs, baseline and
proposed evictions, estimated relative origin-load delta, and configured
freshness-bound violations. The built-in policy always reports zero such
violations because base TTL is a hard ceiling.

## Controlled experiments

Experiments are opt-in. Allocation is explicit and stable:

```text
sha256(experiment_id + NUL + key) modulo 100
```

Values below `MEGACACHE_EXPERIMENT_ALLOCATION_PERCENT` enter the candidate arm;
all others are controls. An enabled experiment requires an allocation from 1
through 99 so both arms can satisfy the guardrail; disable experiments for no
candidate traffic.
After both arms reach `MEGACACHE_EXPERIMENT_MIN_SAMPLES`, MegaCache compares
miss rates. If candidate miss rate exceeds control by more than
`MEGACACHE_EXPERIMENT_MAX_MISS_REGRESSION`, the experiment is automatically
rolled back and all keys use control behavior.

Rollback decisions and reasons are written atomically to
`MEGACACHE_INTELLIGENCE_STATE_FILE` with mode `0600` and restored at startup.
The file and its parent directory are synchronized before success is reported.
The audit list is bounded to 100 decisions. A persistence failure never
re-enables the candidate in the running process and is exposed as
`persistence_error` plus
`intelligence_state_write_errors_total`; without a durable decision, however,
a restart can start the configured experiment again. This local file is not a
consensus store; one process should own it.
