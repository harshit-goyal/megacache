# Operations

## Deployment baseline

Run MegaCache on a private network. Configure its built-in TLS or place it
behind TLS-terminating proxies. Use a named users file, restrict `/metrics` by
network policy, use a read-only container filesystem, and allocate enough
memory for configured byte and entry limits.

Start the process with `mc serve`. `SIGINT` and `SIGTERM` stop both listeners,
signal origin shutdown before waiting on active handlers, allow up to
`MEGACACHE_SHUTDOWN_GRACE_SECONDS` to finish, then close remaining connections
and bounded origin workers. Origin admission, retry backoff, refresh workers,
and coalesced followers observe that signal. It exposes HTTP on port `8080` and
RESP2 on port `6380`. Put both
behind appropriate network controls; use a TCP TLS proxy for RESP when traffic
crosses a trusted boundary. The process runs as an unprivileged user in the
supplied container, logs requests to standard output, and shuts down cleanly.

Version 0.9's cluster is in-process. A comma-separated
`MEGACACHE_CLUSTER_NODES` value creates independent logical storage nodes
managed by one coordinator. This is useful for embedded operation, exercising
replication behavior, and validating failure procedures, but it is not a
multi-host deployment. Do not configure multiple processes with the same list
and expect them to communicate.

For three logical replicas:

```bash
export MEGACACHE_NODE_ID=cache-a
export MEGACACHE_CLUSTER_NODES=cache-a,cache-b,cache-c
export MEGACACHE_REPLICA_COUNT=3
export MEGACACHE_CONSISTENCY=majority
mc serve
```

Every logical node receives the configured per-node entry and memory limits.
Plan process memory for their sum plus transfer buffers.

Origin singleflight is cluster-wide only inside this one coordinator object.
Running the same origin file in multiple processes creates independent
singleflight tables, retry budgets, breakers, and concurrency budgets.

## Freshness event configuration

Copy and edit `events.example.json`, then place event state on durable local
storage:

```bash
export MEGACACHE_EVENTS_FILE="$PWD/events.json"
export MEGACACHE_EVENT_STATE_FILE="/var/lib/megacache/events-state.json"
export MEGACACHE_CATALOG_WEBHOOK_SECRET='replace-with-a-long-random-secret'
mc serve
```

Only one process may own a state file. MegaCache holds a non-blocking advisory
lock on the sibling `.lock` file for the store lifetime; a duplicate owner
fails startup. The state is atomically replaced and fsynced after successful
invalidation or durable dead-letter insertion. Back it up with other
operational metadata, protect both files as mode `0600`, and alert on write or
capacity failures. Do not use an eventually consistent object-store mount.

On connector restart, read the relevant source/stream cursor from
`mc events-status` and reconnect the external Kafka or database driver there.
For Kafka, use `KafkaRecordAdapter` with an application-provided
`RecordConsumer`. PostgreSQL, MySQL, and MongoDB adapters likewise accept
records from native drivers selected and operated by the application.
MegaCache does not manage consumer groups, logical slots, binlog retention,
resume-token expiry, source credentials, or network reconnects. PostgreSQL
drivers must feed each complete transaction/change batch in strict
`(LSN, ordinal)` order and provide a stable zero-based ordinal for every
change, including single-change transactions.

Use unique source/stream pairs and monotonically increasing positions. MySQL
integrations must assign a monotonic sequence across file rotation; MongoDB
integrations must retain the native resume token while providing a monotonic
sequence. Inspect and retry the bounded DLQ with:

```bash
mc events-status
mc events-retry --limit 100
```

DLQ count/byte limits and total state capacity apply backpressure before a
failed event's checkpoint advances; unresolved failures are never evicted.
Lowering limits below existing durable usage causes startup to fail rather than
silently truncating state. Configure stream, payload, cursor, error, DLQ-byte,
and total-state budgets for the expected workload. Leaving
`MEGACACHE_EVENTS_FILE` unset disables event APIs; a file with zero rules is
reported as `enabled: false` and rejects ingestion without checkpointing.

## Origin configuration

Copy `origins.example.json`, narrow its authority and policy, then configure:

```bash
export MEGACACHE_ORIGINS_FILE="$PWD/origins.json"
export MEGACACHE_ORIGIN_WORKER_THREADS=2
export MEGACACHE_ORIGIN_REFRESH_QUEUE_SIZE=1000
export MEGACACHE_ORIGIN_GLOBAL_MAX_CONCURRENCY=64
export MEGACACHE_ORIGIN_GLOBAL_MAX_QUEUE=256
mc serve
```

Per-origin limits live in the JSON definition: timeout, response bytes,
concurrency, queue depth and wait timeout, retries, token budget, backoff,
jitter, circuit-breaker thresholds, freshness windows, and negative statuses.
Definitions are validated and loaded once at startup.

Ordinary public unicast DNS addresses need no network CIDR entry. Private,
loopback, link-local, multicast, reserved, unspecified, NAT64,
IPv4-mapped/translatable, 6to4, Teredo, and other transition addresses are
rejected by default, including unsafe embedded IPv4 addresses. Only an explicit
`allowed_ip_networks` CIDR can permit an exception. Keep those CIDRs as narrow
as possible. MegaCache pins connections to validated addresses and does not
follow redirects, but origin authorization and data classification remain
operator responsibilities.

`max_queue` also bounds followers for each coalesced origin flight, while
`MEGACACHE_ORIGIN_GLOBAL_MAX_QUEUE` bounds followers across flights.
`queue_timeout_seconds` limits their wait. Built-in storage renews refresh
leases for the duration of admitted origin work; tag invalidation removes both
the entry and lease so late refreshes cannot repopulate invalidated keys.

## Capacity planning

`MEGACACHE_MAX_ENTRIES` bounds entry count and
`MEGACACHE_MAX_MEMORY_BYTES` bounds estimated entry bytes.
`MEGACACHE_MAX_ENTRY_BYTES` rejects oversized values before they can evict
useful entries. Estimates include encoded keys, values, tags, and fixed
metadata but not every Python allocator overhead, connection, or request
buffer. Measure representative payloads and leave process-memory headroom.
Keep `MEGACACHE_MAX_BODY_BYTES` close to the largest legitimate request; the
same limit applies to the total RESP command payload.

## Cache intelligence rollout

Keep `MEGACACHE_INTELLIGENCE_ENABLED=false` and
`MEGACACHE_EVICTION_POLICY=lru` during the first baseline capture. Run:

```bash
mc policy-simulate policy-simulation.example.json
```

Then enable bounded telemetry, inspect `mc explain`, and review
`mc recommendations`. Adaptive TTL only shortens positive origin TTLs and
never exceeds an origin's declared freshness bound. Cost-aware eviction is
separate and opt-in.

For a controlled adaptive-TTL experiment, set a stable experiment ID, explicit
candidate allocation, minimum samples per arm, and maximum accepted miss-rate
regression. A regression automatically switches the experiment to
`rolled_back`. Place `MEGACACHE_INTELLIGENCE_STATE_FILE` on protected local
storage; it contains only bounded aggregate decisions, but it is not a
multi-process coordination store. If a rollback cannot be persisted, the
candidate remains disabled in the current process and `persistence_error` plus
`intelligence_state_write_errors_total` report the failure. Because the
decision is then not durable, a restart can re-enable the configured
experiment.

Hot-key extra copies and refresh priority remain bounded. Extra copies are
strictly in-process, are version checked before reads, and never satisfy
quorum. Monitor tracked telemetry capacity, telemetry evictions, hot-copy
updates, and experiment rollbacks. See
[Cache intelligence](intelligence.md) for formulas and limits.

## TLS

Set both variables; setting only one prevents startup:

```bash
export MEGACACHE_TLS_CERT_FILE=/run/secrets/tls.crt
export MEGACACHE_TLS_KEY_FILE=/run/secrets/tls.key
mc serve
```

Both HTTP and RESP listeners then require TLS 1.2 or newer. Certificates and
keys are loaded at startup; restart the service after rotation.

## Monitoring

Scrape `/metrics` and alert on:

- increasing `megacache_misses_total` relative to hits;
- sustained `megacache_evictions_total`, indicating insufficient capacity;
- `megacache_load_errors_total`;
- high `megacache_active_leases` or `megacache_coalesced_total`;
- process memory, CPU, open connections, and restart count;
- `megacache_rejected_entries_total`, indicating values exceed policy;
- `megacache_request_duration_seconds`, partitioned by protocol and operation;
- `megacache_request_errors_total`;
- `megacache_cluster_degraded`, logical node health, ring version, leadership
  term, and replication lag from `mc topology` and `mc status`.
- `megacache_origin_requests_total` by origin/outcome;
- `megacache_origin_breaker_state`, concurrency, and queue depth;
- `origin_load_shed_total`, retry exhaustion, stale-if-error, negative hits,
  and refresh errors from `mc info` or `/v1/stats`.
- `megacache_events_total` by outcome, `event_dead_letter_depth`,
  `event_graph_rejected_total`, checkpoint age, and source cursor lag.
- `megacache_intelligence_tracked_keys`,
  `megacache_intelligence_telemetry_evictions_total`,
  `megacache_intelligence_hot_replications_total`, and
  `megacache_intelligence_experiment_rollbacks_total`.

Counters reset at process restart. `entries`, `tags`, and `active_leases` are
gauges; other exported values are cumulative counters.

Logs use one JSON object per line by default. Set
`MEGACACHE_LOG_FORMAT=text` for local human-readable output. Request logs never
include keys, values, authentication headers, or RESP arguments.

## Membership and failure behavior

The server emits in-process heartbeats every
`MEGACACHE_HEARTBEAT_INTERVAL_SECONDS`. A logical node is marked down after
`MEGACACHE_HEARTBEAT_TIMEOUT_SECONDS`. Keep the interval comfortably below
the timeout. The current leader remains leader while healthy; after its loss,
the smallest healthy ring member is elected and the term increases. Old fence
and refresh-lease tokens then fail deterministically.

`one` can continue with any owner but may observe an older replica before read
repair. `majority` tolerates fewer than half of owners being unavailable.
`all` fails whenever any owner is unavailable. Failed quorum operations return
an explicit error and restore backend LRU/accounting state; callers should not
treat them as successful cache writes. Deterministic input/admission failures
such as invalid TTLs, unserializable values, and oversized entries remain
validation errors rather than quorum failures.

Completed deletes are compacted after every current owner has acknowledged the
tombstone. When an owner is unavailable, tombstones are retained to prevent its
older copy from being repaired back into the cluster. The retained backlog is
bounded by `MEGACACHE_MAX_RETAINED_TOMBSTONES` (default `10000`). Once full,
deletes that would grow the backlog are rejected without partial mutation.
Recover the missing node so read repair can acknowledge the deletion, or drain
and remove it before retrying. Monitor `retained_tombstones`,
`retained_tombstone_limit`, and `tombstones_collected_total`.

The embedded API exposes `add_node`, `drain_node`, `remove_node`,
`plan_rebalance`, and `apply_rebalance`. A plan is tied to its base ring and
leadership fence. Planning requires a healthy latest-version source for every
move. Quorum-confirmed expired or fully evicted entries are reconciled to
deletions before planning, so they do not block topology changes. Transfer
verifies the latest version on every target owner before routing changes or
old-owner cleanup.
Snapshot sessions enforce
`MEGACACHE_SNAPSHOT_PAYLOAD_LIMIT_BYTES`,
`MEGACACHE_SNAPSHOT_CHUNK_BYTES`, and
`MEGACACHE_SNAPSHOT_MAX_IN_FLIGHT`.

There is no native node RPC in 0.9. Real packet loss, asymmetric partitions,
cross-host clocks, and process split brain are outside implemented behavior.

Treat MegaCache as optional infrastructure. Clients should enforce short
timeouts and fall back to the authoritative origin when it is unavailable.
Rate-limit that fallback to avoid transferring a cache outage to the origin.

Because version 0.9 cache entries are in-memory, rolling restarts begin cold. Warm critical
keys gradually with `mc fetch`; the same admission and origin protection
policies apply to warming.
