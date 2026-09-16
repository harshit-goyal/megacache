# Operations

## Deployment baseline

Run MegaCache on a private network. Configure its built-in TLS or place it
behind TLS-terminating proxies. Use a named users file, restrict `/metrics` by
network policy, use a read-only container filesystem, and allocate enough
memory for configured byte and entry limits.

Start the process with `mc serve`. `SIGINT` and `SIGTERM` stop both listeners,
allow active handlers up to `MEGACACHE_SHUTDOWN_GRACE_SECONDS` to finish, then
close remaining connections. It exposes HTTP on port `8080` and
RESP2 on port `6380`. Put both
behind appropriate network controls; use a TCP TLS proxy for RESP when traffic
crosses a trusted boundary. The process runs as an unprivileged user in the
supplied container, logs requests to standard output, and shuts down cleanly.

Version 0.5's cluster is in-process. A comma-separated
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

## Capacity planning

`MEGACACHE_MAX_ENTRIES` bounds entry count and
`MEGACACHE_MAX_MEMORY_BYTES` bounds estimated entry bytes.
`MEGACACHE_MAX_ENTRY_BYTES` rejects oversized values before they can evict
useful entries. Estimates include encoded keys, values, tags, and fixed
metadata but not every Python allocator overhead, connection, or request
buffer. Measure representative payloads and leave process-memory headroom.
Keep `MEGACACHE_MAX_BODY_BYTES` close to the largest legitimate request; the
same limit applies to the total RESP command payload.

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

There is no native node RPC in 0.5. Real packet loss, asymmetric partitions,
cross-host clocks, and process split brain are outside implemented behavior.

Treat MegaCache as optional infrastructure. Clients should enforce short
timeouts and fall back to the authoritative origin when it is unavailable.
Rate-limit that fallback to avoid transferring a cache outage to the origin.

Because version 0.5 is in-memory, rolling restarts begin cold. Warm critical
keys gradually or accept misses while using origin-side admission controls.
