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
- `megacache_request_errors_total`.

Counters reset at process restart. `entries`, `tags`, and `active_leases` are
gauges; other exported values are cumulative counters.

Logs use one JSON object per line by default. Set
`MEGACACHE_LOG_FORMAT=text` for local human-readable output. Request logs never
include keys, values, authentication headers, or RESP arguments.

## Failure behavior

Treat MegaCache as optional infrastructure. Clients should enforce short
timeouts and fall back to the authoritative origin when it is unavailable.
Rate-limit that fallback to avoid transferring a cache outage to the origin.

Because version 0.4 is in-memory, rolling restarts begin cold. Warm critical
keys gradually or accept misses while using origin-side admission controls.
