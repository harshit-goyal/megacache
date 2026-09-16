# Operations

## Deployment baseline

Run MegaCache on a private network behind a TLS-terminating reverse proxy. Set
`MEGACACHE_API_KEY`, restrict `/metrics` by network policy, use a read-only
container filesystem, and allocate enough memory for the configured entry
count and payload limit.

The process runs as an unprivileged user in the supplied container. It logs
HTTP requests to standard output and shuts down cleanly on interruption.

## Capacity planning

`MEGACACHE_MAX_ENTRIES` bounds entry count, not bytes. Actual memory includes
Python objects, decoded JSON values, tags, indexes, and request handling.
Measure representative payloads under load and leave headroom. Keep
`MEGACACHE_MAX_BODY_BYTES` close to the largest legitimate cached object.

## Monitoring

Scrape `/metrics` and alert on:

- increasing `megacache_misses_total` relative to hits;
- sustained `megacache_evictions_total`, indicating insufficient capacity;
- `megacache_load_errors_total`;
- high `megacache_active_leases` or `megacache_coalesced_total`;
- process memory, CPU, open connections, and restart count.

Counters reset at process restart. `entries`, `tags`, and `active_leases` are
gauges; other exported values are cumulative counters.

## Failure behavior

Treat MegaCache as optional infrastructure. Clients should enforce short
timeouts and fall back to the authoritative origin when it is unavailable.
Rate-limit that fallback to avoid transferring a cache outage to the origin.

Because version 0.1 is in-memory, rolling restarts begin cold. Warm critical
keys gradually or accept misses while using origin-side admission controls.

