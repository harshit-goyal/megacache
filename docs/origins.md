# HTTP origin configuration

Set `MEGACACHE_ORIGINS_FILE` to a startup-loaded JSON document:

```json
{
  "origins": [
    {
      "name": "catalog",
      "base_url": "https://catalog.example.com",
      "allowed_hosts": ["catalog.example.com"],
      "allowed_ports": [443],
      "allowed_path_prefixes": ["/v1/products/"]
    }
  ]
}
```

`origins.example.json` contains every policy field.

In managed mode, each tenant explicitly lists allowed origin names in the
control-plane file. MegaCache builds an independent `OriginCache` runtime and
global concurrency budget for that tenant, and `mc origins` exposes only that
tenant's configured origins.

## Required authority policy

| Field | Meaning |
|---|---|
| `name` | Unique 1–64 character identifier selected by clients |
| `base_url` | Fixed `http` or `https` authority; no userinfo, query, fragment, or path |
| `allowed_hosts` | Non-empty exact-host allowlist containing the base host |
| `allowed_ports` | Non-empty port allowlist containing the effective base port |
| `allowed_path_prefixes` | Non-empty normalized absolute-path allowlist |
| `allowed_ip_networks` | Optional exact CIDRs permitting otherwise denied resolved addresses |
| `headers` | Optional fixed request headers; authority and framing headers are forbidden |

Wildcards are not supported. Every DNS answer is validated on every attempt;
one disallowed answer rejects the request. Connections are pinned to a
validated address, redirects are not followed, and HTTPS validates the
declared hostname. Client paths cannot contain a scheme, authority, fragment,
dot segment, backslash, encoded slash, or leave the configured prefixes.

The default network policy accepts only ordinary globally routable unicast
addresses. Private, loopback, link-local, multicast, reserved, unspecified,
IPv4-mapped/translatable, NAT64, 6to4, Teredo, and other transition addresses
are denied. Embedded IPv4 addresses are evaluated by the same policy. A denied
address is usable only when the resolved address, or its embedded IPv4 address,
is explicitly covered by `allowed_ip_networks`.

## Freshness policy

| Field | Default | Meaning |
|---|---:|---|
| `ttl_seconds` | `300` | Fresh cache window |
| `refresh_ahead_seconds` | `0` | Schedule refresh when a fresh read has at most this much TTL |
| `stale_while_revalidate_seconds` | `0` | Return stale immediately while a worker refreshes |
| `stale_if_error_seconds` | `0` | Additional stale window usable only after protected refresh failure |
| `negative_statuses` | `[404,410]` | Cacheable 4xx statuses |
| `negative_ttl_seconds` | `30` | Fresh lifetime of a negative entry |
| `tags` | `[]` | Tags attached to origin-populated entries |

Refresh-ahead is triggered by a read; MegaCache does not scan the entire key
space. Background jobs use the same singleflight, admission, retry, timeout,
and breaker policy as foreground work.

When version 0.9 intelligence is enabled, positive origin refreshes may use a
shorter TTL based on bounded observed content-change history. The declared
`ttl_seconds` remains a hard ceiling, negative TTL is never adapted, and fewer
than two observed loads retain the declared value. Background refresh jobs are
ordered by deterministic bounded access, recency, and load-cost evidence while
retaining the same queue limit and admission safeguards.

Origin hits require a valid internal positive or negative marker with the same
origin name and request identity. The full normalized path and query are sent
to the origin and hashed for cache identity, while persisted and explained
lineage contains only the path so query credentials are not disclosed. An
ordinary value written through the cache API never satisfies origin lineage
and is refreshed instead. Invalidating an origin entry's tag also removes its
refresh lease atomically, so an older in-flight refresh cannot restore the
invalidated value.

## Capacity and resilience policy

| Field | Default | Meaning |
|---|---:|---|
| `timeout_seconds` | `5` | Socket connect/read timeout per attempt |
| `queue_timeout_seconds` | `1` | Maximum admission or coalesced-follower wait |
| `max_response_bytes` | `1048576` | Maximum response body |
| `max_concurrency` | `8` | Active attempts for this origin |
| `max_queue` | `64` | Waiting attempts and followers per coalesced flight |
| `retry_attempts` | `2` | Maximum retries after the initial attempt |
| `retry_backoff_seconds` | `0.05` | Initial exponential backoff |
| `retry_max_backoff_seconds` | `1` | Backoff cap |
| `retry_jitter` | `0.2` | Symmetric fractional jitter from `0` to `1` |
| `retry_budget_capacity` | `32` | Maximum retry tokens |
| `retry_budget_refill_per_second` | `1` | Retry-token refill rate |
| `breaker_failure_threshold` | `5` | Consecutive failed attempts before open |
| `breaker_open_seconds` | `30` | Open-state duration before half-open |
| `breaker_half_open_requests` | `1` | Concurrent half-open probes |

Retryable conditions are connection/timeout/protocol failures and HTTP `408`,
`425`, `429`, `500`, `502`, `503`, and `504`. Other non-success statuses fail
without retry unless configured as negative responses.

The process-wide queue limit also independently bounds all coalesced followers.
Excess or timed-out followers fail with `origin_overloaded`; shutdown wakes
admission and coalesced waits. Built-in storage backends renew refresh leases
while origin work, retries, or backoff are active. A backend without lease
renewal support is rejected deterministically when the configured worst-case
attempt/queue/backoff envelope can outlive the acquired lease.

Process-wide admission and worker settings are environment variables:

| Variable | Default |
|---|---:|
| `MEGACACHE_ORIGIN_WORKER_THREADS` | `2` |
| `MEGACACHE_ORIGIN_REFRESH_QUEUE_SIZE` | `1000` |
| `MEGACACHE_ORIGIN_GLOBAL_MAX_CONCURRENCY` | `64` |
| `MEGACACHE_ORIGIN_GLOBAL_MAX_QUEUE` | `256` |

All counts, sizes, and durations are validated at startup. Restart the process
to apply definition changes.
