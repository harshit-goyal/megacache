# RESP2 and Redis command reference

MegaCache exposes a RESP2 TCP server on port `6380`. It works with `redis-cli`
and clients that only require the supported command subset. It is not a
complete Redis implementation. Most users should use the native
[`mc` commands](commands.md); `redis-cli` exists for compatibility and
migration.

## Connect

Without authentication:

```bash
redis-cli -h 127.0.0.1 -p 6380
```

When `MEGACACHE_API_KEY` is configured:

```bash
redis-cli -h 127.0.0.1 -p 6380 --askpass
```

Named users authenticate with:

```text
AUTH username password
```

The native CLI accepts `--username` and reads the password from
`MEGACACHE_CLI_PASSWORD`.

Set `REDISCLI_AUTH` for non-interactive development or automation. Avoid
passing secrets directly on the command line because they can appear in shell
history and process listings.

```bash
export REDISCLI_AUTH="$MEGACACHE_API_KEY"
redis-cli -h 127.0.0.1 -p 6380 PING
```

## Supported Redis commands

| Command | Syntax | MegaCache behavior |
|---|---|---|
| `PING` | `PING [message]` | Checks the RESP server. |
| `ECHO` | `ECHO message` | Returns the supplied bulk string. |
| `AUTH` | `AUTH password` | Authenticates with `MEGACACHE_API_KEY`. |
| `GET` | `GET key` | Returns a bulk string or nil. |
| `SET` | `SET key value [EX seconds]` | Stores a persistent or expiring value. |
| `MGET` | `MGET key [key ...]` | Returns values in request order. |
| `MSET` | `MSET key value [key value ...]` | Atomically stores persistent values. |
| `DEL` | `DEL key [key ...]` | Deletes keys and returns the count. |
| `EXISTS` | `EXISTS key [key ...]` | Counts existing keys. |
| `EXPIRE` | `EXPIRE key seconds` | Sets a positive TTL. |
| `TTL` | `TTL key` | Returns seconds, `-1` for persistent, or `-2` for absent. |
| `DBSIZE` | `DBSIZE` | Returns the number of live entries. |
| `FLUSHDB` | `FLUSHDB` | Removes all entries and leases. |
| `INFO` | `INFO [section]` | Returns server and MegaCache counters. |
| `SELECT` | `SELECT 0` | Accepts database zero; other databases are unsupported. |
| `HELLO` | `HELLO 2` | Returns RESP2 server metadata. |
| `CLIENT` | `CLIENT SETINFO ...` | Supports client metadata used by newer clients. |
| `COMMAND` | `COMMAND [...]` | Returns an empty command metadata array. |
| `QUIT` | `QUIT` | Closes the connection. |

Examples:

```text
SET session:42 "active"
GET session:42
SET report:daily "ready" EX 300
TTL report:daily
MSET feature:a on feature:b off
MGET feature:a feature:b missing
DEL feature:a feature:b
```

## MegaCache commands

### `MC.SET`

Stores a value with fresh and stale windows and optional invalidation tags:

```text
MC.SET key value [TTL seconds] [STALE seconds]
       [TAGS count tag ...] [LEASE token]
```

Example:

```text
MC.SET product:123 '{"id":123}' TTL 300 STALE 900 TAGS 2 product:123 catalog
```

`TTL` must be positive. `STALE` may be zero. If omitted, configured defaults
apply. At most 100 tags may be attached to an entry. `LEASE` completes a
refresh only when the token is current and belongs to the same key.

### `MC.LEASE`

Reads a key and coordinates refresh ownership:

```text
MC.LEASE key
```

It returns a RESP array:

| State | Response fields | Action |
|---|---|---|
| `fresh` | state, value | Serve the value. |
| `stale_lease` | state, value, token | Serve stale, refresh, then write with `MC.SET ... LEASE token`. |
| `stale` | state, value | Serve stale; another client is refreshing. |
| `lease` | state, token | Load the missing value and write with `MC.SET ... LEASE token`. |
| `loading` | state, retry milliseconds | Retry after the specified interval. |

Example refresh completion:

```text
MC.SET product:123 '{"id":123}' TTL 300 STALE 900 LEASE token-from-mc-lease
```

### `MC.INVALIDATE`

Deletes every entry associated with any supplied tag:

```text
MC.INVALIDATE tag [tag ...]
```

The integer response is the number of deleted entries.

### `MC.FETCH`

Reads through a named declarative HTTP origin:

```text
MC.FETCH key origin /allowed/path
MC.FETCH key origin /allowed/path REFRESH
```

The command requires read and write permission for the key. It returns compact
JSON containing `state`, `origin`, `status_code`, `attempts`, `value`, and
optional `error`. `REFRESH` bypasses a fresh cached value but does not bypass
singleflight, allowlists, admission, retry, timeout, or breaker policy. The
path must be allowed by the named origin; URLs are never accepted.

### `MC.TOPOLOGY`

Returns compact JSON describing the coordinator ring, leader term, nodes,
ownership counts, replication lag, consistency profile, and degraded state:

```text
MC.TOPOLOGY
MC.TOPOLOGY product:123
```

With a key argument, the response contains that key's primary and replica
owners. Administrator permission is required.

### `MC.STATUS`

Returns compact JSON with logical node health, ring version, total replication
lag, known key count, and degraded state. Administrator permission is required.

### `MC.ORIGINS`

Returns compact JSON containing each configured origin's circuit-breaker state,
active concurrency, bounded queue depth, retry tokens, and failure count.
Administrator permission is required.

## Compatibility boundaries

MegaCache currently supports RESP2 string-cache workflows. It does
not support RESP3, pipelined transaction guarantees, pub/sub, scripts,
transactions, streams, hashes, lists, sets, sorted sets, persistence,
Sentinel, or Redis Cluster. MegaCache's replication is internal to
`ClusterStorage`; it does not implement Redis replication protocols.

RESP keys are limited to 1024 bytes. HTTP keys are limited to 1024 characters.
Each argument is limited by
`MEGACACHE_MAX_BODY_BYTES`. Plain `SET` and `MSET` entries have no time-based
expiration but can still be removed by bounded LRU eviction or process restart.
