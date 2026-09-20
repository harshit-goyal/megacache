# Command reference

Run commands from the repository root unless stated otherwise.

## Clone

```bash
git clone https://github.com/harshit-goyal/megacache.git
cd megacache
```

Install the native command:

```bash
python3 -m pip install .
mc --help
```

## Run locally

MegaCache requires Python 3.9 or newer and has no runtime dependencies.

```bash
mc serve
```

Equivalent command:

```bash
make run
```

Running `mc` without a subcommand starts the server. The legacy `megacache`
executable remains available as a backward-compatible alias. The RESP2 service
starts on `localhost:6380`; HTTP starts at
`http://localhost:8080`.

## Configure

Copy the example configuration and export the values required by your
environment:

```bash
cp .env.example .env
set -a
. ./.env
set +a
make run
```

For a network-accessible deployment, configure an API key:

```bash
export MEGACACHE_API_KEY='replace-with-a-long-random-secret'
make run
```

For scoped named users, create a protected users file interactively:

```bash
mc init-users users.json --username admin
export MEGACACHE_USERS_FILE="$PWD/users.json"
mc serve
```

Use named credentials from the native client:

```bash
export MEGACACHE_CLI_USERNAME=admin
export MEGACACHE_CLI_PASSWORD='the-password-entered-during-init'
mc ping
```

`mc hash-password` prints a salted PBKDF2 hash for adding more users to the
file. See [Security](../SECURITY.md) for its format and permission model.

Enable protected HTTP origins with a declarative file:

```bash
cp origins.example.json origins.json
# Replace the example authority, allowlists, and policy.
export MEGACACHE_ORIGINS_FILE="$PWD/origins.json"
mc serve
```

Origins are loaded at startup. The server never accepts a destination URL from
a client; callers select a configured origin and supply only an allowed path.

Enable freshness events with a declarative file and durable state path:

```bash
cp events.example.json events.json
export MEGACACHE_EVENTS_FILE="$PWD/events.json"
export MEGACACHE_EVENT_STATE_FILE="$PWD/events-state.json"
export MEGACACHE_CATALOG_WEBHOOK_SECRET='replace-with-a-long-random-secret'
mc serve
```

The bundled Kafka and database integrations are externally-fed adapter
interfaces, not network clients. See [Freshness events](events.md).

The examples below use this shell variable:

```bash
export MEGACACHE_URL='http://localhost:8080'
export MEGACACHE_API_KEY='replace-with-a-long-random-secret'
export REDISCLI_AUTH="$MEGACACHE_API_KEY"
```

## Native MegaCache commands

Check connectivity:

```bash
mc ping
```

Store a value with fresh and stale windows:

```bash
mc put product:123 '{"id":123,"name":"Desk"}' \
  --ttl 300 \
  --stale 900 \
  --tag product:123 \
  --tag catalog
```

Read and delete:

```bash
mc get product:123
mc delete product:123
```

Manage expiration:

```bash
mc expire product:123 300
mc ttl product:123
```

Work with multiple keys:

```bash
mc mset feature:a on feature:b off
mc mget feature:a feature:b
mc exists feature:a feature:b
```

Coordinate a refresh:

```bash
mc lease product:123
mc put product:123 '{"id":123,"name":"Updated desk"}' \
  --ttl 300 --stale 900 --lease TOKEN_FROM_LEASE
```

Invalidate related entries:

```bash
mc invalidate catalog product:123
```

Read through a configured HTTP origin:

```bash
mc fetch product:123 catalog /v1/products/123
mc fetch product:123 catalog /v1/products/123 --refresh
```

The first form returns a fresh cached value, schedules refresh-ahead or
stale-while-revalidate work when policy requires it, or performs one
coalesced origin request. `--refresh` bypasses a fresh cached value but still
uses singleflight and every origin protection budget. Fetch requires both
`read` and `write` permission for the key.

Inspect the server:

```bash
mc dbsize
mc info
mc topology
mc topology product:123
mc status
mc origins
mc events-status
```

`mc topology` reports the ring version and fingerprint, leader term, logical
nodes, ownership counts, replica lag, configured consistency, and degraded
state. Supplying a key reports its primary and replica owners. `mc status`
returns a compact health summary. Both require administrator permission.
`mc origins` reports per-origin breaker state, active concurrency, queue depth,
retry tokens, and failure count; it also requires administrator permission.

Submit a normalized event from a file or standard input:

```bash
mc event change.json
cat change.json | mc event
mc events-retry --limit 25
```

`mc event` requires `invalidate` permission. Event status and retries require
administrator permission.

Clear all entries only with explicit confirmation:

```bash
mc flush --yes
```

For automation, place `--json` before the subcommand:

```bash
mc --json get product:123
mc --json lease product:123
```

Connection flags also precede the subcommand:

```bash
mc --host cache.internal --port 6380 ping
```

Connect through TLS:

```bash
mc --host cache.example.com --tls --ca-file ./ca.pem ping
```

The native command reads its password from `MEGACACHE_API_KEY`. The complete
syntax is available through `mc --help` and `mc <command> --help`.

## Redis CLI compatibility

Connect interactively:

```bash
redis-cli -h 127.0.0.1 -p 6380
```

Write, read, expire, and delete values:

```bash
redis-cli -p 6380 SET user:42 '{"name":"Ada"}'
redis-cli -p 6380 GET user:42
redis-cli -p 6380 EXPIRE user:42 300
redis-cli -p 6380 TTL user:42
redis-cli -p 6380 EXISTS user:42
redis-cli -p 6380 DEL user:42
```

Write and read multiple values:

```bash
redis-cli -p 6380 MSET feature:a on feature:b off
redis-cli -p 6380 MGET feature:a feature:b
```

Inspect or clear the cache:

```bash
redis-cli -p 6380 DBSIZE
redis-cli -p 6380 INFO
redis-cli -p 6380 FLUSHDB
```

Use freshness windows and tags:

```bash
redis-cli -p 6380 MC.SET product:123 '{"id":123}' \
  TTL 300 STALE 900 TAGS 2 product:123 catalog
redis-cli -p 6380 MC.LEASE product:123
redis-cli -p 6380 MC.INVALIDATE catalog
```

See the [RESP2 reference](resp.md) for the supported command matrix,
authentication, response formats, and compatibility boundaries.

## Run with Docker

Build and start:

```bash
docker compose up --build
```

Run in the background:

```bash
docker compose up --build --detach
```

View logs:

```bash
docker compose logs --follow megacache
```

Stop:

```bash
docker compose down
```

## Check service health

Health and readiness endpoints do not require authentication:

```bash
curl --fail "$MEGACACHE_URL/healthz"
curl --fail "$MEGACACHE_URL/readyz"
```

## HTTP: write a cache entry

Keys are URL-path components and should be percent encoded. For example,
`product:123` becomes `product%3A123`.

```bash
curl --fail-with-body \
  --request PUT "$MEGACACHE_URL/v1/cache/product%3A123" \
  --header "Authorization: Bearer $MEGACACHE_API_KEY" \
  --header 'Content-Type: application/json' \
  --data '{
    "value": {
      "id": 123,
      "name": "Desk"
    },
    "ttl_seconds": 300,
    "stale_seconds": 900,
    "tags": ["product:123", "catalog"]
  }'
```

`ttl_seconds` controls the fresh window. `stale_seconds` controls how long the
entry remains available as stale after the fresh window ends.

## HTTP: read a cache entry

```bash
curl --fail-with-body \
  "$MEGACACHE_URL/v1/cache/product%3A123" \
  --header "Authorization: Bearer $MEGACACHE_API_KEY"
```

The response state is `fresh`, `stale`, or `miss`. A miss returns HTTP `404`.

## HTTP: delete a cache entry

```bash
curl --fail-with-body \
  --request DELETE "$MEGACACHE_URL/v1/cache/product%3A123" \
  --header "Authorization: Bearer $MEGACACHE_API_KEY"
```

## HTTP: invalidate entries by tag

This removes every entry associated with either supplied tag:

```bash
curl --fail-with-body \
  --request POST "$MEGACACHE_URL/v1/invalidate" \
  --header "Authorization: Bearer $MEGACACHE_API_KEY" \
  --header 'Content-Type: application/json' \
  --data '{"tags":["product:123","catalog"]}'
```

## HTTP: refresh a value without a cache stampede

Request a refresh lease:

```bash
curl --fail-with-body \
  --request POST "$MEGACACHE_URL/v1/lease/product%3A123" \
  --header "Authorization: Bearer $MEGACACHE_API_KEY"
```

The response determines the next action:

| State | Action |
|---|---|
| `fresh` | Use the value; no refresh is necessary. |
| `stale_lease` | Serve the stale value, refresh the origin, then write using `lease_token`. |
| `stale` | Serve the stale value; another client owns the refresh. |
| `lease` | Load the missing value and write using `lease_token`. |
| `loading` | Another client is loading the value; retry after `retry_after_seconds`. |

After loading the authoritative value, submit it with the lease:

```bash
export LEASE_TOKEN='token-returned-by-the-lease-request'

curl --fail-with-body \
  --request PUT "$MEGACACHE_URL/v1/cache/product%3A123" \
  --header "Authorization: Bearer $MEGACACHE_API_KEY" \
  --header 'Content-Type: application/json' \
  --data "{
    \"value\":{\"id\":123,\"name\":\"Updated desk\"},
    \"ttl_seconds\":300,
    \"stale_seconds\":900,
    \"tags\":[\"product:123\",\"catalog\"],
    \"lease_token\":\"$LEASE_TOKEN\"
  }"
```

## Inspect statistics and metrics

JSON statistics require authentication:

```bash
curl --fail-with-body \
  "$MEGACACHE_URL/v1/stats" \
  --header "Authorization: Bearer $MEGACACHE_API_KEY"
```

Prometheus metrics are exposed without application authentication:

```bash
curl --fail "$MEGACACHE_URL/metrics"
```

Restrict `/metrics` using network policy or a reverse proxy in production.

## Test

```bash
make test
```

Equivalent command:

```bash
PYTHONPATH=src python3 -m unittest discover -s tests -v
```

## Build a Python wheel

```bash
python3 -m pip wheel --no-deps --wheel-dir dist .
```

Install the generated wheel:

```bash
python3 -m pip install dist/megacache-0.8.0-py3-none-any.whl
mc
```
