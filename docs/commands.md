# Command reference

Run commands from the repository root unless stated otherwise.

## Clone

```bash
git clone https://github.com/harshit-goyal/megacache.git
cd megacache
```

## Run locally

MegaCache requires Python 3.9 or newer and has no runtime dependencies.

```bash
make run
```

Equivalent command:

```bash
PYTHONPATH=src python3 -m megacache
```

The service starts at `http://localhost:8080`.

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

The examples below use this shell variable:

```bash
export MEGACACHE_URL='http://localhost:8080'
export MEGACACHE_API_KEY='replace-with-a-long-random-secret'
```

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

## Write a cache entry

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

## Read a cache entry

```bash
curl --fail-with-body \
  "$MEGACACHE_URL/v1/cache/product%3A123" \
  --header "Authorization: Bearer $MEGACACHE_API_KEY"
```

The response state is `fresh`, `stale`, or `miss`. A miss returns HTTP `404`.

## Delete a cache entry

```bash
curl --fail-with-body \
  --request DELETE "$MEGACACHE_URL/v1/cache/product%3A123" \
  --header "Authorization: Bearer $MEGACACHE_API_KEY"
```

## Invalidate entries by tag

This removes every entry associated with either supplied tag:

```bash
curl --fail-with-body \
  --request POST "$MEGACACHE_URL/v1/invalidate" \
  --header "Authorization: Bearer $MEGACACHE_API_KEY" \
  --header 'Content-Type: application/json' \
  --data '{"tags":["product:123","catalog"]}'
```

## Refresh a value without a cache stampede

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
python3 -m pip install dist/megacache-0.1.0-py3-none-any.whl
megacache
```
