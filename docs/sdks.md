# SDKs and integrations

Version 1.0 supports four dependency-free SDKs:

| SDK | Package/source | Minimum runtime | Conformance command |
|---|---|---:|---|
| Python | root `megacache` package | Python 3.9 | `python3 conformance/run.py --sdk python` |
| Node.js | `sdk/node`, `@megacache/client` | Node.js 18 | `python3 conformance/run.py --sdk node` |
| Go | `sdk/go` module | Go 1.20 | `python3 conformance/run.py --sdk go` |
| Java | `sdk/java`, `io.megacache:megacache-client` | Java 11 | `python3 conformance/run.py --sdk java` |

All SDKs expose common RESP2 string operations (`PING`, `GET`, `SET`, `MGET`,
`MSET`, `DEL`, `EXISTS`, `EXPIRE`, and `TTL`) plus `put`, `fetch`, `lease`,
`invalidate`, and `status`. Transport, protocol, and server command failures
use distinct typed errors. Every typed command validates its RESP shape,
arity, element types, value ranges, and JSON schema; incompatible responses
raise the SDK's protocol error and discard the connection rather than leaking
native cast/index/JSON exceptions.

## L1 and L2 coordination

`get_or_load`/`getOrLoad` first checks a bounded LRU L1, then uses
`MC.LEASE` against MegaCache L2. One local caller runs a loader while local
followers share its result; the L2 lease coordinates healthy clients across
processes. Fresh and stale windows are explicit, and stale values may be
returned when loading fails if `stale_if_error` is enabled.

In Go, an omitted zero-value `CachePolicy.Stale` retains the five-minute
default for source compatibility. Use `policy.WithStale(0)` (or set
`StaleSet: true`) to request an explicit zero stale window. Negative durations
are rejected.

L1 limits are enforced by both entry count and approximate key/value bytes.
Oversized values are not admitted. Every successful local mutation clears L1.
For mutations from other clients, SDKs poll the restart-safe
`MC.INVALIDATIONS` epoch and generation before L1 reads once the polling
interval has elapsed. Therefore an active client's next read after the default
one-second interval clears stale L1 data after one successful network round
trip. A disconnected client can retain data only through its configured stale
deadline; applications requiring immediate revocation should disable L1 for
those keys or explicitly clear it.

## Trace context

SDKs accept a provider/hook for the active W3C `traceparent`; OpenTelemetry is
optional. `MC.TRACEPARENT` associates context with a RESP connection, and
`MC.FETCH ... TRACEPARENT value` propagates it through MegaCache to the
configured origin without changing that connection default. Framework
integrations use request-local context so shared clients do not leak traces
between concurrent requests. Invalid values are rejected. HTTP fetch callers
may send the standard `traceparent` header.

## Framework adapters

Examples intentionally import no optional frameworks:

- Python: WSGI middleware suitable for Flask/Django and a generic DB loader
- Node.js: Express-style middleware and promise-based database loader
- Go: `net/http` middleware and `database/sql` loader
- Java: JDK `HttpHandler` middleware and JDBC loader

They are small adapters meant to be composed with application-owned framework
and driver instances, avoiding dependency/version conflicts.

## v1.0 validation record

The release implementation was exercised locally with Python 3.9.6, Node.js
25.2.1/npm 11.6.2, and OpenJDK/javac 25 using `--release 11`. The Go toolchain
was unavailable in that environment, so Go has source, shared fixtures, and a
CI job but was not claimed as locally executed. CI is configured to run the
same Go conformance test with Go 1.20.

## TLS

Every SDK verifies certificates and hostnames and requires TLS 1.2 or newer
where the standard runtime exposes that control. None exposes an
`insecure_skip_verify` option.
