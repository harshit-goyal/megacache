# Security policy

## Supported versions

Security updates are provided for the latest release on the default branch.
The 1.0 self-hosted control plane has automated isolation and tamper-detection
coverage, but no independent certification or third-party security assessment
is claimed.

## Reporting a vulnerability

Please use GitHub's private vulnerability reporting feature for this
repository. Include affected versions, reproduction steps, impact, and any
known mitigations. Do not open a public issue for an unpatched vulnerability.

Maintainers will acknowledge a complete report within seven days and coordinate
disclosure after a fix is available.

## Deployment responsibilities

MegaCache supports TLS 1.2 or newer when `MEGACACHE_TLS_CERT_FILE` and
`MEGACACHE_TLS_KEY_FILE` are configured. Otherwise, deploy it only on a private
network behind HTTP and TCP TLS proxies. Restrict listener ports plus health
and metrics endpoints with network policy.

Create a named-users file with:

```bash
mc init-users /run/secrets/megacache-users.json \
  --username admin --tenant default
```

The file contains salted PBKDF2-HMAC-SHA256 password hashes and should be
readable only by the MegaCache process. Each user has one or more permissions:

| Permission | Access |
|---|---|
| `read` | Read cache keys |
| `write` | Create, update, delete, expire, and lease keys |
| `invalidate` | Invalidate entries by tag |
| `admin` | All operations, statistics, and cache flushing |

`key_prefixes` restrict read and write operations to named key prefixes. An
empty list permits every key allowed by the user's permissions.

Example:

```json
{
  "version": 2,
  "users": [
    {
      "username": "catalog-reader",
      "password_hash": "pbkdf2_sha256$600000$BASE64_SALT$BASE64_DIGEST",
      "permissions": ["read"],
      "key_prefixes": ["catalog:"],
      "tenant_id": "default",
      "roles": []
    }
  ]
}
```

Generate each hash interactively with `mc hash-password`. Users are loaded at
startup, so restart after rotating the file. `MEGACACHE_API_KEY` remains a
legacy unrestricted administrator credential and should not be used for new
deployments. HTTP named users authenticate with Basic authentication and RESP
clients use `AUTH username password`; both must run over TLS.

Cached values reside in process memory and must be treated according to their
data classification. Request logs intentionally exclude cache keys, values,
authorization headers, passwords, and RESP arguments.

## Managed tenant security

Managed mode requires a named-users file and binds each authenticated identity
to exactly one `tenant_id`. Clients cannot choose a tenant through a request
header or cache-key prefix. Existing `key_prefixes` are evaluated within that
tenant. Each tenant receives a separate engine/coordinator, tag index, origin
runtime, event store, invalidation cursor, and intelligence store. Tenant
administrators cannot list other tenants or retrieve their operations, usage,
audit records, origins, events, or recommendations.

Data permissions are separate from control roles:

- `tenant_admin` manages backup, restore, export, deletion, and status only for
  its bound tenant;
- `platform_admin` manages control metadata across tenants;
- `operator` manages desired/observed deployment metadata;
- `auditor` exports verified audit records; and
- `billing_admin` prepares deterministic usage batches.

Use separate credentials for these responsibilities. The legacy API key has
broad control roles for migration and should not be enabled in a managed
deployment.

Protect `MEGACACHE_CONTROL_STATE_DIRECTORY` with host filesystem permissions.
The state file is bounded, atomically replaced, and guarded by a lifetime
advisory lock. Audit segments are append-only and HMAC chained; startup fails
if retained history does not verify. When the segment bound is reached,
control mutations fail closed rather than silently dropping records. Cache
traffic remains independent of metering persistence, and an error is exposed
for reconciliation. Pruning requires an auditor/platform role, an explicit
irreversible CLI/API acknowledgement, and an exact sequence/hash from an
exported global chain; a signed anchor preserves retained-chain continuity.

Backups and data/audit exports are encrypted with a random nonce,
purpose-separated HMAC-SHA256-derived keys, an HMAC PRF stream, and
encrypt-then-MAC authentication. The local environment and JSON-file key
providers require 32–64 bytes of key material and retain key IDs for rotation.
They are not a cloud KMS. Applications embedding MegaCache can implement the
`KeyProvider` interface for an external KMS/HSM. Keep old keys available until
all retained artifacts using them have expired, and keep the namespace key ID
stable for the lifetime of the control-state directory.

Restore requires successful artifact/quota validation, a short-lived
cryptographic validation token, an exact tenant confirmation, and an explicit
irreversible acknowledgement. Tenant deletion has a separate short-lived
challenge and removes in-memory values plus managed artifacts, but cannot
guarantee physical erasure from filesystem snapshots, backups, or storage
media. Audit and aggregate usage records are intentionally retained without
cache values.

## Cache intelligence security

Cache intelligence is disabled by default. It uses bounded counters and fixed
formulas; no model loading, arbitrary code, query language, or untrusted
expression evaluation is supported. Per-key and per-class telemetry have
explicit limits, and neither keys nor classes appear as Prometheus labels.
Content-change history stores only SHA-256 digests, not prior values.

`mc explain` and its HTTP/RESP equivalents require `read` permission for the
specific key, so named-user key prefixes protect key visibility. Bulk
recommendations, simulation, and experiment status require `admin` because
they can contain key names or operational policy data. Explain output never
contains cached values.

Protect `MEGACACHE_INTELLIGENCE_STATE_FILE` as operational metadata. MegaCache
creates replacement files with mode `0600` and records only a bounded
experiment decision audit, but directory permissions, backup retention, and
single-process ownership remain operator responsibilities. The file is not a
consensus store.

## HTTP origin security

`mc fetch` and `MC.FETCH` never accept a destination URL. They accept a
configured origin name plus an absolute path. Every origin must declare:

- one fixed `http` or `https` authority;
- exact allowed hostnames and ports;
- normalized allowed path prefixes; and
- explicit CIDRs for any intentionally reachable address denied by default.

MegaCache resolves the declared hostname on every attempt, rejects the request
unless every answer is ordinary global unicast or CIDR-allowlisted, and pins
the connection to a validated address. Private, loopback, link-local,
multicast, reserved, unspecified, NAT64, IPv4-mapped/translatable, 6to4,
Teredo, and other transition addresses are denied by default. Embedded IPv4
addresses receive the same validation. HTTPS still verifies the certificate
against the declared hostname. Redirects are not followed. Userinfo,
fragments, path traversal, network-path references, encoded slashes, unsafe
fixed headers, and oversized responses are rejected.

Treat `MEGACACHE_ORIGINS_FILE` as security-sensitive configuration. Keep
allowlists narrow, prefer TLS, use dedicated origin credentials with read-only
scope, and restrict configuration-file permissions. Adding broad private
network CIDRs materially expands what a compromised write-capable MegaCache
client can reach. Origin definitions are loaded at startup, so restart after
rotation. MegaCache 1.0 intentionally has no arbitrary URL mode.

## Event and webhook security

Direct HTTP/RESP event ingestion requires `invalidate` permission. Treat event
producers as authoritative: transformation rules can invalidate every key or
tag they are configured to derive. Keep rule filters and namespace ranges
narrow, and protect `MEGACACHE_EVENTS_FILE` from untrusted modification.

Webhook endpoints authenticate independently with HMAC-SHA256 over
length-framed source, timestamp, delivery ID, and exact request-body bytes.
This prevents field-boundary ambiguity and binds the route/source and delivery
identity to the signature. Secrets must contain at least 16 bytes; use
high-entropy values, prefer `secret_env` over inline JSON, transmit webhooks
over TLS, and rotate by restarting MegaCache with updated configuration.
Timestamp tolerance and durable one-use delivery identifiers limit replay;
live claims are never evicted to admit new deliveries.
MegaCache returns a generic authentication error and never logs signatures,
delivery identifiers, event bodies, rendered keys, or secrets.
The normalized event source is bound to the configured webhook name, so one
webhook credential cannot select another source's transformation rules.
Managed mode additionally requires every webhook source name to belong to at
most one tenant and routes it directly to that tenant's independent event
store.

Protect `MEGACACHE_EVENT_STATE_FILE` as sensitive operational data. It contains
source cursors, event IDs, delivery claims, and failed event payloads in the
dead-letter queue. The file is created mode `0600`, but its parent directory,
backups, volume permissions, retention, and secure deletion remain operator
responsibilities. Exactly one process may own a state file; MegaCache enforces
this with an advisory sibling-file lock on macOS/Linux. State, stream, payload,
cursor, error, replay, and dead-letter count/byte limits are security
boundaries. Capacity exhaustion rejects work before checkpoint advancement.

Kafka and database adapters do not open network connections and do not process
credentials. Applications supplying native clients remain responsible for
TLS, broker/database authentication, least-privilege replication accounts,
consumer-group or slot ownership, network allowlists, and driver updates.

## Cluster security boundary

Version 1.0 provides an in-process cluster coordinator and no node-to-node
network listener. Logical node IDs in `MEGACACHE_CLUSTER_NODES` are local
configuration, not authenticated identities. Do not expose or build an
unauthenticated RPC shim around `ClusterStorage`.

A future inter-process transport must provide mutual authentication,
encryption, replay protection, message and snapshot size enforcement, term and
ring-version validation, and authorization for membership changes. Until then,
all logical replicas share one process security boundary and fail together if
that process is compromised or terminated.

Origin singleflight is likewise shared only inside one current
`ClusterStorage` coordinator. Separate processes do not coordinate origin
requests and must be budgeted independently.

Hot-key replication has the same boundary: it creates only best-effort copies
among logical nodes in one process, does not count toward quorum, and provides
no independent failure domain or multi-host protection.
