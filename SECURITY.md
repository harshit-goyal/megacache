# Security policy

## Supported versions

MegaCache is currently alpha software. Security updates are provided for the
latest release on the default branch.

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
mc init-users /run/secrets/megacache-users.json --username admin
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
  "users": [
    {
      "username": "catalog-reader",
      "password_hash": "pbkdf2_sha256$600000$BASE64_SALT$BASE64_DIGEST",
      "permissions": ["read"],
      "key_prefixes": ["catalog:"]
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
rotation. MegaCache 0.6 intentionally has no arbitrary URL mode.

## Cluster security boundary

Version 0.6 provides an in-process cluster coordinator and no node-to-node
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
