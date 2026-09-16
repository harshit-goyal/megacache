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
