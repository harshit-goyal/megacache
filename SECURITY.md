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

MegaCache does not terminate TLS. Deploy it on a private network behind a TLS
proxy, configure `MEGACACHE_API_KEY`, and restrict health and metrics endpoints
with network policy. Cached values reside in process memory and must be treated
according to their data classification.

