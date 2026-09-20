# MegaCache Python SDK 0.9

The Python 3.9+ SDK ships in the root `megacache` package and has no runtime
dependencies. `MegaCacheClient` supports common RESP2 operations, typed
errors, fetch/lease/invalidate/status, bounded byte/entry L1 caching,
per-key request coalescing, stale-if-error, and invalidation cursor polling.

```python
from megacache.client import CachePolicy, MegaCacheClient

with MegaCacheClient() as cache:
    result = cache.get_or_load(
        "user:42",
        lambda: database.load_user(42),
        CachePolicy(ttl_seconds=60, stale_seconds=300, tags=("users",)),
    )
```

Pass `traceparent_provider` to bridge an OpenTelemetry context without making
OpenTelemetry a dependency. TLS uses `ssl.create_default_context`, including
certificate and hostname verification.
The WSGI integration keeps trace context request-local, including during lazy
response iteration.
