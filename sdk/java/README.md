# MegaCache Java SDK 0.8

The Java 11+ SDK uses only the JDK. It includes RESP2 operations, typed
exceptions, MegaCache lease/fetch/invalidate/status methods, bounded L1
caching, per-key `CompletableFuture` coalescing, stale-if-error, cursor polling,
and dependency-free W3C `traceparent` hooks.

```java
MegaCacheClient client = new MegaCacheClient();
MegaCacheClient.CachePolicy policy = new MegaCacheClient.CachePolicy();
MegaCacheClient.CachedValue value =
    client.getOrLoad("user:42", () -> databaseLoad(), policy);
```

TLS uses the JVM trust store (or a caller-provided `SSLContext`) and enables
HTTPS endpoint identification. `Integrations` provides JDK `HttpHandler` and
JDBC examples without adding framework or driver dependencies.
Its HTTP trace context is scoped to the handling thread and restored when the
request completes.
