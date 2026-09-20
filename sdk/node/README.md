# MegaCache Node.js SDK 1.0

Dependency-free Node.js 18+ RESP2 client with typed errors, bounded byte/entry
L1 caching, per-key promise coalescing, stale-if-error, and L2 lease
coordination. TLS verifies certificates and hostnames; insecure verification
cannot be enabled through this API.

```js
const { MegaCacheClient } = require("@megacache/client");
const cache = new MegaCacheClient({ host: "cache.example", tls: true });
const result = await cache.getOrLoad("user:42", async () =>
  JSON.stringify(await database.loadUser(42)), {
    ttlSeconds: 60, staleSeconds: 300, tags: ["users"]
  });
cache.close();
```

`traceparentProvider` may return the active W3C `traceparent`; no OpenTelemetry
package is required. `src/integrations.js` contains dependency-free Express-
The middleware uses asynchronous request-local context, so concurrent requests
can safely share one client.
