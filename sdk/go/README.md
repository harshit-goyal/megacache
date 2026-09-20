# MegaCache Go SDK 1.0

Package `megacache` supports Go 1.20+ using only the standard library. It
provides RESP2 operations, typed errors, MegaCache fetch/lease/invalidate/status,
bounded L1 caching, per-key request coalescing, stale-if-error, cursor polling,
and W3C `traceparent` hooks. TLS requires certificate and hostname validation.

```go
client, _ := megacache.New(megacache.Options{Address: "127.0.0.1:6380"})
defer client.Close()
value, err := client.GetOrLoad("user:42", loadUser, megacache.CachePolicy{
    TTL: time.Minute, Stale: 5*time.Minute, Tags: []string{"users"},
})
```

The zero value of `CachePolicy.Stale` keeps the compatibility default of five
minutes. To explicitly disable stale serving, mark the field as set with
`WithStale`:

```go
policy := megacache.CachePolicy{TTL: time.Minute}.WithStale(0)
```

Negative TTL or stale durations are rejected before any command is sent.

`integrations.go` contains `net/http` middleware and a `database/sql` loader
without importing a framework or database driver. The middleware stores
`traceparent` on the request context; pass that context to `FetchContext` so
concurrent requests never mutate shared client state.
