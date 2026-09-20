# Freshness events and change-data-capture adapters

Version 0.7 adds a dependency-free event ingestion core. It accepts normalized
change events, applies declarative key/tag rules, expands a bounded dependency
graph, invalidates matching cache state, and then durably advances a replay
checkpoint. The design is at-least-once: a crash before checkpoint persistence
can repeat an invalidation, but it cannot silently skip a successfully
checkpointed change.

In managed mode, each tenant has an independent event state file, checkpoint
space, deduplication set, DLQ, dependency traversal, and invalidation target.
Webhook source names are assigned to exactly one tenant in the control-plane
configuration, while authenticated direct events use the caller's bound
tenant. Event status never merges checkpoints across tenants.

## Event envelope

Every source is normalized to:

```json
{
  "event_id": "catalog-products-1042",
  "source": "postgres",
  "stream": "catalog-slot",
  "position": 1042,
  "cursor": "0/16B6C50",
  "operation": "update",
  "schema_id": "catalog.product@2",
  "timestamp": 1789911000,
  "payload": {
    "id": 42,
    "tenant_id": "acme"
  }
}
```

`event_id` is the durable idempotency key. `(source, stream, position)` is the
ordered checkpoint. Positions must be non-negative, monotonically increasing
integers within a stream. `cursor` preserves a native resume token such as a
PostgreSQL LSN, MySQL file/position, or MongoDB resume token. Adapters convert
their native order into `position`.

## Configuration

Copy `events.example.json` and set:

```bash
export MEGACACHE_EVENTS_FILE="$PWD/events.json"
export MEGACACHE_EVENT_STATE_FILE="/var/lib/megacache/events-state.json"
export MEGACACHE_CATALOG_WEBHOOK_SECRET='a-long-random-secret'
mc serve
```

Rules use scalar placeholders such as `{payload.id}`, `{source}`, and
`{operation}`. Missing or non-scalar fields dead-letter the event rather than
partially advancing it. Rules can filter by source, stream, operation, and
schema name.

Namespaces use `name:vN:key`. Policies are:

- `strict`: invalidate only `write_version`;
- `rolling`: invalidate every compatible reader version;
- `dual_write`: expose all compatible write keys to embedded callers and
  invalidate every compatible version.

`ReaderRange` declares versions the current process can consume.
`SchemaRegistry.register_migration()` installs an explicit in-process migration
between schema versions. JSON configuration cannot load executable migration
code.

Dependencies are directed from an authoritative cache key to a derived key.
Configuration limits nodes, edges, fanout, traversal depth, and total expanded
invalidations. Edge insertion rejects cycles. Before mutating cache state, the
ingestor computes the complete dependency closure. Exceeding depth or node
bounds rejects the event without invalidation or checkpoint advancement;
traversal never returns a partial closure.

## HTTP and webhook ingestion

Authenticated application clients with `invalidate` permission can submit:

```text
POST /v1/events
```

Webhook producers use:

```text
POST /v1/events/webhook/{configured-name}
X-MegaCache-Timestamp: UNIX_SECONDS
X-MegaCache-Delivery: UNIQUE_DELIVERY_ID
X-MegaCache-Signature: sha256=HEX_HMAC
```

The HMAC-SHA256 input begins with the ASCII bytes
`megacache-webhook-v2|`, followed by source, timestamp header, delivery header,
and exact body in that order. Each field is encoded as its ASCII decimal byte
length, a literal `:`, and the unmodified bytes. For example, integrations may
call `webhook_signature_payload(source, timestamp, delivery, body)` before
HMAC calculation. This framing prevents concatenation ambiguity and binds the
route/source and delivery ID as well as the timestamp and body.

MegaCache uses constant-time comparison, a bounded timestamp tolerance, and
durable one-use delivery claims. Claims remain live for the full configured
tolerance after acceptance. If all claim slots are live, the endpoint applies
backpressure rather than evicting replay protection. Webhook secrets may be
loaded from environment variables with `secret_env`; this is preferred over
embedding them in the events file. The event `source` must equal the configured
webhook name (or may be omitted and is filled with that name), preventing one
webhook credential from impersonating another event source.

Operational endpoints:

- `GET /v1/events/status` (administrator);
- `POST /v1/events/retry` with `{"limit":100}` (administrator).

Equivalent RESP commands are `MC.EVENT`, `MC.EVENT.STATUS`, and
`MC.EVENT.RETRY`. The native CLI exposes `mc event`, `mc events-status`, and
`mc events-retry`. If `MEGACACHE_EVENTS_FILE` is unset, the event APIs report
that ingestion is not configured. A configured document with no rules reports
`enabled: false`; ingestion and retries are rejected without checkpoint
advancement.

## Adapter boundary

MegaCache deliberately does **not** include Kafka, PostgreSQL, MySQL, or
MongoDB wire clients. It has zero runtime dependencies and cannot safely
pretend that protocol handshakes, authentication, replication-slot ownership,
consumer groups, acknowledgements, or driver failover exist.

The public adapter types accept records obtained by an application-selected
driver:

- `RecordConsumer` and `KafkaRecordAdapter`;
- `PostgresLogicalAdapter` with `PostgresLogicalRecord`;
- `MySQLBinlogAdapter` with `MySQLBinlogRecord`;
- `MongoChangeStreamAdapter` with `MongoChangeRecord`.

`RecordConsumer.poll()` supplies `KafkaRecord` objects and `commit()` receives
next offsets only after each record is either applied or durably dead-lettered.
Database adapters are externally fed through `feed()`. Callers should reconnect
their native driver using the cursor in `event_status()["checkpoints"]`.

For PostgreSQL, `parse_postgres_lsn()` converts the two 32-bit halves of a
`HEX/HEX` LSN to an ordered integer. A WAL LSN identifies a transaction record,
not necessarily one row change, so every `PostgresLogicalRecord` must include
a stable zero-based `ordinal`. Feed one complete batch in strict
`(LSN, ordinal)` order. The adapter checkpoints the composite ordered position
`(parsed_lsn << 32) | ordinal` and generates IDs containing the slot, LSN, and
ordinal. On reconnect, start at the checkpoint's native LSN cursor and replay
that LSN; composite positions safely discard already completed ordinals.
Drivers must never reorder, omit, or renumber changes between retries.

MySQL integrations must provide a monotonic `sequence` across binlog file
rotation. MongoDB integrations must provide a monotonic `sequence` while
preserving the opaque `resume_token` as `cursor`.

## Checkpoints, deduplication, and dead letters

`AtomicCheckpointStore` writes a complete state document to a sibling file,
flushes it, atomically replaces the old file, and fsyncs the containing
directory where supported. It stores:

- per-stream positions and native cursors;
- a bounded set of recently completed event IDs;
- bounded webhook replay claims;
- a count- and byte-bounded dead-letter queue with attempts, failure
  timestamps, retry time, and retryability.

Cache invalidation happens before checkpoint persistence. A crash between those
steps repeats harmless invalidation after restart. A failed event and its
advanced checkpoint are persisted in the same atomic state update, so the
connector can continue while the failure remains visible and retryable.
Before cache mutation, completion is checked against stream and total-state
budgets. Before a failed event advances its checkpoint, its complete DLQ entry
is checked against DLQ count/byte and total-state budgets. Capacity exhaustion
therefore applies backpressure and leaves the prior checkpoint unchanged.
Unresolved dead letters are never evicted.

The state store is safe for threads in one process and holds a non-blocking
advisory lock on a sibling `.lock` file for its lifetime. A second owner fails
startup; the lock is released by `close()`. This is ownership, not
multi-process consensus. Put state on a durable local filesystem, back it up,
monitor DLQ depth/bytes and total state bytes, and never place it on an
eventually consistent object-store mount.

State format v2 uses length-framed source/stream checkpoint keys and
source/delivery replay keys. Unambiguous v1 keys migrate atomically at startup;
ambiguous v1 keys are rejected for operator review rather than guessed.
Lowering any configured bound below existing durable usage fails startup and
never truncates records silently.

Deduplication history is intentionally bounded. Events older than the retained
ID window are still rejected when their position is at or behind the durable
stream checkpoint. Maximum stream count, total state bytes, event payload
bytes, cursor bytes, retained error bytes, replay claims, and DLQ count/bytes
all have explicit environment-configurable limits.
