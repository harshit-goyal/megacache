# Self-hosted managed control plane

MegaCache 1.0 adds an opt-in, dependency-free control-plane foundation. It is
designed for one self-hosted process and for integration with an external
orchestrator, billing system, and key-management system. It does **not** claim
to provide a hosted service, a browser UI, compliance certification, or
multi-process data-plane isolation.

## Enable it

Create tenant configuration from `control-plane.example.json`, create a v2
users file with a `tenant_id` on every user, and provide one encryption key
source:

```bash
cp control-plane.example.json control-plane.json
export MEGACACHE_CONTROL_PLANE_FILE="$PWD/control-plane.json"
export MEGACACHE_CONTROL_STATE_DIRECTORY="$PWD/megacache-control-state"
export MEGACACHE_USERS_FILE="$PWD/users.json"
export MEGACACHE_CONTROL_MASTER_KEY="$(
  python3 -c 'import base64,secrets; print(base64.urlsafe_b64encode(secrets.token_bytes(32)).decode())'
)"
mc serve
```

For file-based key rotation, set `MEGACACHE_CONTROL_KEY_FILE` instead of
`MEGACACHE_CONTROL_MASTER_KEY`. The file must be a regular file inaccessible
to group and other users:

```json
{
  "version": 1,
  "active_key_id": "local-2026-09",
  "namespace_key_id": "namespace-v1",
  "keys": {
    "local-2026-09": "BASE64_32_TO_64_BYTE_KEY",
    "local-2026-08": "PREVIOUS_BASE64_KEY_NEEDED_FOR_OLD_ARTIFACTS",
    "namespace-v1": "STABLE_BASE64_NAMESPACE_KEY"
  }
}
```

Keep historical artifact keys until retention expires and keep
`namespace_key_id` stable for the life of the state directory.

Exactly one process may own a control-state directory. MegaCache holds an
advisory lock for its lifetime, atomically replaces and fsyncs the bounded
state file, rejects symlink state/artifact files, creates files with mode
`0600`, and migrates supported older state on startup.

The version 1 control file accepts:

| Tenant field | Meaning |
|---|---|
| `id`, `display_name`, `enabled` | Stable identity and startup availability |
| `quotas` | Entries, bytes, per-entry bytes, operations/burst, connections, origin concurrency |
| `origins`, `webhooks` | Explicit names available to this tenant |
| `regions`, `primary_region` | Allowed placement metadata |
| `deployment` | Desired version, replica count, max unavailable, and drain state |
| `backup` | Schedule interval plus count/time retention |
| `disaster_recovery` | RPO, RTO, and recovery-region metadata |
| `retention` | Usage periods, completed operations, and exports retained |

Populate `origins` only with names from `MEGACACHE_ORIGINS_FILE` and
`webhooks` only with names from `MEGACACHE_EVENTS_FILE`. A webhook name can be
assigned to one tenant only.

## Identity and isolation

A managed user is bound to one tenant by the users file:

```json
{
  "version": 2,
  "users": [
    {
      "username": "catalog-admin",
      "password_hash": "pbkdf2_sha256$600000$...",
      "permissions": ["admin"],
      "key_prefixes": ["catalog:"],
      "tenant_id": "default",
      "roles": ["tenant_admin"]
    }
  ]
}
```

The authenticated tenant cannot be selected through an HTTP header or RESP
argument. Key prefixes are evaluated against the tenant's logical keys, so the
existing prefix model remains useful inside each tenant.

Every tenant receives an independent cache/coordinator, reverse tag index,
lease table, origin facade, event checkpoint store, intelligence telemetry,
and invalidation cursor. A tenant therefore cannot enumerate another tenant's
keys, tags, origins, event cursors, dead letters, or policy intelligence.
Webhook source names may be assigned to only one tenant.

A keyed HMAC derives an opaque namespace identifier used in control metadata,
audit records, state paths, and encrypted artifact associated data. Tenant
data is not multiplexed by a caller-controlled key prefix.

## Quotas and noisy-neighbor controls

Each tenant declares hard limits for:

- entries, total estimated bytes, and one-entry bytes;
- operations per second and token-bucket burst;
- simultaneous HTTP/RESP connections; and
- origin concurrency.

Entry and byte limits are enforced by the tenant's independent bounded cache,
so another tenant cannot consume its capacity or drive its eviction policy.
Connection and operation quotas are checked before dispatch. Origin caches
have independent global admission budgets per tenant. The sum of configured
tenant entry and byte quotas may not exceed the configured per-logical-node
limits; total process memory still scales with the in-process replica count.

These controls bound in-process contention; they are not operating-system CPU,
memory, disk-I/O, or network cgroups. Run separate processes or containers
when hard failure-domain/resource isolation is required.

## Durable metering and billing contract

Completed HTTP/RESP requests update hourly, tenant-scoped counters for
operations, errors, request/response payload bytes, origin operations, and
accepted connections. Period history is bounded, cumulative counters and
sequence ranges support reconciliation, and every update uses atomic durable
state replacement. A state-write failure is exposed in dashboard alerts and
metrics but does not stop an otherwise healthy data plane.

`BillingProvider.export_usage(batch_id, payload)` is the replaceable provider
contract. The batch identifier is derived from canonical sorted usage records,
so retrying an unchanged range produces the same identifier. MegaCache ships
only a local JSON-file adapter for integration/testing and the HTTP/RESP export
surface; it does not contact or emulate a commercial billing provider.

```bash
mc billing-export --tenant default
```

## Audit records

Administrative mutations and asynchronous operation transitions are written
to append-only JSONL segments. Every record contains its predecessor hash and
an HMAC-SHA256 digest under the identified key. Startup verifies the complete
retained chain and refuses corrupted audit history. Segments rotate at the
configured byte bound. When the configured segment count is exhausted,
control-plane mutations fail closed while cache traffic continues.

Export verified records directly or create an encrypted audit artifact:

```bash
mc audit --tenant default --after 0 --limit 1000
mc audit-export --tenant default
```

MegaCache never silently deletes audit segments. After exporting and durably
archiving the unfiltered chain, an auditor can remove only fully covered
segments by supplying an exact exported sequence/hash boundary:

```bash
mc audit-prune THROUGH_SEQUENCE EXPORTED_RECORD_HASH --yes
```

A signed local anchor preserves the next retained record's chain continuity.
If the segment bound is reached before this explicit workflow, new
control-plane mutations fail closed while cache traffic continues.

## Backups, restore, retention, and disaster recovery

Scheduled and manual backups contain a consistent storage snapshot and are
encrypted before being written. The stdlib-only envelope uses independent
HMAC-SHA256-derived encryption and authentication keys, a random 256-bit
nonce, a PRF counter stream, encrypt-then-MAC authentication, and a plaintext
SHA-256 checksum. Keys are supplied by the replaceable `KeyProvider` contract
and are never stored in an artifact.

```bash
mc backup --tenant default
mc operation OPERATION_ID
mc restore-validate BACKUP_ID --tenant default
mc restore default BACKUP_ID VALIDATION_TOKEN --confirm default --yes
mc dr-drill BACKUP_ID --tenant default
```

A restore is accepted only after the exact artifact has passed decryption,
identity, schema, checksum, and quota validation. It requires a short-lived
HMAC validation token, the exact tenant ID, and an explicit `--yes`. Replacement
restore drains admitted tenant operations and rolls back the prior in-memory
snapshot if mutation fails. A drill restores into a bounded scratch engine and
records measured local validation time plus RPO/RTO results without changing
live data. Validation and replacement restore can temporarily hold one
additional tenant-sized snapshot in process memory; reserve that headroom.

Backup age/count, usage-period, completed-operation, and export retention are
enforced by the scheduler. DR records describe RPO/RTO and recovery regions;
regional failover itself belongs to the external orchestrator.

## Deployment desired and observed state

The control plane stores a generation-numbered desired state with region set,
replica count, target version, `rolling` strategy, maximum unavailable count,
and drain flag. An external orchestrator reports bounded observed instances.
The dashboard reports version, generation, health, and drain drift:

```bash
mc deployment-status
mc deployment-set default deployment.json
mc deployment-observe default observation.json
```

`deployment.json`:

```json
{
  "version": "1.1.0",
  "regions": ["local-primary", "local-recovery"],
  "replicas": 2,
  "max_unavailable": 1,
  "drain": false
}
```

`observation.json`:

```json
{
  "instance_id": "cache-a",
  "region": "local-primary",
  "version": "1.0.0",
  "health": "healthy",
  "draining": false,
  "observed_generation": 1
}
```

MegaCache records metadata only. It does not create machines, move traffic,
manage DNS, or perform cross-process rolling upgrades.

## Privacy workflows

Tenant data and audit exports are asynchronous, bounded, encrypted operations
with durable status:

```bash
mc data-export --tenant default
mc operation OPERATION_ID
```

Pending/running work is re-queued after restart and is therefore at-least-once.
Artifact creation and operation idempotency keys make retries safe; a caller
must still inspect the terminal operation record.

Deletion uses a short-lived cryptographic challenge, exact tenant-name
confirmation, and an explicit irreversible flag:

```bash
mc tenant-delete-challenge --tenant default
mc tenant-delete default CHALLENGE --confirm default --yes
```

Deletion stops new tenant traffic, drains active requests, flushes in-memory
values, closes tenant workers/state stores, and removes managed backup/export
artifacts. Bounded usage aggregates and audit records remain as operational
evidence and contain no cache values. The deleted identity can still read its
control/operation status and verified audit records, but data-plane commands
remain unavailable. Physical secure erasure depends on the
host filesystem, snapshots, and storage controller; MegaCache does not claim
cryptographic erasure or regulatory compliance.

Leaving a deleted tenant in the startup configuration does not recreate its
data plane; the durable tombstone wins. Reuse a tenant ID only through an
explicit, separately reviewed state-migration procedure.

## Roles

Data permissions (`read`, `write`, `invalidate`, `admin`) apply only inside the
authenticated tenant. Control roles are separate:

| Role | Scope |
|---|---|
| `tenant_admin` | Own tenant dashboard, backup, restore, export, deletion |
| `platform_admin` | All control metadata and tenant workflows |
| `operator` | Desired/observed deployment and orchestrator views |
| `auditor` | Verified audit export |
| `billing_admin` | Deterministic usage export |

`admin` remains a tenant data-plane permission and does not by itself grant
cross-tenant access. For compatibility, an older user entry with `admin` and
no `roles` field receives only `tenant_admin`; specify an explicit empty role
array to keep a data-only administrator.

## Explicit limitations

- There is no hosted dashboard; `/v1/control/status` and `mc control-status`
  return the dashboard document.
- There is no bundled external orchestrator, billing provider, cloud KMS, or
  compliance workflow/certification.
- The default key providers read protected local configuration. Integrators
  may implement `KeyProvider` for an external KMS.
- Every tenant data plane, logical replica, scheduler, and worker is still in
  one process and one failure domain.
- Cache values remain in memory. Backups are local encrypted files and are not
  remotely replicated unless an operator moves them.
- DR drills validate local backup recovery; they do not prove regional network,
  DNS, compute, or storage failover.
