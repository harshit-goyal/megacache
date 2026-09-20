"""Self-hosted multi-tenant control-plane primitives.

The control plane intentionally manages metadata and independent in-process
tenant data planes.  It does not provision hosts, contact a billing provider,
or turn the in-process cluster coordinator into a multi-host system.
"""

from __future__ import annotations

import base64
import binascii
import datetime
import fcntl
import hashlib
import hmac
import json
import logging
import math
import os
import queue
import re
import secrets
import stat
import threading
import time
from collections import defaultdict
from dataclasses import dataclass
from typing import (
    Any,
    Callable,
    Dict,
    Iterable,
    List,
    Mapping,
    MutableMapping,
    Optional,
    Protocol,
    Sequence,
    Tuple,
)

from .engine import CacheEngine
from .storage import StorageBackend, StorageEntry

CONTROL_STATE_VERSION = 2
CONTROL_CONFIG_VERSION = 1
CONTROL_PLANE_VERSION = "1.0.0"
_IDENTIFIER = re.compile(r"[A-Za-z0-9][A-Za-z0-9._-]{0,127}\Z")
_ARTIFACT_ID = re.compile(r"[a-f0-9]{32}\Z")
_ARTIFACT_FILE = re.compile(
    r"(backup|export|audit-export)-([a-f0-9]{32})\.mcar\Z"
)
_AUDIT_SEGMENT = re.compile(r"audit-([0-9]{8})\.jsonl\Z")
_USAGE_PERIOD = re.compile(
    r"[0-9]{4}-(?:0[1-9]|1[0-2])-(?:0[1-9]|[12][0-9]|3[01])"
    r"T(?:[01][0-9]|2[0-3]):00Z\Z"
)
_ZERO_HASH = "0" * 64
LOG = logging.getLogger("megacache.control")


class ControlPlaneError(ValueError):
    """Invalid control-plane request or state."""


class ControlPlaneUnavailable(ControlPlaneError):
    """The requested control-plane capability is unavailable."""


class ControlPlaneCapacity(ControlPlaneError):
    """A configured durable or in-memory bound has been reached."""


class TenantNotFound(ControlPlaneError):
    """The tenant is not configured."""


class TenantUnavailable(ControlPlaneError):
    """The tenant data plane is suspended, draining, or deleted."""


class TenantQuotaExceeded(ControlPlaneError):
    """A tenant connection or throughput quota rejected work."""


class ArtifactError(ControlPlaneError):
    """An encrypted backup or export is invalid."""


def _canonical(value: Any) -> bytes:
    try:
        return json.dumps(
            value,
            sort_keys=True,
            separators=(",", ":"),
            ensure_ascii=False,
            allow_nan=False,
        ).encode("utf-8")
    except (TypeError, ValueError) as exc:
        raise ControlPlaneError("value is not valid bounded JSON") from exc


def _json_copy(value: Any) -> Any:
    return json.loads(_canonical(value).decode("utf-8"))


def _identifier(value: Any, field: str) -> str:
    if not isinstance(value, str) or _IDENTIFIER.fullmatch(value) is None:
        raise ControlPlaneError(
            "{} must be a 1-128 character identifier".format(field)
        )
    return value


def _bounded_text(value: Any, field: str, maximum: int = 1024) -> str:
    if (
        not isinstance(value, str)
        or not value
        or len(value.encode("utf-8")) > maximum
    ):
        raise ControlPlaneError(
            "{} must contain 1 to {} UTF-8 bytes".format(field, maximum)
        )
    return value


def _integer(
    value: Any, field: str, *, minimum: int = 0, maximum: int = 2**63 - 1
) -> int:
    if (
        isinstance(value, bool)
        or not isinstance(value, int)
        or value < minimum
        or value > maximum
    ):
        raise ControlPlaneError(
            "{} must be an integer from {} through {}".format(
                field, minimum, maximum
            )
        )
    return value


def _number(
    value: Any, field: str, *, minimum: float = 0.0
) -> float:
    if (
        isinstance(value, bool)
        or not isinstance(value, (int, float))
        or not math.isfinite(value)
        or float(value) < minimum
    ):
        raise ControlPlaneError(
            "{} must be a finite number not below {}".format(field, minimum)
        )
    return float(value)


def _usage_period(value: Optional[str], field: str) -> Optional[str]:
    if value is None:
        return None
    if not isinstance(value, str) or _USAGE_PERIOD.fullmatch(value) is None:
        raise ControlPlaneError(
            "{} must use YYYY-MM-DDTHH:00Z".format(field)
        )
    try:
        datetime.datetime.strptime(value, "%Y-%m-%dT%H:00Z")
    except ValueError as exc:
        raise ControlPlaneError(
            "{} must be a valid UTC hour".format(field)
        ) from exc
    return value


def _keys_only(
    value: Mapping[str, Any], allowed: Iterable[str], field: str
) -> None:
    unknown = set(value) - set(allowed)
    if unknown:
        raise ControlPlaneError(
            "{} contains unsupported fields: {}".format(
                field, ", ".join(sorted(unknown))
            )
        )


def _identifier_tuple(value: Any, field: str) -> Tuple[str, ...]:
    if not isinstance(value, list):
        raise ControlPlaneError("{} must be an array".format(field))
    result = tuple(_identifier(item, field) for item in value)
    if len(result) != len(set(result)):
        raise ControlPlaneError("{} contains duplicates".format(field))
    return result


def _read_json_file(path: str, field: str, maximum_bytes: int) -> Dict[str, Any]:
    if not isinstance(path, str) or not path:
        raise ControlPlaneError("{} path must be non-empty".format(field))
    absolute = os.path.abspath(path)
    flags = os.O_RDONLY | getattr(os, "O_CLOEXEC", 0)
    flags |= getattr(os, "O_NOFOLLOW", 0)
    try:
        descriptor = os.open(absolute, flags)
        try:
            metadata = os.fstat(descriptor)
            if not stat.S_ISREG(metadata.st_mode):
                raise ControlPlaneError("{} must be a regular file".format(field))
            if metadata.st_size > maximum_bytes:
                raise ControlPlaneCapacity(
                    "{} exceeds its configured byte limit".format(field)
                )
            with os.fdopen(descriptor, "r", encoding="utf-8") as source:
                descriptor = -1
                document = json.load(source)
        finally:
            if descriptor >= 0:
                os.close(descriptor)
    except (OSError, UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise ControlPlaneError(
            "unable to load {} '{}': {}".format(field, absolute, exc)
        ) from exc
    if not isinstance(document, dict):
        raise ControlPlaneError("{} must contain a JSON object".format(field))
    return document


def _secure_directory(path: str) -> str:
    if not isinstance(path, str) or not path:
        raise ControlPlaneError("control state directory must be non-empty")
    requested = os.path.abspath(path)
    if os.path.lexists(requested) and stat.S_ISLNK(os.lstat(requested).st_mode):
        raise ControlPlaneError(
            "control state directory must not be a symbolic link"
        )
    absolute = os.path.realpath(requested)
    os.makedirs(absolute, mode=0o700, exist_ok=True)
    metadata = os.lstat(absolute)
    if not stat.S_ISDIR(metadata.st_mode) or stat.S_ISLNK(metadata.st_mode):
        raise ControlPlaneError(
            "control state directory must be a real directory"
        )
    os.chmod(absolute, 0o700)
    return absolute


def _atomic_write(path: str, payload: bytes, maximum_bytes: int) -> None:
    if len(payload) > maximum_bytes:
        raise ControlPlaneCapacity("durable state exceeds configured byte limit")
    parent = os.path.dirname(path)
    _secure_directory(parent)
    pending = "{}.new-{}".format(path, secrets.token_hex(12))
    flags = os.O_WRONLY | os.O_CREAT | os.O_EXCL
    flags |= getattr(os, "O_CLOEXEC", 0) | getattr(os, "O_NOFOLLOW", 0)
    descriptor: Optional[int] = None
    try:
        descriptor = os.open(pending, flags, 0o600)
        os.fchmod(descriptor, 0o600)
        with os.fdopen(descriptor, "wb") as destination:
            descriptor = None
            destination.write(payload)
            destination.flush()
            os.fsync(destination.fileno())
        os.replace(pending, path)
        try:
            directory_descriptor = os.open(
                parent,
                os.O_RDONLY
                | getattr(os, "O_CLOEXEC", 0)
                | getattr(os, "O_DIRECTORY", 0),
            )
            try:
                os.fsync(directory_descriptor)
            finally:
                os.close(directory_descriptor)
        except OSError:
            pass
    finally:
        if descriptor is not None:
            os.close(descriptor)
        try:
            os.unlink(pending)
        except FileNotFoundError:
            pass


@dataclass(frozen=True)
class TenantQuotas:
    max_entries: int
    max_bytes: int
    max_entry_bytes: int
    ops_per_second: int
    burst_ops: int
    max_connections: int
    origin_concurrency: int

    def __post_init__(self) -> None:
        for name in (
            "max_entries",
            "max_bytes",
            "max_entry_bytes",
            "ops_per_second",
            "burst_ops",
            "max_connections",
            "origin_concurrency",
        ):
            _integer(getattr(self, name), name, minimum=1)
        if self.max_entry_bytes > self.max_bytes:
            raise ControlPlaneError("max_entry_bytes cannot exceed max_bytes")
        if self.burst_ops < self.ops_per_second:
            raise ControlPlaneError("burst_ops cannot be below ops_per_second")

    @classmethod
    def from_dict(
        cls,
        value: Mapping[str, Any],
        *,
        default_entries: int,
        default_bytes: int,
        default_entry_bytes: int,
    ) -> "TenantQuotas":
        if not isinstance(value, Mapping):
            raise ControlPlaneError("tenant quotas must be an object")
        _keys_only(
            value,
            (
                "max_entries",
                "max_bytes",
                "max_entry_bytes",
                "ops_per_second",
                "burst_ops",
                "max_connections",
                "origin_concurrency",
            ),
            "tenant quotas",
        )
        rate = _integer(
            value.get("ops_per_second", 1000),
            "ops_per_second",
            minimum=1,
        )
        result = cls(
            max_entries=_integer(
                value.get("max_entries", default_entries),
                "max_entries",
                minimum=1,
            ),
            max_bytes=_integer(
                value.get("max_bytes", default_bytes),
                "max_bytes",
                minimum=1,
            ),
            max_entry_bytes=_integer(
                value.get("max_entry_bytes", default_entry_bytes),
                "max_entry_bytes",
                minimum=1,
            ),
            ops_per_second=rate,
            burst_ops=_integer(
                value.get("burst_ops", rate),
                "burst_ops",
                minimum=1,
            ),
            max_connections=_integer(
                value.get("max_connections", 100),
                "max_connections",
                minimum=1,
            ),
            origin_concurrency=_integer(
                value.get("origin_concurrency", 16),
                "origin_concurrency",
                minimum=1,
            ),
        )
        if result.max_entry_bytes > result.max_bytes:
            raise ControlPlaneError(
                "max_entry_bytes cannot exceed tenant max_bytes"
            )
        if result.burst_ops < result.ops_per_second:
            raise ControlPlaneError(
                "burst_ops cannot be below ops_per_second"
            )
        return result


@dataclass(frozen=True)
class BackupPolicy:
    interval_seconds: int
    retention_count: int
    retention_seconds: int

    def __post_init__(self) -> None:
        _integer(self.interval_seconds, "backup interval_seconds", minimum=0)
        _integer(self.retention_count, "backup retention_count", minimum=1)
        _integer(self.retention_seconds, "backup retention_seconds", minimum=1)


@dataclass(frozen=True)
class DisasterRecoveryPlan:
    rpo_seconds: int
    rto_seconds: int
    recovery_regions: Tuple[str, ...]

    def __post_init__(self) -> None:
        _integer(self.rpo_seconds, "rpo_seconds", minimum=1)
        _integer(self.rto_seconds, "rto_seconds", minimum=1)
        if not self.recovery_regions:
            raise ControlPlaneError("recovery_regions must not be empty")
        for region in self.recovery_regions:
            _identifier(region, "recovery region")


@dataclass(frozen=True)
class TenantRetention:
    usage_periods: int
    completed_operations: int
    export_count: int

    def __post_init__(self) -> None:
        _integer(self.usage_periods, "usage_periods", minimum=1)
        _integer(self.completed_operations, "completed_operations", minimum=1)
        _integer(self.export_count, "export_count", minimum=1)


@dataclass(frozen=True)
class TenantDefinition:
    tenant_id: str
    display_name: str
    enabled: bool
    quotas: TenantQuotas
    origins: Tuple[str, ...]
    webhooks: Tuple[str, ...]
    regions: Tuple[str, ...]
    primary_region: str
    desired_version: str
    desired_replicas: int
    max_unavailable: int
    drain: bool
    backup: BackupPolicy
    disaster_recovery: DisasterRecoveryPlan
    retention: TenantRetention

    def __post_init__(self) -> None:
        _identifier(self.tenant_id, "tenant id")
        _bounded_text(self.display_name, "tenant display_name", 256)
        if not isinstance(self.enabled, bool):
            raise ControlPlaneError("tenant enabled must be a boolean")
        if not self.regions or self.primary_region not in self.regions:
            raise ControlPlaneError("primary_region must be in regions")
        for field, values in (
            ("origins", self.origins),
            ("webhooks", self.webhooks),
            ("regions", self.regions),
        ):
            if len(values) != len(set(values)):
                raise ControlPlaneError("{} contains duplicates".format(field))
            for item in values:
                _identifier(item, field)
        if any(
            region not in self.regions
            for region in self.disaster_recovery.recovery_regions
        ):
            raise ControlPlaneError(
                "recovery_regions must be selected from tenant regions"
            )
        _bounded_text(self.desired_version, "deployment version", 128)
        _integer(self.desired_replicas, "deployment replicas", minimum=1)
        _integer(
            self.max_unavailable,
            "deployment max_unavailable",
            minimum=0,
            maximum=self.desired_replicas,
        )
        if not isinstance(self.drain, bool):
            raise ControlPlaneError("deployment drain must be a boolean")

    @classmethod
    def from_dict(
        cls,
        value: Mapping[str, Any],
        *,
        default_entries: int,
        default_bytes: int,
        default_entry_bytes: int,
        max_usage_periods: int,
        max_operations: int,
    ) -> "TenantDefinition":
        if not isinstance(value, Mapping):
            raise ControlPlaneError("each tenant must be an object")
        _keys_only(
            value,
            (
                "id",
                "display_name",
                "enabled",
                "quotas",
                "origins",
                "webhooks",
                "regions",
                "primary_region",
                "deployment",
                "backup",
                "disaster_recovery",
                "retention",
            ),
            "tenant",
        )
        tenant_id = _identifier(value.get("id"), "tenant id")
        display_name = value.get("display_name", tenant_id)
        _bounded_text(display_name, "tenant display_name", 256)
        enabled = value.get("enabled", True)
        if not isinstance(enabled, bool):
            raise ControlPlaneError("tenant enabled must be a boolean")
        quotas = TenantQuotas.from_dict(
            value.get("quotas", {}),
            default_entries=default_entries,
            default_bytes=default_bytes,
            default_entry_bytes=default_entry_bytes,
        )
        origins = _identifier_tuple(value.get("origins", []), "origins")
        webhooks = _identifier_tuple(value.get("webhooks", []), "webhooks")
        regions = _identifier_tuple(value.get("regions", ["local"]), "regions")
        if not regions:
            raise ControlPlaneError("tenant regions must not be empty")
        primary_region = _identifier(
            value.get("primary_region", regions[0]), "primary_region"
        )
        if primary_region not in regions:
            raise ControlPlaneError("primary_region must be in regions")

        deployment = value.get("deployment", {})
        if not isinstance(deployment, Mapping):
            raise ControlPlaneError("tenant deployment must be an object")
        _keys_only(
            deployment,
            ("version", "replicas", "max_unavailable", "drain"),
            "tenant deployment",
        )
        desired_version = _bounded_text(
            deployment.get("version", CONTROL_PLANE_VERSION),
            "deployment version",
            128,
        )
        desired_replicas = _integer(
            deployment.get("replicas", 1),
            "deployment replicas",
            minimum=1,
            maximum=10_000,
        )
        max_unavailable = _integer(
            deployment.get("max_unavailable", 1),
            "deployment max_unavailable",
            minimum=0,
            maximum=desired_replicas,
        )
        drain = deployment.get("drain", False)
        if not isinstance(drain, bool):
            raise ControlPlaneError("deployment drain must be a boolean")

        backup = value.get("backup", {})
        if not isinstance(backup, Mapping):
            raise ControlPlaneError("tenant backup must be an object")
        _keys_only(
            backup,
            ("interval_seconds", "retention_count", "retention_seconds"),
            "tenant backup",
        )
        backup_policy = BackupPolicy(
            interval_seconds=_integer(
                backup.get("interval_seconds", 0),
                "backup interval_seconds",
                minimum=0,
            ),
            retention_count=_integer(
                backup.get("retention_count", 24),
                "backup retention_count",
                minimum=1,
                maximum=10_000,
            ),
            retention_seconds=_integer(
                backup.get("retention_seconds", 2_592_000),
                "backup retention_seconds",
                minimum=1,
            ),
        )

        recovery = value.get("disaster_recovery", {})
        if not isinstance(recovery, Mapping):
            raise ControlPlaneError(
                "tenant disaster_recovery must be an object"
            )
        _keys_only(
            recovery,
            ("rpo_seconds", "rto_seconds", "recovery_regions"),
            "tenant disaster_recovery",
        )
        recovery_regions = _identifier_tuple(
            recovery.get(
                "recovery_regions",
                [region for region in regions if region != primary_region]
                or [primary_region],
            ),
            "recovery_regions",
        )
        if any(region not in regions for region in recovery_regions):
            raise ControlPlaneError(
                "recovery_regions must be selected from tenant regions"
            )
        dr_plan = DisasterRecoveryPlan(
            rpo_seconds=_integer(
                recovery.get("rpo_seconds", 3600),
                "rpo_seconds",
                minimum=1,
            ),
            rto_seconds=_integer(
                recovery.get("rto_seconds", 900),
                "rto_seconds",
                minimum=1,
            ),
            recovery_regions=recovery_regions,
        )

        retention = value.get("retention", {})
        if not isinstance(retention, Mapping):
            raise ControlPlaneError("tenant retention must be an object")
        _keys_only(
            retention,
            ("usage_periods", "completed_operations", "export_count"),
            "tenant retention",
        )
        retention_policy = TenantRetention(
            usage_periods=_integer(
                retention.get("usage_periods", max_usage_periods),
                "retention usage_periods",
                minimum=1,
                maximum=max_usage_periods,
            ),
            completed_operations=_integer(
                retention.get("completed_operations", max_operations),
                "retention completed_operations",
                minimum=1,
                maximum=max_operations,
            ),
            export_count=_integer(
                retention.get("export_count", 10),
                "retention export_count",
                minimum=1,
                maximum=10_000,
            ),
        )
        return cls(
            tenant_id=tenant_id,
            display_name=display_name,
            enabled=enabled,
            quotas=quotas,
            origins=origins,
            webhooks=webhooks,
            regions=regions,
            primary_region=primary_region,
            desired_version=desired_version,
            desired_replicas=desired_replicas,
            max_unavailable=max_unavailable,
            drain=drain,
            backup=backup_policy,
            disaster_recovery=dr_plan,
            retention=retention_policy,
        )


@dataclass(frozen=True)
class ControlPlaneDefinition:
    default_tenant: str
    tenants: Tuple[TenantDefinition, ...]

    def __post_init__(self) -> None:
        _identifier(self.default_tenant, "default tenant")
        identifiers = tuple(tenant.tenant_id for tenant in self.tenants)
        if not identifiers:
            raise ControlPlaneError("at least one tenant is required")
        if len(identifiers) != len(set(identifiers)):
            raise ControlPlaneError("tenant ids must be unique")
        if self.default_tenant not in identifiers:
            raise ControlPlaneError("default tenant is not configured")

    @property
    def tenant_ids(self) -> Tuple[str, ...]:
        return tuple(tenant.tenant_id for tenant in self.tenants)

    def tenant(self, tenant_id: str) -> TenantDefinition:
        for tenant in self.tenants:
            if tenant.tenant_id == tenant_id:
                return tenant
        raise TenantNotFound("tenant is not configured")


def load_control_plane_definition(
    path: str,
    *,
    max_tenants: int,
    max_entries: int,
    max_bytes: int,
    max_entry_bytes: int,
    max_usage_periods: int,
    max_operations: int,
) -> ControlPlaneDefinition:
    document = _read_json_file(path, "control-plane file", 1_048_576)
    _keys_only(document, ("version", "default_tenant", "tenants"), "control plane")
    if document.get("version") != CONTROL_CONFIG_VERSION:
        raise ControlPlaneError("unsupported control-plane configuration version")
    raw_tenants = document.get("tenants")
    if not isinstance(raw_tenants, list) or not raw_tenants:
        raise ControlPlaneError("control plane must define at least one tenant")
    if len(raw_tenants) > max_tenants:
        raise ControlPlaneCapacity("configured tenant count exceeds its limit")
    tenant_default_entries = max(1, max_entries // len(raw_tenants))
    tenant_default_bytes = max(1, max_bytes // len(raw_tenants))
    tenants = tuple(
        TenantDefinition.from_dict(
            item,
            default_entries=tenant_default_entries,
            default_bytes=tenant_default_bytes,
            default_entry_bytes=min(max_entry_bytes, tenant_default_bytes),
            max_usage_periods=max_usage_periods,
            max_operations=max_operations,
        )
        for item in raw_tenants
    )
    identifiers = tuple(tenant.tenant_id for tenant in tenants)
    if len(identifiers) != len(set(identifiers)):
        raise ControlPlaneError("tenant ids must be unique")
    if sum(tenant.quotas.max_entries for tenant in tenants) > max_entries:
        raise ControlPlaneCapacity(
            "tenant entry quotas exceed MEGACACHE_MAX_ENTRIES"
        )
    if sum(tenant.quotas.max_bytes for tenant in tenants) > max_bytes:
        raise ControlPlaneCapacity(
            "tenant byte quotas exceed MEGACACHE_MAX_MEMORY_BYTES"
        )
    if any(
        tenant.quotas.max_entry_bytes > max_entry_bytes for tenant in tenants
    ):
        raise ControlPlaneCapacity(
            "tenant entry-size quota exceeds MEGACACHE_MAX_ENTRY_BYTES"
        )
    assigned_webhooks: Dict[str, str] = {}
    for tenant in tenants:
        for source in tenant.webhooks:
            previous = assigned_webhooks.setdefault(source, tenant.tenant_id)
            if previous != tenant.tenant_id:
                raise ControlPlaneError(
                    "webhook '{}' is assigned to multiple tenants".format(source)
                )
    default_tenant = _identifier(
        document.get("default_tenant", identifiers[0]), "default_tenant"
    )
    if default_tenant not in identifiers:
        raise ControlPlaneError("default_tenant must name a configured tenant")
    return ControlPlaneDefinition(default_tenant, tenants)


class KeyProvider(Protocol):
    """Replaceable source for active and historical 256-bit keys."""

    def active_key_id(self) -> str: ...

    def namespace_key_id(self) -> str: ...

    def key(self, key_id: str) -> bytes: ...


class StaticKeyProvider:
    """In-memory key provider useful for embedding and tests."""

    def __init__(self, key: bytes, key_id: str = "local-v1") -> None:
        self._key_id = _identifier(key_id, "key id")
        self._key = _validate_key_material(key)

    def active_key_id(self) -> str:
        return self._key_id

    def namespace_key_id(self) -> str:
        return self._key_id

    def key(self, key_id: str) -> bytes:
        if not hmac.compare_digest(key_id, self._key_id):
            raise ControlPlaneUnavailable("requested key id is unavailable")
        return self._key


class JSONFileKeyProvider:
    """Protected local JSON keyring; external KMS adapters implement KeyProvider."""

    def __init__(self, path: str) -> None:
        document = _read_json_file(path, "control key file", 65_536)
        _keys_only(
            document,
            ("version", "active_key_id", "namespace_key_id", "keys"),
            "key file",
        )
        if document.get("version") != 1:
            raise ControlPlaneError("unsupported control key file version")
        active = _identifier(document.get("active_key_id"), "active_key_id")
        raw_keys = document.get("keys")
        if not isinstance(raw_keys, Mapping) or not raw_keys:
            raise ControlPlaneError("control key file must contain keys")
        keys: Dict[str, bytes] = {}
        for key_id, encoded in raw_keys.items():
            normalized = _identifier(key_id, "key id")
            keys[normalized] = _decode_key(encoded)
        if active not in keys:
            raise ControlPlaneError("active_key_id is absent from keys")
        namespace_key = _identifier(
            document.get("namespace_key_id", active), "namespace_key_id"
        )
        if namespace_key not in keys:
            raise ControlPlaneError("namespace_key_id is absent from keys")
        metadata = os.stat(os.path.abspath(path), follow_symlinks=False)
        if metadata.st_mode & 0o077:
            raise ControlPlaneError(
                "control key file must not be accessible by group or others"
            )
        self._active = active
        self._namespace = namespace_key
        self._keys = keys

    def active_key_id(self) -> str:
        return self._active

    def namespace_key_id(self) -> str:
        return self._namespace

    def key(self, key_id: str) -> bytes:
        try:
            return self._keys[key_id]
        except KeyError as exc:
            raise ControlPlaneUnavailable(
                "requested key id is unavailable"
            ) from exc


def load_key_provider(
    encoded_key: Optional[str], key_file: Optional[str]
) -> KeyProvider:
    if bool(encoded_key) == bool(key_file):
        raise ControlPlaneError(
            "configure exactly one control master key or control key file"
        )
    if key_file is not None:
        return JSONFileKeyProvider(key_file)
    assert encoded_key is not None
    return StaticKeyProvider(_decode_key(encoded_key))


def _decode_key(value: Any) -> bytes:
    if not isinstance(value, str) or len(value) > 4096:
        raise ControlPlaneError("control key must be bounded base64")
    try:
        padded = value + "=" * (-len(value) % 4)
        key = base64.b64decode(
            padded.encode("ascii"), altchars=b"-_", validate=True
        )
    except (ValueError, UnicodeEncodeError, binascii.Error) as exc:
        raise ControlPlaneError("control key must be valid base64") from exc
    return _validate_key_material(key)


def _validate_key_material(value: bytes) -> bytes:
    if not isinstance(value, bytes) or not 32 <= len(value) <= 64:
        raise ControlPlaneError("control keys must contain 32 to 64 bytes")
    return bytes(value)


def _derived_key(provider: KeyProvider, key_id: str, purpose: bytes) -> bytes:
    return hmac.new(
        provider.key(key_id),
        b"megacache-control-v1\x00" + purpose,
        hashlib.sha256,
    ).digest()


def tenant_namespace_id(provider: KeyProvider, tenant_id: str) -> str:
    normalized = _identifier(tenant_id, "tenant id")
    namespace_key = getattr(provider, "namespace_key_id", None)
    key_id = (
        provider.active_key_id()
        if namespace_key is None
        else namespace_key()
    )
    digest = hmac.new(
        _derived_key(provider, key_id, b"tenant-namespace"),
        normalized.encode("utf-8"),
        hashlib.sha256,
    ).hexdigest()
    return digest[:32]


class AtomicControlState:
    """Atomically replaced, size-bounded state with one advisory owner."""

    def __init__(self, directory: str, maximum_bytes: int) -> None:
        self.directory = _secure_directory(directory)
        self.path = os.path.join(self.directory, "control-state.json")
        self.lock_path = self.path + ".lock"
        self.maximum_bytes = _integer(
            maximum_bytes, "control state maximum_bytes", minimum=1024
        )
        self._descriptor: Optional[int] = None
        self._closed = False
        self._acquire_lock()

    def _acquire_lock(self) -> None:
        flags = os.O_RDWR | os.O_CREAT | getattr(os, "O_CLOEXEC", 0)
        flags |= getattr(os, "O_NOFOLLOW", 0)
        descriptor = os.open(self.lock_path, flags, 0o600)
        os.fchmod(descriptor, 0o600)
        try:
            fcntl.flock(descriptor, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except (BlockingIOError, OSError) as exc:
            os.close(descriptor)
            raise ControlPlaneUnavailable(
                "control state is already owned by another process"
            ) from exc
        self._descriptor = descriptor

    def load(self) -> Optional[Dict[str, Any]]:
        try:
            metadata = os.lstat(self.path)
        except FileNotFoundError:
            return None
        if stat.S_ISLNK(metadata.st_mode) or not stat.S_ISREG(metadata.st_mode):
            raise ControlPlaneError("control state must be a regular file")
        if metadata.st_mode & 0o077:
            raise ControlPlaneError(
                "control state must not be accessible by group or others"
            )
        if metadata.st_size > self.maximum_bytes:
            raise ControlPlaneCapacity(
                "existing control state exceeds configured byte limit"
            )
        try:
            descriptor = os.open(
                self.path,
                os.O_RDONLY
                | getattr(os, "O_CLOEXEC", 0)
                | getattr(os, "O_NOFOLLOW", 0),
            )
            with os.fdopen(descriptor, "r", encoding="utf-8") as source:
                value = json.load(source)
        except (OSError, json.JSONDecodeError) as exc:
            raise ControlPlaneError(
                "unable to load control state: {}".format(exc)
            ) from exc
        if not isinstance(value, dict):
            raise ControlPlaneError("control state must contain an object")
        return value

    def persist(self, state: Mapping[str, Any]) -> None:
        if self._closed:
            raise ControlPlaneUnavailable("control state is closed")
        _atomic_write(self.path, _canonical(state), self.maximum_bytes)

    def close(self) -> None:
        if self._closed:
            return
        self._closed = True
        descriptor = self._descriptor
        self._descriptor = None
        if descriptor is not None:
            try:
                fcntl.flock(descriptor, fcntl.LOCK_UN)
            finally:
                os.close(descriptor)

    def __del__(self) -> None:
        try:
            self.close()
        except Exception:
            pass


class TamperEvidentAuditLog:
    """Append-only HMAC hash chain split into bounded rotation segments."""

    def __init__(
        self,
        directory: str,
        provider: KeyProvider,
        *,
        segment_bytes: int,
        max_segments: int,
        max_record_bytes: int = 16_384,
        clock: Callable[[], float] = time.time,
    ) -> None:
        self.directory = _secure_directory(os.path.join(directory, "audit"))
        self.provider = provider
        self.segment_bytes = _integer(
            segment_bytes, "audit segment_bytes", minimum=1024
        )
        self.max_segments = _integer(
            max_segments, "audit max_segments", minimum=1
        )
        self.max_record_bytes = _integer(
            max_record_bytes, "audit max_record_bytes", minimum=512
        )
        self._clock = clock
        self._lock = threading.RLock()
        self._records: List[Dict[str, Any]] = []
        self._segments: List[int] = []
        self._segment_ranges: Dict[int, Tuple[int, int]] = {}
        self._anchor_path = os.path.join(self.directory, "audit-anchor.json")
        self._anchor_sequence = 0
        self._anchor_hash = _ZERO_HASH
        self._load_anchor()
        self._load_and_verify()

    @property
    def head_hash(self) -> str:
        with self._lock:
            return (
                self._anchor_hash
                if not self._records
                else str(self._records[-1]["hash"])
            )

    @property
    def sequence(self) -> int:
        with self._lock:
            return (
                self._anchor_sequence
                if not self._records
                else int(self._records[-1]["sequence"])
            )

    def append(
        self,
        *,
        actor: str,
        action: str,
        outcome: str,
        tenant_namespace: Optional[str] = None,
        details: Optional[Mapping[str, Any]] = None,
    ) -> Dict[str, Any]:
        actor_value = _bounded_text(actor, "audit actor", 256)
        action_value = _identifier(action, "audit action")
        outcome_value = _identifier(outcome, "audit outcome")
        if tenant_namespace is not None:
            tenant_namespace = _identifier(
                tenant_namespace, "tenant namespace"
            )
        bounded_details = _json_copy(dict(details or {}))
        if len(_canonical(bounded_details)) > 8192:
            raise ControlPlaneCapacity("audit details exceed byte limit")
        with self._lock:
            key_id = self.provider.active_key_id()
            record = {
                "version": 1,
                "sequence": self.sequence + 1,
                "timestamp": self._clock(),
                "key_id": key_id,
                "previous_hash": self.head_hash,
                "tenant_namespace": tenant_namespace,
                "actor": actor_value,
                "action": action_value,
                "outcome": outcome_value,
                "details": bounded_details,
            }
            digest = hmac.new(
                _derived_key(self.provider, key_id, b"audit-record"),
                _canonical(record),
                hashlib.sha256,
            ).hexdigest()
            record["hash"] = digest
            payload = _canonical(record) + b"\n"
            if len(payload) > self.max_record_bytes:
                raise ControlPlaneCapacity("audit record exceeds byte limit")
            segment = self._segment_for_append(len(payload))
            path = self._segment_path(segment)
            flags = os.O_WRONLY | os.O_APPEND | os.O_CREAT
            flags |= getattr(os, "O_CLOEXEC", 0)
            flags |= getattr(os, "O_NOFOLLOW", 0)
            descriptor = os.open(path, flags, 0o600)
            try:
                os.fchmod(descriptor, 0o600)
                with os.fdopen(descriptor, "ab") as destination:
                    destination.write(payload)
                    destination.flush()
                    os.fsync(destination.fileno())
            except Exception:
                try:
                    os.close(descriptor)
                except OSError:
                    pass
                raise
            self._records.append(record)
            current_range = self._segment_ranges.get(segment)
            sequence = int(record["sequence"])
            self._segment_ranges[segment] = (
                sequence if current_range is None else current_range[0],
                sequence,
            )
            try:
                directory_descriptor = os.open(
                    self.directory,
                    os.O_RDONLY
                    | getattr(os, "O_CLOEXEC", 0)
                    | getattr(os, "O_DIRECTORY", 0),
                )
                try:
                    os.fsync(directory_descriptor)
                finally:
                    os.close(directory_descriptor)
            except OSError:
                pass
            return _json_copy(record)

    def export(
        self,
        *,
        tenant_namespace: Optional[str] = None,
        after_sequence: int = 0,
        limit: int = 1000,
    ) -> Dict[str, Any]:
        after = _integer(
            after_sequence, "after_sequence", minimum=0, maximum=2**63 - 1
        )
        bounded_limit = _integer(limit, "audit limit", minimum=1, maximum=1000)
        with self._lock:
            selected = [
                record
                for record in self._records
                if int(record["sequence"]) > after
                and (
                    tenant_namespace is None
                    or record["tenant_namespace"] == tenant_namespace
                )
            ][:bounded_limit]
            return {
                "version": 1,
                "chain_valid": True,
                "anchor": {
                    "sequence": self._anchor_sequence,
                    "hash": self._anchor_hash,
                },
                "head_hash": self.head_hash,
                "last_sequence": self.sequence,
                "records": _json_copy(selected),
                "truncated": len(selected) == bounded_limit
                and any(
                    int(record["sequence"]) > int(selected[-1]["sequence"])
                    and (
                        tenant_namespace is None
                        or record["tenant_namespace"] == tenant_namespace
                    )
                    for record in self._records
                ),
            }

    def status(self) -> Dict[str, Any]:
        with self._lock:
            return {
                "records": len(self._records),
                "segments": len(self._segments),
                "max_segments": self.max_segments,
                "segment_bytes": self.segment_bytes,
                "last_sequence": self.sequence,
                "head_hash": self.head_hash,
                "anchor_sequence": self._anchor_sequence,
                "anchor_hash": self._anchor_hash,
                "verified": True,
            }

    def prune_exported(
        self, through_sequence: int, expected_hash: str
    ) -> Dict[str, Any]:
        through = _integer(
            through_sequence,
            "audit prune sequence",
            minimum=1,
            maximum=2**63 - 1,
        )
        if (
            not isinstance(expected_hash, str)
            or re.fullmatch(r"[a-f0-9]{64}", expected_hash) is None
        ):
            raise ControlPlaneError("audit prune hash is invalid")
        with self._lock:
            record = next(
                (
                    item
                    for item in self._records
                    if int(item["sequence"]) == through
                ),
                None,
            )
            if record is None or not hmac.compare_digest(
                str(record["hash"]), expected_hash
            ):
                raise ControlPlaneError(
                    "audit prune confirmation does not match retained history"
                )
            eligible = [
                number
                for number, (_, maximum) in self._segment_ranges.items()
                if maximum <= through
            ]
            if not eligible:
                return {
                    "removed_segments": 0,
                    "anchor_sequence": self._anchor_sequence,
                    "anchor_hash": self._anchor_hash,
                }
            anchor_sequence = max(
                self._segment_ranges[number][1] for number in eligible
            )
            anchor_record = next(
                item
                for item in self._records
                if int(item["sequence"]) == anchor_sequence
            )
            self._write_anchor(
                anchor_sequence, str(anchor_record["hash"])
            )
            removed = 0
            for number in sorted(eligible):
                path = self._segment_path(number)
                try:
                    metadata = os.lstat(path)
                    if stat.S_ISLNK(metadata.st_mode) or not stat.S_ISREG(
                        metadata.st_mode
                    ):
                        raise ControlPlaneError(
                            "audit segment must be a regular file"
                        )
                    os.unlink(path)
                    removed += 1
                except FileNotFoundError:
                    continue
                self._segment_ranges.pop(number, None)
            self._segments = [
                number for number in self._segments if number not in eligible
            ]
            self._records = [
                item
                for item in self._records
                if int(item["sequence"]) > anchor_sequence
            ]
            return {
                "removed_segments": removed,
                "anchor_sequence": self._anchor_sequence,
                "anchor_hash": self._anchor_hash,
            }

    def _segment_for_append(self, payload_bytes: int) -> int:
        if not self._segments:
            self._segments.append(1)
            return 1
        current = self._segments[-1]
        path = self._segment_path(current)
        try:
            metadata = os.lstat(path)
        except FileNotFoundError:
            size = 0
        else:
            if stat.S_ISLNK(metadata.st_mode) or not stat.S_ISREG(
                metadata.st_mode
            ):
                raise ControlPlaneError(
                    "audit segment must be a regular file"
                )
            size = metadata.st_size
        if size + payload_bytes <= self.segment_bytes:
            return current
        if len(self._segments) >= self.max_segments:
            raise ControlPlaneCapacity(
                "audit segment capacity is exhausted; export records and "
                "increase the configured retention bound"
            )
        current += 1
        self._segments.append(current)
        return current

    def _load_and_verify(self) -> None:
        names = []
        for name in os.listdir(self.directory):
            match = _AUDIT_SEGMENT.fullmatch(name)
            if match is not None:
                names.append((int(match.group(1)), name))
        names.sort()
        if len(names) > self.max_segments:
            raise ControlPlaneCapacity(
                "existing audit segments exceed configured limit"
            )
        previous = self._anchor_hash
        sequence = self._anchor_sequence
        for number, name in names:
            path = os.path.join(self.directory, name)
            metadata = os.lstat(path)
            if stat.S_ISLNK(metadata.st_mode) or not stat.S_ISREG(
                metadata.st_mode
            ):
                raise ControlPlaneError(
                    "audit segments must be regular files"
                )
            if metadata.st_mode & 0o077:
                raise ControlPlaneError(
                    "audit segments must not be accessible by group or others"
                )
            if metadata.st_size > self.segment_bytes + self.max_record_bytes:
                raise ControlPlaneCapacity(
                    "existing audit segment exceeds configured limit"
                )
            descriptor = os.open(
                path,
                os.O_RDONLY
                | getattr(os, "O_CLOEXEC", 0)
                | getattr(os, "O_NOFOLLOW", 0),
            )
            segment_min = None
            segment_max = None
            with os.fdopen(descriptor, "rb") as source:
                for raw in source:
                    if len(raw) > self.max_record_bytes:
                        raise ControlPlaneCapacity(
                            "existing audit record exceeds configured limit"
                        )
                    try:
                        record = json.loads(raw)
                    except json.JSONDecodeError as exc:
                        raise ControlPlaneError(
                            "audit log contains invalid JSON"
                        ) from exc
                    if not isinstance(record, dict):
                        raise ControlPlaneError(
                            "audit log contains an invalid record"
                        )
                    supplied = record.pop("hash", None)
                    record_sequence = record.get("sequence")
                    if (
                        isinstance(record_sequence, int)
                        and record_sequence <= self._anchor_sequence
                    ):
                        continue
                    expected = hmac.new(
                        _derived_key(
                            self.provider,
                            _identifier(record.get("key_id"), "audit key id"),
                            b"audit-record",
                        ),
                        _canonical(record),
                        hashlib.sha256,
                    ).hexdigest()
                    if (
                        not isinstance(supplied, str)
                        or not hmac.compare_digest(supplied, expected)
                        or record.get("previous_hash") != previous
                        or record.get("sequence") != sequence + 1
                    ):
                        raise ControlPlaneError(
                            "audit chain verification failed"
                        )
                    record["hash"] = supplied
                    previous = supplied
                    sequence += 1
                    self._records.append(record)
                    segment_min = (
                        sequence if segment_min is None else segment_min
                    )
                    segment_max = sequence
            if segment_min is not None and segment_max is not None:
                self._segments.append(number)
                self._segment_ranges[number] = (segment_min, segment_max)
            elif self._anchor_sequence > 0:
                os.unlink(path)
            else:
                self._segments.append(number)

    def _segment_path(self, number: int) -> str:
        return os.path.join(self.directory, "audit-{:08d}.jsonl".format(number))

    def _load_anchor(self) -> None:
        try:
            metadata = os.lstat(self._anchor_path)
        except FileNotFoundError:
            return
        if (
            stat.S_ISLNK(metadata.st_mode)
            or not stat.S_ISREG(metadata.st_mode)
            or metadata.st_mode & 0o077
        ):
            raise ControlPlaneError("audit anchor must be a protected regular file")
        document = _read_json_file(
            self._anchor_path, "audit anchor", self.max_record_bytes
        )
        if set(document) != {
            "version",
            "sequence",
            "hash",
            "key_id",
            "signature",
        }:
            raise ControlPlaneError("audit anchor has unsupported fields")
        signature = document.pop("signature")
        key_id = _identifier(document.get("key_id"), "audit anchor key id")
        expected = hmac.new(
            _derived_key(self.provider, key_id, b"audit-anchor"),
            _canonical(document),
            hashlib.sha256,
        ).hexdigest()
        if (
            document.get("version") != 1
            or not isinstance(document.get("sequence"), int)
            or document["sequence"] < 0
            or not isinstance(document.get("hash"), str)
            or re.fullmatch(r"[a-f0-9]{64}", document["hash"]) is None
            or not isinstance(signature, str)
            or not hmac.compare_digest(signature, expected)
        ):
            raise ControlPlaneError("audit anchor verification failed")
        self._anchor_sequence = document["sequence"]
        self._anchor_hash = document["hash"]

    def _write_anchor(self, sequence: int, digest: str) -> None:
        key_id = self.provider.active_key_id()
        document = {
            "version": 1,
            "sequence": sequence,
            "hash": digest,
            "key_id": key_id,
        }
        document["signature"] = hmac.new(
            _derived_key(self.provider, key_id, b"audit-anchor"),
            _canonical(document),
            hashlib.sha256,
        ).hexdigest()
        _atomic_write(
            self._anchor_path,
            _canonical(document),
            self.max_record_bytes,
        )
        self._anchor_sequence = sequence
        self._anchor_hash = digest


class EncryptedArtifactStore:
    """Bounded authenticated encryption using an HMAC-SHA256 PRF stream."""

    def __init__(
        self, directory: str, provider: KeyProvider, maximum_bytes: int
    ) -> None:
        self.directory = _secure_directory(os.path.join(directory, "artifacts"))
        self.provider = provider
        self.maximum_bytes = _integer(
            maximum_bytes, "artifact maximum_bytes", minimum=1024
        )

    def write(
        self,
        kind: str,
        tenant_namespace: str,
        document: Mapping[str, Any],
    ) -> Dict[str, Any]:
        if kind not in ("backup", "export", "audit-export"):
            raise ArtifactError("unsupported artifact kind")
        namespace = _identifier(tenant_namespace, "tenant namespace")
        plaintext = _canonical(document)
        if len(plaintext) > self.maximum_bytes:
            raise ControlPlaneCapacity(
                "{} payload exceeds configured byte limit".format(kind)
            )
        key_id = self.provider.active_key_id()
        nonce = secrets.token_bytes(32)
        encryption_key = _derived_key(
            self.provider,
            key_id,
            b"artifact-encryption\x00"
            + namespace.encode("ascii")
            + b"\x00"
            + kind.encode("ascii"),
        )
        authentication_key = _derived_key(
            self.provider,
            key_id,
            b"artifact-authentication\x00"
            + namespace.encode("ascii")
            + b"\x00"
            + kind.encode("ascii"),
        )
        ciphertext = self._xor_stream(plaintext, encryption_key, nonce)
        header = {
            "version": 1,
            "algorithm": "HMAC-SHA256-STREAM-ETM",
            "kind": kind,
            "tenant_namespace": namespace,
            "key_id": key_id,
            "nonce": base64.b64encode(nonce).decode("ascii"),
            "plaintext_sha256": hashlib.sha256(plaintext).hexdigest(),
        }
        tag = hmac.new(
            authentication_key,
            _canonical(header) + b"\x00" + ciphertext,
            hashlib.sha256,
        ).hexdigest()
        envelope = dict(header)
        envelope["ciphertext"] = base64.b64encode(ciphertext).decode("ascii")
        envelope["tag"] = tag
        payload = _canonical(envelope)
        if len(payload) > self.maximum_bytes:
            raise ControlPlaneCapacity(
                "{} artifact exceeds configured byte limit".format(kind)
            )
        artifact_id = secrets.token_hex(16)
        filename = "{}-{}.mcar".format(kind, artifact_id)
        _atomic_write(
            os.path.join(self.directory, filename),
            payload,
            self.maximum_bytes,
        )
        return {
            "artifact_id": artifact_id,
            "filename": filename,
            "kind": kind,
            "key_id": key_id,
            "size_bytes": len(payload),
            "sha256": hashlib.sha256(payload).hexdigest(),
            "plaintext_sha256": header["plaintext_sha256"],
        }

    def read(
        self,
        kind: str,
        artifact_id: str,
        tenant_namespace: str,
    ) -> Dict[str, Any]:
        if kind not in ("backup", "export", "audit-export"):
            raise ArtifactError("unsupported artifact kind")
        if not isinstance(artifact_id, str) or _ARTIFACT_ID.fullmatch(
            artifact_id
        ) is None:
            raise ArtifactError("invalid artifact id")
        namespace = _identifier(tenant_namespace, "tenant namespace")
        filename = "{}-{}.mcar".format(kind, artifact_id)
        path = os.path.join(self.directory, filename)
        try:
            metadata = os.lstat(path)
        except FileNotFoundError as exc:
            raise ArtifactError("artifact is not available") from exc
        if (
            stat.S_ISLNK(metadata.st_mode)
            or not stat.S_ISREG(metadata.st_mode)
            or metadata.st_mode & 0o077
        ):
            raise ArtifactError(
                "artifact must be a protected regular file"
            )
        envelope = _read_json_file(path, "{} artifact".format(kind), self.maximum_bytes)
        required = {
            "version",
            "algorithm",
            "kind",
            "tenant_namespace",
            "key_id",
            "nonce",
            "plaintext_sha256",
            "ciphertext",
            "tag",
        }
        if set(envelope) != required:
            raise ArtifactError("artifact envelope has unsupported fields")
        if (
            envelope["version"] != 1
            or envelope["algorithm"] != "HMAC-SHA256-STREAM-ETM"
            or envelope["kind"] != kind
            or envelope["tenant_namespace"] != namespace
        ):
            raise ArtifactError("artifact identity does not match request")
        key_id = _identifier(envelope["key_id"], "artifact key id")
        try:
            nonce = base64.b64decode(envelope["nonce"], validate=True)
            ciphertext = base64.b64decode(
                envelope["ciphertext"], validate=True
            )
        except (ValueError, TypeError, binascii.Error) as exc:
            raise ArtifactError("artifact encoding is invalid") from exc
        if len(nonce) != 32 or len(ciphertext) > self.maximum_bytes:
            raise ArtifactError("artifact ciphertext is invalid")
        header = {
            key: envelope[key]
            for key in (
                "version",
                "algorithm",
                "kind",
                "tenant_namespace",
                "key_id",
                "nonce",
                "plaintext_sha256",
            )
        }
        authentication_key = _derived_key(
            self.provider,
            key_id,
            b"artifact-authentication\x00"
            + namespace.encode("ascii")
            + b"\x00"
            + kind.encode("ascii"),
        )
        expected = hmac.new(
            authentication_key,
            _canonical(header) + b"\x00" + ciphertext,
            hashlib.sha256,
        ).hexdigest()
        supplied = envelope["tag"]
        if not isinstance(supplied, str) or not hmac.compare_digest(
            supplied, expected
        ):
            raise ArtifactError("artifact authentication failed")
        encryption_key = _derived_key(
            self.provider,
            key_id,
            b"artifact-encryption\x00"
            + namespace.encode("ascii")
            + b"\x00"
            + kind.encode("ascii"),
        )
        plaintext = self._xor_stream(ciphertext, encryption_key, nonce)
        if not hmac.compare_digest(
            hashlib.sha256(plaintext).hexdigest(),
            str(envelope["plaintext_sha256"]),
        ):
            raise ArtifactError("artifact plaintext checksum failed")
        try:
            document = json.loads(plaintext)
        except (UnicodeDecodeError, json.JSONDecodeError) as exc:
            raise ArtifactError("artifact plaintext is invalid") from exc
        if not isinstance(document, dict):
            raise ArtifactError("artifact plaintext must contain an object")
        return document

    def delete(self, kind: str, artifact_id: str) -> None:
        if kind not in ("backup", "export", "audit-export"):
            raise ArtifactError("unsupported artifact kind")
        if not isinstance(artifact_id, str) or _ARTIFACT_ID.fullmatch(
            artifact_id
        ) is None:
            raise ArtifactError("invalid artifact id")
        path = os.path.join(
            self.directory, "{}-{}.mcar".format(kind, artifact_id)
        )
        try:
            metadata = os.lstat(path)
            if stat.S_ISLNK(metadata.st_mode) or not stat.S_ISREG(
                metadata.st_mode
            ):
                raise ArtifactError("artifact must be a regular file")
            os.unlink(path)
        except FileNotFoundError:
            return

    def delete_namespace(self, tenant_namespace: str) -> int:
        namespace = _identifier(tenant_namespace, "tenant namespace")
        removed = 0
        for name in os.listdir(self.directory):
            match = _ARTIFACT_FILE.fullmatch(name)
            if match is None:
                continue
            path = os.path.join(self.directory, name)
            metadata = os.lstat(path)
            if stat.S_ISLNK(metadata.st_mode) or not stat.S_ISREG(
                metadata.st_mode
            ):
                raise ArtifactError(
                    "artifact directory contains an unsafe entry"
                )
            document = _read_json_file(
                path, "encrypted artifact", self.maximum_bytes
            )
            if document.get("tenant_namespace") != namespace:
                continue
            os.unlink(path)
            removed += 1
        return removed

    @staticmethod
    def _xor_stream(payload: bytes, key: bytes, nonce: bytes) -> bytes:
        output = bytearray(len(payload))
        offset = 0
        counter = 0
        while offset < len(payload):
            block = hmac.new(
                key,
                nonce + counter.to_bytes(8, "big"),
                hashlib.sha256,
            ).digest()
            count = min(len(block), len(payload) - offset)
            for index in range(count):
                output[offset + index] = payload[offset + index] ^ block[index]
            offset += count
            counter += 1
        return bytes(output)


class BillingProvider(Protocol):
    """Replaceable billing sink; implementations must honor batch_id."""

    def export_usage(
        self, batch_id: str, payload: Mapping[str, Any]
    ) -> Mapping[str, Any]: ...


class JSONFileBillingProvider:
    """Local deterministic billing sink for integration and air-gapped export."""

    def __init__(self, directory: str, maximum_bytes: int = 8_388_608) -> None:
        self.directory = _secure_directory(directory)
        self.maximum_bytes = _integer(
            maximum_bytes, "billing export maximum_bytes", minimum=1024
        )

    def export_usage(
        self, batch_id: str, payload: Mapping[str, Any]
    ) -> Mapping[str, Any]:
        normalized = _identifier(batch_id, "billing batch id")
        encoded = _canonical(payload)
        path = os.path.join(self.directory, normalized + ".json")
        if os.path.exists(path):
            outcome = self._verify_existing(path, encoded)
        else:
            if len(encoded) > self.maximum_bytes:
                raise ControlPlaneCapacity(
                    "billing export exceeds configured byte limit"
                )
            flags = os.O_WRONLY | os.O_CREAT | os.O_EXCL
            flags |= getattr(os, "O_CLOEXEC", 0)
            flags |= getattr(os, "O_NOFOLLOW", 0)
            try:
                descriptor = os.open(path, flags, 0o600)
            except FileExistsError:
                outcome = self._verify_existing(path, encoded)
            else:
                with os.fdopen(descriptor, "wb") as destination:
                    destination.write(encoded)
                    destination.flush()
                    os.fsync(destination.fileno())
                try:
                    directory_descriptor = os.open(
                        self.directory,
                        os.O_RDONLY
                        | getattr(os, "O_CLOEXEC", 0)
                        | getattr(os, "O_DIRECTORY", 0),
                    )
                    try:
                        os.fsync(directory_descriptor)
                    finally:
                        os.close(directory_descriptor)
                except OSError:
                    pass
                outcome = "written"
        return {"outcome": outcome, "path": path}

    def _verify_existing(self, path: str, encoded: bytes) -> str:
        existing = _read_json_file(path, "billing export", self.maximum_bytes)
        if not hmac.compare_digest(
            hashlib.sha256(_canonical(existing)).digest(),
            hashlib.sha256(encoded).digest(),
        ):
            raise ControlPlaneError(
                "billing batch id already exists with different content"
            )
        return "already_present"


class _TenantRuntime:
    def __init__(
        self,
        definition: TenantDefinition,
        engine: StorageBackend,
        namespace_id: str,
        monotonic: Callable[[], float],
    ) -> None:
        self.definition = definition
        self.engine = engine
        self.namespace_id = namespace_id
        self._monotonic = monotonic
        self._tokens = float(definition.quotas.burst_ops)
        self._last_refill = monotonic()
        self._connections = 0
        self._active_operations = 0
        self._accepting = bool(definition.enabled)
        self._maintenance = False
        self._condition = threading.Condition(threading.RLock())
        self.quota_rejections = 0
        self.connection_rejections = 0

    @property
    def connections(self) -> int:
        with self._condition:
            return self._connections

    @property
    def active_operations(self) -> int:
        with self._condition:
            return self._active_operations

    @property
    def maintenance(self) -> bool:
        with self._condition:
            return self._maintenance

    def set_accepting(self, value: bool) -> None:
        with self._condition:
            self._accepting = bool(value)
            self._condition.notify_all()

    def acquire_connection(self) -> None:
        with self._condition:
            if self._connections >= self.definition.quotas.max_connections:
                self.connection_rejections += 1
                raise TenantQuotaExceeded(
                    "tenant connection quota is exhausted"
                )
            self._connections += 1

    def release_connection(self) -> None:
        with self._condition:
            if self._connections > 0:
                self._connections -= 1
            self._condition.notify_all()

    def begin_operation(self, allow_unavailable: bool = False) -> None:
        with self._condition:
            if not self._accepting and not allow_unavailable:
                raise TenantUnavailable("tenant data plane is not accepting work")
            if self._maintenance and not allow_unavailable:
                raise TenantUnavailable(
                    "tenant data plane is in a maintenance window"
                )
            now = self._monotonic()
            elapsed = max(0.0, now - self._last_refill)
            self._tokens = min(
                float(self.definition.quotas.burst_ops),
                self._tokens
                + elapsed * float(self.definition.quotas.ops_per_second),
            )
            self._last_refill = now
            if self._tokens < 1.0:
                self.quota_rejections += 1
                raise TenantQuotaExceeded(
                    "tenant operation quota is exhausted"
                )
            self._tokens -= 1.0
            self._active_operations += 1

    def end_operation(self) -> None:
        with self._condition:
            if self._active_operations > 0:
                self._active_operations -= 1
            self._condition.notify_all()

    def begin_maintenance(self, timeout: float) -> None:
        deadline = self._monotonic() + max(0.0, timeout)
        with self._condition:
            self._maintenance = True
            while self._active_operations:
                remaining = deadline - self._monotonic()
                if remaining <= 0:
                    self._maintenance = False
                    self._condition.notify_all()
                    raise TenantUnavailable(
                        "tenant maintenance drain timed out"
                    )
                self._condition.wait(remaining)

    def end_maintenance(self) -> None:
        with self._condition:
            self._maintenance = False
            self._condition.notify_all()


def _encode_value(value: Any) -> Dict[str, Any]:
    if isinstance(value, bytes):
        return {
            "encoding": "base64",
            "value": base64.b64encode(value).decode("ascii"),
        }
    return {"encoding": "json", "value": value}


def _decode_value(value: Any) -> Any:
    if not isinstance(value, Mapping) or set(value) != {"encoding", "value"}:
        raise ArtifactError("artifact entry contains an invalid value")
    if value["encoding"] == "json":
        _canonical(value["value"])
        return value["value"]
    if value["encoding"] == "base64" and isinstance(value["value"], str):
        try:
            return base64.b64decode(value["value"], validate=True)
        except (ValueError, binascii.Error) as exc:
            raise ArtifactError("artifact entry contains invalid base64") from exc
    raise ArtifactError("artifact entry contains an unsupported encoding")


def _entries_document(
    tenant_namespace: str,
    entries: Iterable[StorageEntry],
    *,
    purpose: str,
    created_at: float,
) -> Dict[str, Any]:
    values = []
    for entry in entries:
        if not isinstance(entry, StorageEntry):
            raise ArtifactError("storage export returned an invalid entry")
        values.append(
            {
                "key": entry.key,
                "value": _encode_value(entry.value),
                "fresh_for_seconds": entry.fresh_for_seconds,
                "stale_for_seconds": entry.stale_for_seconds,
                "tags": list(entry.tags),
                "persistent": bool(entry.persistent),
            }
        )
    values.sort(key=lambda item: item["key"])
    return {
        "version": 1,
        "purpose": purpose,
        "tenant_namespace": tenant_namespace,
        "created_at": created_at,
        "entries": values,
    }


def _document_entries(
    document: Mapping[str, Any],
    tenant_namespace: str,
    purpose: str,
    *,
    now: Optional[float] = None,
) -> Tuple[StorageEntry, ...]:
    if (
        document.get("version") != 1
        or document.get("purpose") != purpose
        or document.get("tenant_namespace") != tenant_namespace
        or not isinstance(document.get("entries"), list)
    ):
        raise ArtifactError("artifact payload identity is invalid")
    created_at = _number(
        document.get("created_at"), "artifact created_at", minimum=0
    )
    age = max(0.0, (time.time() if now is None else now) - created_at)
    entries = []
    seen = set()
    for raw in document["entries"]:
        if not isinstance(raw, Mapping):
            raise ArtifactError("artifact contains an invalid entry")
        if set(raw) != {
            "key",
            "value",
            "fresh_for_seconds",
            "stale_for_seconds",
            "tags",
            "persistent",
        }:
            raise ArtifactError("artifact entry has unsupported fields")
        key = raw["key"]
        tags = raw["tags"]
        if (
            not isinstance(key, str)
            or not key
            or len(key) > 1024
            or key in seen
            or not isinstance(tags, list)
            or any(
                not isinstance(tag, str)
                or not tag
                or len(tag.encode("utf-8")) > 1024
                for tag in tags
            )
            or len(tags) != len(set(tags))
            or not isinstance(raw["persistent"], bool)
        ):
            raise ArtifactError("artifact entry validation failed")
        seen.add(key)
        persistent = raw["persistent"]
        fresh = raw["fresh_for_seconds"]
        stale = raw["stale_for_seconds"]
        if persistent:
            if fresh is not None or stale is not None:
                raise ArtifactError(
                    "persistent artifact entries cannot contain deadlines"
                )
        else:
            if (
                isinstance(fresh, bool)
                or not isinstance(fresh, (int, float))
                or not math.isfinite(fresh)
                or fresh <= 0
                or isinstance(stale, bool)
                or not isinstance(stale, (int, float))
                or not math.isfinite(stale)
                or stale < 0
            ):
                raise ArtifactError(
                    "artifact entry contains invalid freshness windows"
                )
            total_remaining = float(fresh) + float(stale) - age
            if total_remaining <= 0:
                continue
            fresh_remaining = max(0.0, float(fresh) - age)
            stale_remaining = max(0.0, total_remaining - fresh_remaining)
        entries.append(
            StorageEntry(
                key=key,
                value=_decode_value(raw["value"]),
                fresh_for_seconds=None if persistent else fresh_remaining,
                stale_for_seconds=None if persistent else stale_remaining,
                tags=tuple(tags),
                persistent=persistent,
            )
        )
    return tuple(entries)


class ManagedControlPlane:
    """Routes authenticated tenants to isolated in-process data planes."""

    def __init__(
        self,
        definition: ControlPlaneDefinition,
        engines: Mapping[str, StorageBackend],
        *,
        state_directory: str,
        key_provider: KeyProvider,
        state_max_bytes: int = 16_777_216,
        max_usage_periods: int = 744,
        max_operations: int = 1000,
        audit_segment_bytes: int = 1_048_576,
        audit_max_segments: int = 32,
        artifact_max_bytes: int = 67_108_864,
        scheduler_interval_seconds: float = 30.0,
        clock: Callable[[], float] = time.time,
        monotonic: Callable[[], float] = time.monotonic,
        start_workers: bool = True,
        tenant_state_paths: Optional[Mapping[str, Iterable[str]]] = None,
    ) -> None:
        if len(definition.tenant_ids) != len(set(definition.tenant_ids)):
            raise ControlPlaneError("tenant ids must be unique")
        if definition.default_tenant not in definition.tenant_ids:
            raise ControlPlaneError("default tenant is not configured")
        if set(engines) != set(definition.tenant_ids):
            raise ControlPlaneError(
                "tenant engine mapping must exactly match configured tenants"
            )
        webhook_owners: Dict[str, str] = {}
        for tenant in definition.tenants:
            for source in tenant.webhooks:
                previous = webhook_owners.setdefault(source, tenant.tenant_id)
                if previous != tenant.tenant_id:
                    raise ControlPlaneError(
                        "webhook '{}' is assigned to multiple tenants".format(
                            source
                        )
                    )
        self.definition = definition
        self.default_tenant = definition.default_tenant
        self.key_provider = key_provider
        self.max_usage_periods = _integer(
            max_usage_periods, "max_usage_periods", minimum=1
        )
        self.max_operations = _integer(
            max_operations, "max_operations", minimum=1
        )
        self.scheduler_interval_seconds = _number(
            scheduler_interval_seconds,
            "scheduler_interval_seconds",
            minimum=0.1,
        )
        self._clock = clock
        self._monotonic = monotonic
        self._lock = threading.RLock()
        self._billing_lock = threading.Lock()
        self._state_store = AtomicControlState(
            state_directory, state_max_bytes
        )
        try:
            self._audit = TamperEvidentAuditLog(
                self._state_store.directory,
                key_provider,
                segment_bytes=audit_segment_bytes,
                max_segments=audit_max_segments,
                clock=clock,
            )
            self._artifacts = EncryptedArtifactStore(
                self._state_store.directory,
                key_provider,
                artifact_max_bytes,
            )
        except Exception:
            self._state_store.close()
            raise
        self._tenant_state_paths = {
            tenant_id: tuple(
                os.path.realpath(os.path.abspath(path)) for path in paths
            )
            for tenant_id, paths in (tenant_state_paths or {}).items()
        }
        self._runtimes: Dict[str, _TenantRuntime] = {}
        for tenant in definition.tenants:
            namespace = tenant_namespace_id(key_provider, tenant.tenant_id)
            self._runtimes[tenant.tenant_id] = _TenantRuntime(
                tenant,
                engines[tenant.tenant_id],
                namespace,
                monotonic,
            )
        self._persistence_errors = 0
        self._last_persistence_error: Optional[str] = None
        self._request_counts: Dict[Tuple[str, str, str], int] = defaultdict(int)
        self._latency_counts: Dict[Tuple[str, str, float], int] = defaultdict(int)
        self._latency_sums: Dict[Tuple[str, str], float] = defaultdict(float)
        self._latency_buckets = (
            0.001,
            0.005,
            0.01,
            0.025,
            0.05,
            0.1,
            0.25,
            0.5,
            1.0,
            2.5,
            5.0,
        )
        self._closed = threading.Event()
        self._operations: "queue.Queue[Optional[str]]" = queue.Queue(
            maxsize=max_operations
        )
        try:
            self._state = self._load_or_initialize_state()
            self._reconcile_audit_checkpoint()
            self._reconcile_artifacts()
        except Exception:
            self._state_store.close()
            raise
        for tenant_id, runtime in self._runtimes.items():
            status = self._state["tenants"][tenant_id]["status"]
            runtime.set_accepting(status == "active")
            if status == "deleted":
                begin = getattr(runtime.engine, "begin_shutdown", None)
                if begin is not None:
                    begin()
                close = getattr(runtime.engine, "close", None)
                if close is not None:
                    close(1.0)
                self._remove_tenant_state_files(tenant_id)
        self._worker: Optional[threading.Thread] = None
        self._scheduler: Optional[threading.Thread] = None
        if start_workers:
            self._worker = threading.Thread(
                target=self._operation_worker,
                name="megacache-control-operations",
                daemon=True,
            )
            self._scheduler = threading.Thread(
                target=self._scheduler_worker,
                name="megacache-control-scheduler",
                daemon=True,
            )
            self._worker.start()
            self._scheduler.start()
            self._requeue_incomplete()

    @property
    def tenant_ids(self) -> Tuple[str, ...]:
        return self.definition.tenant_ids

    def namespace_for(self, tenant_id: str) -> str:
        return self._runtime(tenant_id).namespace_id

    def engine_for(self, principal_or_tenant: Any) -> StorageBackend:
        tenant_id = (
            principal_or_tenant
            if isinstance(principal_or_tenant, str)
            else getattr(principal_or_tenant, "tenant_id", None)
        )
        runtime = self._runtime(tenant_id)
        status = self._tenant_state(runtime.definition.tenant_id)["status"]
        if status != "active":
            raise TenantUnavailable(
                "tenant data plane is {}".format(status)
            )
        return runtime.engine

    def engine_for_webhook(self, source: str) -> Tuple[str, StorageBackend]:
        normalized = _identifier(source, "webhook source")
        for tenant in self.definition.tenants:
            if normalized in tenant.webhooks:
                status = self._tenant_state(tenant.tenant_id)["status"]
                if status != "active":
                    raise TenantUnavailable(
                        "tenant data plane is {}".format(status)
                    )
                return tenant.tenant_id, self._runtimes[tenant.tenant_id].engine
        raise TenantNotFound("webhook source is not configured")

    def acquire_connection(self, tenant_id: str) -> None:
        runtime = self._runtime(tenant_id)
        runtime.acquire_connection()
        self._record_usage(
            tenant_id,
            protocol="connection",
            operation="open",
            request_bytes=0,
            response_bytes=0,
            success=True,
            connections=1,
        )

    def release_connection(self, tenant_id: str) -> None:
        self._runtime(tenant_id).release_connection()

    def begin_operation(
        self, tenant_id: str, *, control: bool = False
    ) -> None:
        self._runtime(tenant_id).begin_operation(
            allow_unavailable=control
        )

    def end_operation(self, tenant_id: str) -> None:
        self._runtime(tenant_id).end_operation()

    def observe_tenant_request(
        self,
        tenant_id: str,
        protocol: str,
        operation: str,
        request_bytes: int,
        response_bytes: int,
        success: bool,
        duration_seconds: Optional[float] = None,
    ) -> None:
        if duration_seconds is not None:
            self._observe_aggregate(
                protocol, operation, duration_seconds, success
            )
        self._record_usage(
            tenant_id,
            protocol=protocol,
            operation=operation,
            request_bytes=max(0, int(request_bytes)),
            response_bytes=max(0, int(response_bytes)),
            success=bool(success),
            connections=0,
        )

    def resolve_tenant(
        self, principal: Any, requested: Optional[str] = None
    ) -> str:
        own = getattr(principal, "tenant_id", None)
        target = own if requested is None else requested
        if not isinstance(target, str):
            raise TenantNotFound("tenant is not configured")
        roles = frozenset(getattr(principal, "roles", ()))
        if target != own and not roles.intersection(
            ("platform_admin", "operator", "auditor", "billing_admin")
        ):
            raise TenantNotFound("tenant is not configured")
        self._runtime(target)
        return target

    def tenant_status(self, tenant_id: str) -> Dict[str, Any]:
        runtime = self._runtime(tenant_id)
        definition = runtime.definition
        with self._lock:
            state = _json_copy(self._state["tenants"][tenant_id])
            usage = _json_copy(
                self._state["usage"].get(
                    tenant_id, {"periods": {}, "cumulative": {}}
                )
            )
            operations = [
                self._public_operation(item)
                for item in self._state["operations"]
                if item["tenant_id"] == tenant_id
            ][-20:]
            pending_operations = sum(
                item["tenant_id"] == tenant_id
                and item["status"] in ("pending", "running")
                for item in self._state["operations"]
            )
        if state["status"] == "deleted":
            stats = {"entries": 0, "memory_bytes": 0}
            data_status = {
                "healthy_nodes": 0,
                "total_nodes": 0,
                "degraded": False,
                "known_keys": 0,
                "status": "deleted",
            }
            origins = {}
        else:
            stats = runtime.engine.stats()
            data_status_method = getattr(runtime.engine, "status", None)
            data_status = (
                {
                    "healthy_nodes": 1,
                    "total_nodes": 1,
                    "degraded": False,
                    "known_keys": runtime.engine.size(),
                }
                if data_status_method is None
                else data_status_method()
            )
            origins = self._origin_status(runtime.engine)
        return {
            "version": CONTROL_PLANE_VERSION,
            "tenant_id": tenant_id,
            "tenant_namespace": runtime.namespace_id,
            "display_name": definition.display_name,
            "status": state["status"],
            "data_plane_boundary": "single_process",
            "external_orchestrator_required": True,
            "quotas": {
                "max_entries": definition.quotas.max_entries,
                "max_bytes": definition.quotas.max_bytes,
                "max_entry_bytes": definition.quotas.max_entry_bytes,
                "ops_per_second": definition.quotas.ops_per_second,
                "burst_ops": definition.quotas.burst_ops,
                "max_connections": definition.quotas.max_connections,
                "origin_concurrency": definition.quotas.origin_concurrency,
            },
            "quota_state": {
                "connections": runtime.connections,
                "active_operations": runtime.active_operations,
                "operation_rejections_total": runtime.quota_rejections,
                "connection_rejections_total": runtime.connection_rejections,
            },
            "data_plane": data_status,
            "storage": {
                "entries": int(stats.get("entries", 0)),
                "memory_bytes": int(stats.get("memory_bytes", 0)),
                "memory_limit_bytes": definition.quotas.max_bytes,
            },
            "origins": origins,
            "usage": usage,
            "placement": {
                "regions": list(definition.regions),
                "primary_region": definition.primary_region,
                "desired": state["desired_deployment"],
                "observed": list(state["observed_deployments"].values()),
            },
            "backups": state["backups"],
            "exports": state["exports"],
            "disaster_recovery": {
                "rpo_seconds": definition.disaster_recovery.rpo_seconds,
                "rto_seconds": definition.disaster_recovery.rto_seconds,
                "recovery_regions": list(
                    definition.disaster_recovery.recovery_regions
                ),
                "drills": state["drills"],
            },
            "operations": operations,
            "alerts": self._alerts(tenant_id, state, stats),
            "metering": {
                "durable": self._last_persistence_error is None,
                "persistence_errors_total": self._persistence_errors,
                "last_error": self._last_persistence_error,
            },
            "control_plane": {
                "state_version": CONTROL_STATE_VERSION,
                "audit_verified": True,
                "operation_queue_depth": pending_operations,
            },
        }

    def list_tenants(self) -> Dict[str, Any]:
        return {
            "version": CONTROL_PLANE_VERSION,
            "data_plane_boundary": "single_process",
            "audit": self._audit.status(),
            "tenants": [
                {
                    "tenant_id": tenant.tenant_id,
                    "tenant_namespace": self.namespace_for(tenant.tenant_id),
                    "display_name": tenant.display_name,
                    "status": self._tenant_state(tenant.tenant_id)["status"],
                    "primary_region": tenant.primary_region,
                }
                for tenant in self.definition.tenants
            ],
        }

    def orchestrator_status(self) -> Dict[str, Any]:
        with self._lock:
            tenants = []
            for definition in self.definition.tenants:
                state = self._state["tenants"][definition.tenant_id]
                tenants.append(
                    {
                        "tenant_id": definition.tenant_id,
                        "tenant_namespace": self.namespace_for(
                            definition.tenant_id
                        ),
                        "status": state["status"],
                        "allowed_regions": list(definition.regions),
                        "desired": _json_copy(state["desired_deployment"]),
                        "observed": list(
                            _json_copy(state["observed_deployments"]).values()
                        ),
                    }
                )
        return {
            "version": 1,
            "boundary": "metadata_only_external_orchestrator",
            "tenants": tenants,
        }

    def set_desired_deployment(
        self,
        tenant_id: str,
        value: Mapping[str, Any],
        *,
        actor: str,
    ) -> Dict[str, Any]:
        runtime = self._runtime(tenant_id)
        if not isinstance(value, Mapping):
            raise ControlPlaneError("deployment must be an object")
        _keys_only(
            value,
            ("version", "regions", "replicas", "max_unavailable", "drain"),
            "deployment",
        )
        current = self._tenant_state(tenant_id)["desired_deployment"]
        version = _bounded_text(
            value.get("version", current["version"]),
            "deployment version",
            128,
        )
        raw_regions = value.get("regions", current["regions"])
        if not isinstance(raw_regions, list) or not raw_regions:
            raise ControlPlaneError("deployment regions must be a non-empty array")
        regions = tuple(_identifier(item, "deployment region") for item in raw_regions)
        if len(regions) != len(set(regions)) or any(
            region not in runtime.definition.regions for region in regions
        ):
            raise ControlPlaneError(
                "deployment regions must be unique allowed regions"
            )
        replicas = _integer(
            value.get("replicas", current["replicas"]),
            "deployment replicas",
            minimum=1,
            maximum=10_000,
        )
        max_unavailable = _integer(
            value.get("max_unavailable", current["max_unavailable"]),
            "deployment max_unavailable",
            minimum=0,
            maximum=replicas,
        )
        drain = value.get("drain", current["drain"])
        if not isinstance(drain, bool):
            raise ControlPlaneError("deployment drain must be a boolean")
        now = self._clock()

        def mutation(state: MutableMapping[str, Any]) -> None:
            desired = state["tenants"][tenant_id]["desired_deployment"]
            state["tenants"][tenant_id]["desired_deployment"] = {
                "generation": int(desired["generation"]) + 1,
                "version": version,
                "regions": list(regions),
                "replicas": replicas,
                "strategy": "rolling",
                "max_unavailable": max_unavailable,
                "drain": drain,
                "updated_at": now,
            }

        self._audit_action(
            actor,
            "deployment_change",
            tenant_id,
            "requested",
            {
                "version": version,
                "regions": list(regions),
                "replicas": replicas,
                "max_unavailable": max_unavailable,
                "drain": drain,
            },
        )
        self._mutate_state(mutation)
        return _json_copy(
            self._tenant_state(tenant_id)["desired_deployment"]
        )

    def report_observed_deployment(
        self,
        tenant_id: str,
        value: Mapping[str, Any],
        *,
        actor: str,
    ) -> Dict[str, Any]:
        runtime = self._runtime(tenant_id)
        if not isinstance(value, Mapping):
            raise ControlPlaneError("deployment observation must be an object")
        _keys_only(
            value,
            (
                "instance_id",
                "region",
                "version",
                "health",
                "draining",
                "observed_generation",
            ),
            "deployment observation",
        )
        instance_id = _identifier(value.get("instance_id"), "instance_id")
        region = _identifier(value.get("region"), "region")
        if region not in runtime.definition.regions:
            raise ControlPlaneError("observed region is not allowed for tenant")
        version = _bounded_text(value.get("version"), "observed version", 128)
        health = value.get("health")
        if health not in ("healthy", "degraded", "unavailable"):
            raise ControlPlaneError(
                "observed health must be healthy, degraded, or unavailable"
            )
        draining = value.get("draining", False)
        if not isinstance(draining, bool):
            raise ControlPlaneError("observed draining must be a boolean")
        generation = _integer(
            value.get("observed_generation", 0),
            "observed_generation",
            minimum=0,
        )
        observation = {
            "instance_id": instance_id,
            "region": region,
            "version": version,
            "health": health,
            "draining": draining,
            "observed_generation": generation,
            "observed_at": self._clock(),
        }

        def mutation(state: MutableMapping[str, Any]) -> None:
            observed = state["tenants"][tenant_id]["observed_deployments"]
            if instance_id not in observed and len(observed) >= 256:
                oldest = min(
                    observed,
                    key=lambda key: float(observed[key]["observed_at"]),
                )
                del observed[oldest]
            observed[instance_id] = observation

        self._mutate_state(mutation)
        self._audit_action(
            actor,
            "deployment_observed",
            tenant_id,
            "recorded",
            {
                "instance_id": instance_id,
                "region": region,
                "version": version,
                "health": health,
                "draining": draining,
                "observed_generation": generation,
            },
        )
        return _json_copy(observation)

    def enqueue_operation(
        self,
        kind: str,
        tenant_id: str,
        *,
        actor: str,
        parameters: Optional[Mapping[str, Any]] = None,
        idempotency_key: Optional[str] = None,
    ) -> Dict[str, Any]:
        if kind not in (
            "backup",
            "data_export",
            "restore_validate",
            "restore",
            "dr_drill",
            "tenant_delete",
            "audit_export",
        ):
            raise ControlPlaneError("unsupported control-plane operation")
        runtime = self._runtime(tenant_id)
        status = self._tenant_state(tenant_id)["status"]
        if status != "active" and kind not in (
            "tenant_delete",
            "audit_export",
        ):
            raise TenantUnavailable("tenant data plane is {}".format(status))
        params = _json_copy(dict(parameters or {}))
        if len(_canonical(params)) > 16_384:
            raise ControlPlaneCapacity("operation parameters exceed byte limit")
        actor_value = _bounded_text(actor, "operation actor", 256)
        normalized_idempotency = None
        if idempotency_key is not None:
            normalized_idempotency = _bounded_text(
                idempotency_key, "idempotency_key", 256
            )
        with self._lock:
            if normalized_idempotency is not None:
                for existing in self._state["operations"]:
                    if (
                        existing.get("tenant_id") == tenant_id
                        and existing.get("kind") == kind
                        and existing.get("idempotency_key")
                        == normalized_idempotency
                    ):
                        return self._public_operation(existing)
            self._ensure_operation_capacity_locked(tenant_id)
            operation_id = secrets.token_hex(16)
            operation = {
                "operation_id": operation_id,
                "kind": kind,
                "tenant_id": tenant_id,
                "status": "pending",
                "actor": actor_value,
                "idempotency_key": normalized_idempotency,
                "parameters": params,
                "created_at": self._clock(),
                "started_at": None,
                "completed_at": None,
                "result": None,
                "error": None,
            }
            self._audit_action(
                actor_value,
                "operation_requested",
                tenant_id,
                "accepted",
                {"kind": kind, "operation_id": operation_id},
            )

            def mutation(state: MutableMapping[str, Any]) -> None:
                state["operations"].append(operation)

            self._mutate_state_locked(mutation)
        try:
            self._operations.put_nowait(operation_id)
        except queue.Full as exc:
            self._update_operation(
                operation_id,
                status="failed",
                error="operation queue capacity is exhausted",
            )
            raise ControlPlaneCapacity(
                "operation queue capacity is exhausted"
            ) from exc
        return self._public_operation(operation)

    def request_backup(
        self,
        tenant_id: str,
        *,
        actor: str,
        reason: str = "manual",
        idempotency_key: Optional[str] = None,
    ) -> Dict[str, Any]:
        return self.enqueue_operation(
            "backup",
            tenant_id,
            actor=actor,
            parameters={"reason": _bounded_text(reason, "backup reason", 128)},
            idempotency_key=idempotency_key,
        )

    def request_data_export(
        self,
        tenant_id: str,
        *,
        actor: str,
        idempotency_key: Optional[str] = None,
    ) -> Dict[str, Any]:
        return self.enqueue_operation(
            "data_export",
            tenant_id,
            actor=actor,
            idempotency_key=idempotency_key,
        )

    def request_restore_validation(
        self,
        tenant_id: str,
        backup_id: str,
        *,
        actor: str,
        idempotency_key: Optional[str] = None,
    ) -> Dict[str, Any]:
        return self.enqueue_operation(
            "restore_validate",
            tenant_id,
            actor=actor,
            parameters={
                "backup_id": _identifier(backup_id, "backup_id")
            },
            idempotency_key=idempotency_key,
        )

    def request_restore(
        self,
        tenant_id: str,
        backup_id: str,
        validation_token: str,
        confirmation: str,
        *,
        actor: str,
        idempotency_key: Optional[str] = None,
    ) -> Dict[str, Any]:
        if confirmation != tenant_id:
            raise ControlPlaneError(
                "restore confirmation must exactly match tenant id"
            )
        _bounded_text(validation_token, "validation_token", 4096)
        return self.enqueue_operation(
            "restore",
            tenant_id,
            actor=actor,
            parameters={
                "backup_id": _identifier(backup_id, "backup_id"),
                "validation_token": validation_token,
                "confirmed_tenant_id": confirmation,
            },
            idempotency_key=idempotency_key,
        )

    def request_drill(
        self,
        tenant_id: str,
        backup_id: str,
        *,
        actor: str,
        idempotency_key: Optional[str] = None,
    ) -> Dict[str, Any]:
        return self.enqueue_operation(
            "dr_drill",
            tenant_id,
            actor=actor,
            parameters={
                "backup_id": _identifier(backup_id, "backup_id")
            },
            idempotency_key=idempotency_key,
        )

    def request_audit_export(
        self,
        tenant_id: str,
        *,
        actor: str,
        after_sequence: int = 0,
        limit: int = 1000,
        idempotency_key: Optional[str] = None,
    ) -> Dict[str, Any]:
        return self.enqueue_operation(
            "audit_export",
            tenant_id,
            actor=actor,
            parameters={
                "after_sequence": _integer(
                    after_sequence, "after_sequence", minimum=0
                ),
                "limit": _integer(
                    limit, "audit limit", minimum=1, maximum=1000
                ),
            },
            idempotency_key=idempotency_key,
        )

    def run_pending_operations(self, limit: int = 100) -> int:
        """Synchronously execute pending work when background workers are disabled."""
        bounded = _integer(limit, "operation limit", minimum=1, maximum=1000)
        completed = 0
        while completed < bounded:
            try:
                operation_id = self._operations.get_nowait()
            except queue.Empty:
                break
            if operation_id is None:
                self._operations.task_done()
                break
            try:
                self._run_operation(operation_id)
            finally:
                self._operations.task_done()
            completed += 1
        return completed

    def operation_status(
        self, operation_id: str, tenant_id: Optional[str] = None
    ) -> Dict[str, Any]:
        normalized = _identifier(operation_id, "operation id")
        with self._lock:
            for operation in self._state["operations"]:
                if operation["operation_id"] != normalized:
                    continue
                if tenant_id is not None and operation["tenant_id"] != tenant_id:
                    break
                return self._public_operation(operation)
        raise TenantNotFound("operation is not available")

    def wait_operation(
        self, operation_id: str, timeout: float = 10.0
    ) -> Dict[str, Any]:
        deadline = self._monotonic() + max(0.0, timeout)
        while True:
            value = self.operation_status(operation_id)
            if value["status"] in ("succeeded", "failed"):
                return value
            if self._monotonic() >= deadline:
                return value
            time.sleep(0.01)

    def create_deletion_challenge(
        self, tenant_id: str, *, actor: str, lifetime_seconds: int = 300
    ) -> Dict[str, Any]:
        self._runtime(tenant_id)
        if self._tenant_state(tenant_id)["status"] != "active":
            raise TenantUnavailable("tenant is not active")
        lifetime = _integer(
            lifetime_seconds,
            "deletion challenge lifetime",
            minimum=30,
            maximum=3600,
        )
        expires_at = int(self._clock()) + lifetime
        nonce = secrets.token_hex(16)
        namespace = self.namespace_for(tenant_id)
        payload = {
            "version": 1,
            "tenant_namespace": namespace,
            "expires_at": expires_at,
            "nonce": nonce,
        }
        key_id = self.key_provider.active_key_id()
        signature = hmac.new(
            _derived_key(self.key_provider, key_id, b"tenant-deletion"),
            _canonical(payload),
            hashlib.sha256,
        ).hexdigest()
        token_document = dict(payload)
        token_document["key_id"] = key_id
        token_document["signature"] = signature
        token = base64.urlsafe_b64encode(_canonical(token_document)).decode(
            "ascii"
        ).rstrip("=")
        self._audit_action(
            actor,
            "tenant_deletion_challenge",
            tenant_id,
            "issued",
            {"expires_at": expires_at},
        )
        return {
            "tenant_id": tenant_id,
            "challenge": token,
            "expires_at": expires_at,
            "confirmation": tenant_id,
            "warning": "tenant deletion is irreversible",
        }

    def request_tenant_deletion(
        self,
        tenant_id: str,
        *,
        actor: str,
        challenge: str,
        confirmation: str,
        idempotency_key: Optional[str] = None,
    ) -> Dict[str, Any]:
        if confirmation != tenant_id:
            raise ControlPlaneError(
                "confirmation must exactly match the tenant id"
            )
        self._verify_deletion_challenge(tenant_id, challenge)
        runtime = self._runtime(tenant_id)
        with self._lock:
            if self._state["tenants"][tenant_id]["status"] != "active":
                raise TenantUnavailable("tenant is not active")

            def mark_pending(state: MutableMapping[str, Any]) -> None:
                state["tenants"][tenant_id]["status"] = "deletion_pending"

            self._mutate_state_locked(mark_pending)
            runtime.set_accepting(False)
        try:
            return self.enqueue_operation(
                "tenant_delete",
                tenant_id,
                actor=actor,
                parameters={"confirmed_tenant_id": tenant_id},
                idempotency_key=idempotency_key,
            )
        except Exception:
            with self._lock:
                def restore(state: MutableMapping[str, Any]) -> None:
                    state["tenants"][tenant_id]["status"] = "active"

                self._mutate_state_locked(restore)
                runtime.set_accepting(True)
            raise

    def export_audit(
        self,
        *,
        tenant_id: Optional[str] = None,
        after_sequence: int = 0,
        limit: int = 1000,
    ) -> Dict[str, Any]:
        namespace = (
            None if tenant_id is None else self.namespace_for(tenant_id)
        )
        return self._audit.export(
            tenant_namespace=namespace,
            after_sequence=after_sequence,
            limit=limit,
        )

    def prune_audit(
        self,
        *,
        through_sequence: int,
        expected_hash: str,
        actor: str,
    ) -> Dict[str, Any]:
        result = self._audit.prune_exported(
            through_sequence, expected_hash
        )
        self._audit_action(
            actor,
            "audit_prune",
            None,
            "completed",
            {
                "through_sequence": through_sequence,
                "removed_segments": result["removed_segments"],
            },
        )
        return result

    def billing_export(
        self,
        *,
        tenant_id: Optional[str] = None,
        start_period: Optional[str] = None,
        end_period: Optional[str] = None,
        provider: Optional[BillingProvider] = None,
        actor: str,
    ) -> Dict[str, Any]:
        start_period = _usage_period(start_period, "start_period")
        end_period = _usage_period(end_period, "end_period")
        if (
            start_period is not None
            and end_period is not None
            and start_period > end_period
        ):
            raise ControlPlaneError("start_period must not follow end_period")
        with self._billing_lock:
            tenants = (
                self.definition.tenant_ids
                if tenant_id is None
                else (self._runtime(tenant_id).definition.tenant_id,)
            )
            records = []
            with self._lock:
                for current in sorted(tenants):
                    usage = self._state["usage"].get(current, {"periods": {}})
                    for period, counters in sorted(usage["periods"].items()):
                        if start_period is not None and period < start_period:
                            continue
                        if end_period is not None and period > end_period:
                            continue
                        records.append(
                            {
                                "tenant_namespace": self.namespace_for(current),
                                "period": period,
                                "usage": _json_copy(counters),
                            }
                        )
            payload = {
                "version": 1,
                "unit": "raw_usage_counters",
                "scope": [
                    self.namespace_for(current) for current in sorted(tenants)
                ],
                "start_period": start_period,
                "end_period": end_period,
                "records": records,
            }
            digest = hashlib.sha256(_canonical(payload)).hexdigest()
            batch_id = "usage-" + digest[:32]
            with self._lock:
                existing = next(
                    (
                        item
                        for item in self._state["billing_exports"]
                        if item["batch_id"] == batch_id
                    ),
                    None,
                )
            if existing is not None and (
                provider is None or existing["status"] == "delivered"
            ):
                result = _json_copy(existing)
                result["idempotent_replay"] = True
                result["payload"] = payload
                return result

            receipt = None
            status = "prepared"
            if provider is not None:
                supplied = provider.export_usage(batch_id, payload)
                receipt = _json_copy(dict(supplied))
                if len(_canonical(receipt)) > 8192:
                    raise ControlPlaneCapacity(
                        "billing provider receipt exceeds byte limit"
                    )
                status = "delivered"
            record = {
                "batch_id": batch_id,
                "sha256": digest,
                "status": status,
                "record_count": len(records),
                "created_at": self._clock(),
                "provider_receipt": receipt,
            }

            def mutation(state: MutableMapping[str, Any]) -> None:
                exports = state["billing_exports"]
                exports[:] = [
                    item for item in exports if item["batch_id"] != batch_id
                ]
                exports.append(record)
                del exports[:-1000]

            self._mutate_state(mutation)
            self._audit_action(
                actor,
                "billing_export",
                tenant_id,
                status,
                {"batch_id": batch_id, "record_count": len(records)},
            )
            result = _json_copy(record)
            result["idempotent_replay"] = False
            result["payload"] = payload
            return result

    def run_scheduled_backups(self) -> Tuple[str, ...]:
        queued = []
        now = self._clock()
        for definition in self.definition.tenants:
            if definition.backup.interval_seconds <= 0:
                continue
            state = self._tenant_state(definition.tenant_id)
            if state["status"] != "active":
                continue
            successful = [
                item
                for item in state["backups"]
                if item.get("status") == "succeeded"
            ]
            last = (
                0.0
                if not successful
                else max(float(item["created_at"]) for item in successful)
            )
            if now - last < definition.backup.interval_seconds:
                continue
            with self._lock:
                pending = any(
                    operation["tenant_id"] == definition.tenant_id
                    and operation["kind"] == "backup"
                    and operation["status"] in ("pending", "running")
                    for operation in self._state["operations"]
                )
            if pending:
                continue
            try:
                operation = self.enqueue_operation(
                    "backup",
                    definition.tenant_id,
                    actor="scheduler",
                    parameters={"reason": "schedule"},
                    idempotency_key="scheduled-{}".format(
                        int(now // definition.backup.interval_seconds)
                    ),
                )
            except ControlPlaneError:
                continue
            queued.append(operation["operation_id"])
        return tuple(queued)

    def run_retention(self) -> Dict[str, int]:
        now = self._clock()
        removed_artifacts: List[Tuple[str, str]] = []
        counts = {"backups": 0, "exports": 0, "operations": 0}

        def mutation(state: MutableMapping[str, Any]) -> None:
            for definition in self.definition.tenants:
                tenant = state["tenants"][definition.tenant_id]
                backup_cutoff = now - definition.backup.retention_seconds
                kept_backups = []
                for item in tenant["backups"]:
                    if float(item["created_at"]) < backup_cutoff:
                        counts["backups"] += 1
                        removed_artifacts.append(
                            ("backup", str(item["artifact_id"]))
                        )
                    else:
                        kept_backups.append(item)
                tenant["backups"] = kept_backups[
                    -definition.backup.retention_count :
                ]
                kept_exports = []
                for item in tenant["exports"]:
                    expires_at = float(
                        item.get(
                            "expires_at",
                            float(item["created_at"])
                            + definition.backup.retention_seconds,
                        )
                    )
                    if expires_at < now:
                        counts["exports"] += 1
                        removed_artifacts.append(
                            (str(item["kind"]), str(item["artifact_id"]))
                        )
                    else:
                        kept_exports.append(item)
                tenant["exports"] = kept_exports[
                    -definition.retention.export_count :
                ]

            retained = []
            completed_by_tenant: Dict[str, int] = {}
            for operation in reversed(state["operations"]):
                tenant_id = operation["tenant_id"]
                if operation["status"] not in ("succeeded", "failed"):
                    retained.append(operation)
                    continue
                count = completed_by_tenant.get(tenant_id, 0)
                limit = self._runtime(
                    tenant_id
                ).definition.retention.completed_operations
                if count < limit:
                    retained.append(operation)
                    completed_by_tenant[tenant_id] = count + 1
                else:
                    counts["operations"] += 1
            state["operations"] = list(reversed(retained))

        self._mutate_state(mutation)
        for kind, artifact_id in removed_artifacts:
            self._artifacts.delete(kind, artifact_id)
        return counts

    def status(self) -> Dict[str, Any]:
        data_statuses = []
        for runtime in self._runtimes.values():
            if self._tenant_state(runtime.definition.tenant_id)["status"] == "deleted":
                continue
            method = getattr(runtime.engine, "status", None)
            data_statuses.append(
                (
                    {
                        "healthy_nodes": 1,
                        "total_nodes": 1,
                        "degraded": False,
                    }
                    if method is None
                    else method()
                )
            )
        return {
            "healthy_nodes": sum(
                int(value.get("healthy_nodes", 0)) for value in data_statuses
            ),
            "total_nodes": sum(
                int(value.get("total_nodes", 0)) for value in data_statuses
            ),
            "degraded": any(bool(value.get("degraded")) for value in data_statuses),
            "control_plane_degraded": self._last_persistence_error is not None,
            "tenants": len(self._runtimes),
            "active_tenants": sum(
                self._tenant_state(tenant_id)["status"] == "active"
                for tenant_id in self._runtimes
            ),
            "data_plane_boundary": "single_process",
        }

    def stats(self) -> Dict[str, int]:
        totals: Dict[str, int] = {}
        for tenant_id, runtime in self._runtimes.items():
            if self._tenant_state(tenant_id)["status"] == "deleted":
                continue
            for name, value in runtime.engine.stats().items():
                if isinstance(value, bool):
                    numeric = int(value)
                elif isinstance(value, int):
                    numeric = value
                else:
                    continue
                totals[name] = totals.get(name, 0) + numeric
        totals["control_plane_tenants"] = len(self._runtimes)
        totals["control_plane_active_tenants"] = sum(
            self._tenant_state(tenant_id)["status"] == "active"
            for tenant_id in self._runtimes
        )
        totals["control_plane_meter_persistence_errors_total"] = (
            self._persistence_errors
        )
        totals["control_plane_quota_rejections_total"] = sum(
            runtime.quota_rejections for runtime in self._runtimes.values()
        )
        totals["control_plane_connection_rejections_total"] = sum(
            runtime.connection_rejections for runtime in self._runtimes.values()
        )
        totals["control_plane_connections"] = sum(
            runtime.connections for runtime in self._runtimes.values()
        )
        totals["control_plane_audit_records"] = self._audit.sequence
        return totals

    def prometheus_metrics(self) -> str:
        gauges = {
            "entries",
            "memory_bytes",
            "memory_limit_bytes",
            "active_leases",
            "control_plane_tenants",
            "control_plane_active_tenants",
            "control_plane_connections",
            "control_plane_audit_records",
        }
        lines = []
        for name, value in sorted(self.stats().items()):
            metric = "megacache_{}".format(name)
            lines.extend(
                (
                    "# HELP {} MegaCache aggregate metric without tenant labels.".format(
                        metric
                    ),
                    "# TYPE {} {}".format(
                        metric, "gauge" if name in gauges else "counter"
                    ),
                    "{} {}".format(metric, value),
                )
            )
        lines.extend(
            (
                "# HELP megacache_requests_total Requests aggregated across tenants.",
                "# TYPE megacache_requests_total counter",
            )
        )
        with self._lock:
            for labels, value in sorted(self._request_counts.items()):
                protocol, operation, status = labels
                lines.append(
                    'megacache_requests_total{{protocol="{}",operation="{}",status="{}"}} {}'.format(
                        self._prometheus_label(protocol),
                        self._prometheus_label(operation),
                        self._prometheus_label(status),
                        value,
                    )
                )
            lines.extend(
                (
                    "# HELP megacache_request_duration_seconds Request latency aggregated across tenants.",
                    "# TYPE megacache_request_duration_seconds histogram",
                )
            )
            for protocol, operation in sorted(self._latency_sums):
                label = 'protocol="{}",operation="{}"'.format(
                    self._prometheus_label(protocol),
                    self._prometheus_label(operation),
                )
                for bucket in self._latency_buckets:
                    lines.append(
                        'megacache_request_duration_seconds_bucket{{{},le="{}"}} {}'.format(
                            label,
                            bucket,
                            self._latency_counts[
                                (protocol, operation, bucket)
                            ],
                        )
                    )
                count = sum(
                    value
                    for (current_protocol, current_operation, status), value
                    in self._request_counts.items()
                    if current_protocol == protocol
                    and current_operation == operation
                )
                lines.append(
                    'megacache_request_duration_seconds_bucket{{{},le="+Inf"}} {}'.format(
                        label, count
                    )
                )
                lines.append(
                    "megacache_request_duration_seconds_sum{{{}}} {}".format(
                        label, self._latency_sums[(protocol, operation)]
                    )
                )
                lines.append(
                    "megacache_request_duration_seconds_count{{{}}} {}".format(
                        label, count
                    )
                )
        return "\n".join(lines) + "\n"

    def observe_request(
        self,
        protocol: str,
        operation: str,
        duration_seconds: float,
        success: bool,
    ) -> None:
        self._observe_aggregate(
            protocol, operation, duration_seconds, success
        )

    def heartbeat(self, node_id: str) -> None:
        for runtime in self._runtimes.values():
            heartbeat = getattr(runtime.engine, "heartbeat", None)
            if heartbeat is not None:
                heartbeat(node_id)

    def begin_shutdown(self) -> None:
        if self._closed.is_set():
            return
        self._closed.set()
        for runtime in self._runtimes.values():
            begin = getattr(runtime.engine, "begin_shutdown", None)
            if begin is not None:
                begin()
        try:
            self._operations.put_nowait(None)
        except queue.Full:
            pass

    def close(self, timeout: float = 10.0) -> None:
        self.begin_shutdown()
        deadline = self._monotonic() + max(0.0, timeout)
        for thread in (self._scheduler, self._worker):
            if thread is not None:
                thread.join(max(0.0, deadline - self._monotonic()))
        for runtime in self._runtimes.values():
            close = getattr(runtime.engine, "close", None)
            if close is not None:
                close(max(0.0, deadline - self._monotonic()))
        self._state_store.close()

    def _runtime(self, tenant_id: Any) -> _TenantRuntime:
        if not isinstance(tenant_id, str):
            raise TenantNotFound("tenant is not configured")
        try:
            return self._runtimes[tenant_id]
        except KeyError as exc:
            raise TenantNotFound("tenant is not configured") from exc

    def _tenant_state(self, tenant_id: str) -> Dict[str, Any]:
        with self._lock:
            return _json_copy(self._state["tenants"][tenant_id])

    def _load_or_initialize_state(self) -> Dict[str, Any]:
        loaded = self._state_store.load()
        changed = False
        if loaded is None:
            state = self._empty_state()
            changed = True
        else:
            state = loaded
            if state.get("version") == 1:
                state = self._migrate_v1(state)
                changed = True
        if state.get("version") != CONTROL_STATE_VERSION:
            raise ControlPlaneError("unsupported control state version")
        now = self._clock()
        state.setdefault("created_at", now)
        state.setdefault("meter_sequence", 0)
        state.setdefault("usage", {})
        state.setdefault("operations", [])
        state.setdefault("billing_exports", [])
        state.setdefault("audit_sequence", 0)
        state.setdefault("audit_head", _ZERO_HASH)
        state.setdefault("tenants", {})
        for definition in self.definition.tenants:
            namespace = self.namespace_for(definition.tenant_id)
            existing = state["tenants"].get(definition.tenant_id)
            if existing is None:
                state["tenants"][definition.tenant_id] = self._empty_tenant_state(
                    definition, namespace, now
                )
                changed = True
            else:
                if existing.get("namespace_id") != namespace:
                    raise ControlPlaneError(
                        "tenant namespace key does not match durable state"
                    )
                defaults = self._empty_tenant_state(
                    definition, namespace, now
                )
                for key, value in defaults.items():
                    if key not in existing:
                        existing[key] = value
                        changed = True
                if not definition.enabled and existing.get("status") == "active":
                    existing["status"] = "suspended"
                    changed = True
            if definition.tenant_id not in state["usage"]:
                state["usage"][definition.tenant_id] = {
                    "periods": {},
                    "cumulative": self._empty_usage_counters(),
                }
                changed = True
            else:
                usage = state["usage"][definition.tenant_id]
                if not isinstance(usage, dict):
                    raise ControlPlaneError(
                        "control state contains invalid tenant usage"
                    )
                if "periods" not in usage:
                    usage["periods"] = {}
                    changed = True
                if "cumulative" not in usage:
                    usage["cumulative"] = self._empty_usage_counters()
                    changed = True
                else:
                    for name, count in self._empty_usage_counters().items():
                        if name not in usage["cumulative"]:
                            usage["cumulative"][name] = count
                            changed = True
        configured = set(self.definition.tenant_ids)
        unexpected = set(state["tenants"]) - configured
        if unexpected:
            raise ControlPlaneError(
                "control state contains tenants absent from configuration"
            )
        self._validate_state(state)
        if changed:
            self._state_store.persist(state)
        return state

    def _empty_state(self) -> Dict[str, Any]:
        now = self._clock()
        return {
            "version": CONTROL_STATE_VERSION,
            "created_at": now,
            "meter_sequence": 0,
            "tenants": {
                definition.tenant_id: self._empty_tenant_state(
                    definition,
                    self.namespace_for(definition.tenant_id),
                    now,
                )
                for definition in self.definition.tenants
            },
            "usage": {
                definition.tenant_id: {
                    "periods": {},
                    "cumulative": self._empty_usage_counters(),
                }
                for definition in self.definition.tenants
            },
            "operations": [],
            "billing_exports": [],
            "audit_sequence": 0,
            "audit_head": _ZERO_HASH,
        }

    @staticmethod
    def _empty_tenant_state(
        definition: TenantDefinition, namespace: str, now: float
    ) -> Dict[str, Any]:
        return {
            "namespace_id": namespace,
            "status": "active" if definition.enabled else "suspended",
            "created_at": now,
            "deleted_at": None,
            "desired_deployment": {
                "generation": 1,
                "version": definition.desired_version,
                "regions": [definition.primary_region],
                "replicas": definition.desired_replicas,
                "strategy": "rolling",
                "max_unavailable": definition.max_unavailable,
                "drain": definition.drain,
                "updated_at": now,
            },
            "observed_deployments": {},
            "backups": [],
            "exports": [],
            "drills": [],
            "last_restore": None,
        }

    @staticmethod
    def _empty_usage_counters() -> Dict[str, int]:
        return {
            "operations": 0,
            "errors": 0,
            "request_bytes": 0,
            "response_bytes": 0,
            "origin_operations": 0,
            "connections": 0,
        }

    def _migrate_v1(self, state: Mapping[str, Any]) -> Dict[str, Any]:
        migrated = _json_copy(state)
        migrated["version"] = CONTROL_STATE_VERSION
        migrated.setdefault("meter_sequence", 0)
        migrated.setdefault("usage", {})
        migrated.setdefault("operations", [])
        migrated.setdefault("billing_exports", [])
        migrated.setdefault("audit_sequence", 0)
        migrated.setdefault("audit_head", _ZERO_HASH)
        for tenant in migrated.get("tenants", {}).values():
            if isinstance(tenant, dict):
                tenant.setdefault("observed_deployments", {})
                tenant.setdefault("backups", [])
                tenant.setdefault("exports", [])
                tenant.setdefault("drills", [])
                tenant.setdefault("last_restore", None)
                tenant.setdefault("deleted_at", None)
        return migrated

    def _validate_state(self, state: Mapping[str, Any]) -> None:
        if (
            not isinstance(state, Mapping)
            or state.get("version") != CONTROL_STATE_VERSION
            or not isinstance(state.get("tenants"), Mapping)
            or not isinstance(state.get("usage"), Mapping)
            or not isinstance(state.get("operations"), list)
            or not isinstance(state.get("billing_exports"), list)
            or not isinstance(state.get("audit_sequence"), int)
            or not isinstance(state.get("audit_head"), str)
        ):
            raise ControlPlaneError("control state has an unsupported format")
        if len(state["operations"]) > self.max_operations:
            raise ControlPlaneCapacity(
                "control operations exceed configured limit"
            )
        if len(state["billing_exports"]) > 1000:
            raise ControlPlaneCapacity(
                "billing export receipts exceed configured limit"
            )
        for tenant_id in self.definition.tenant_ids:
            tenant = state["tenants"].get(tenant_id)
            usage = state["usage"].get(tenant_id)
            if (
                not isinstance(tenant, Mapping)
                or tenant.get("status")
                not in ("active", "suspended", "deletion_pending", "deleted")
                or not isinstance(tenant.get("desired_deployment"), Mapping)
                or not isinstance(tenant.get("observed_deployments"), Mapping)
                or not isinstance(tenant.get("backups"), list)
                or not isinstance(tenant.get("exports"), list)
                or not isinstance(tenant.get("drills"), list)
                or not isinstance(usage, Mapping)
                or not isinstance(usage.get("periods"), Mapping)
                or not isinstance(usage.get("cumulative"), Mapping)
            ):
                raise ControlPlaneError(
                    "control state contains invalid tenant metadata"
                )
            retention = self._runtime(tenant_id).definition.retention
            if len(usage["periods"]) > retention.usage_periods:
                raise ControlPlaneCapacity(
                    "tenant usage history exceeds configured limit"
                )
            if len(tenant["backups"]) > self._runtime(
                tenant_id
            ).definition.backup.retention_count:
                raise ControlPlaneCapacity(
                    "tenant backup history exceeds configured limit"
                )
            if len(tenant["exports"]) > retention.export_count:
                raise ControlPlaneCapacity(
                    "tenant export history exceeds configured limit"
                )
            if len(tenant["observed_deployments"]) > 256:
                raise ControlPlaneCapacity(
                    "tenant deployment observations exceed configured limit"
                )
            if len(tenant["drills"]) > 100:
                raise ControlPlaneCapacity(
                    "tenant drill history exceeds configured limit"
                )
            for collection, allowed_kinds in (
                (tenant["backups"], ("backup",)),
                (tenant["exports"], ("export", "audit-export")),
            ):
                for item in collection:
                    if not isinstance(item, Mapping):
                        raise ControlPlaneError(
                            "control state contains invalid artifact metadata"
                        )
                    artifact_id = item.get("artifact_id")
                    kind = item.get("kind")
                    if (
                        not isinstance(artifact_id, str)
                        or _ARTIFACT_ID.fullmatch(artifact_id) is None
                        or kind not in allowed_kinds
                        or item.get("filename")
                        != "{}-{}.mcar".format(kind, artifact_id)
                    ):
                        raise ControlPlaneError(
                            "control state contains unsafe artifact metadata"
                        )
            for period, counters in usage["periods"].items():
                _usage_period(period, "stored usage period")
                if not isinstance(counters, Mapping):
                    raise ControlPlaneError(
                        "control state contains invalid usage counters"
                    )
                for name in self._empty_usage_counters():
                    _integer(
                        counters.get(name, 0),
                        "stored usage {}".format(name),
                        minimum=0,
                    )
            for name in self._empty_usage_counters():
                _integer(
                    usage["cumulative"].get(name, 0),
                    "stored cumulative usage {}".format(name),
                    minimum=0,
                )
        for operation in state["operations"]:
            if (
                not isinstance(operation, Mapping)
                or operation.get("status")
                not in ("pending", "running", "succeeded", "failed")
                or operation.get("tenant_id") not in self.definition.tenant_ids
            ):
                raise ControlPlaneError(
                    "control state contains an invalid operation"
                )
            _identifier(operation.get("operation_id"), "operation id")
        if len(_canonical(state)) > self._state_store.maximum_bytes:
            raise ControlPlaneCapacity(
                "control state exceeds configured byte limit"
            )

    def _mutate_state(
        self, mutation: Callable[[MutableMapping[str, Any]], None]
    ) -> None:
        with self._lock:
            self._mutate_state_locked(mutation)

    def _mutate_state_locked(
        self, mutation: Callable[[MutableMapping[str, Any]], None]
    ) -> None:
        candidate = _json_copy(self._state)
        mutation(candidate)
        self._validate_state(candidate)
        self._state_store.persist(candidate)
        self._state = candidate
        self._last_persistence_error = None

    def _record_usage(
        self,
        tenant_id: str,
        *,
        protocol: str,
        operation: str,
        request_bytes: int,
        response_bytes: int,
        success: bool,
        connections: int,
    ) -> None:
        definition = self._runtime(tenant_id).definition
        period = datetime.datetime.fromtimestamp(
            self._clock(), datetime.timezone.utc
        ).strftime("%Y-%m-%dT%H:00Z")
        origin = operation in ("MC.FETCH", "POST /v1/fetch/{key}")

        def mutation(state: MutableMapping[str, Any]) -> None:
            state["meter_sequence"] = min(
                2**63 - 1, int(state.get("meter_sequence", 0)) + 1
            )
            usage = state["usage"][tenant_id]
            counters = usage["periods"].setdefault(
                period, self._empty_usage_counters()
            )
            cumulative = usage["cumulative"]
            increments = {
                "operations": 0 if protocol == "connection" else 1,
                "errors": 0 if success else 1,
                "request_bytes": request_bytes,
                "response_bytes": response_bytes,
                "origin_operations": int(origin),
                "connections": connections,
            }
            for name, increment in increments.items():
                counters[name] = min(
                    2**63 - 1, int(counters.get(name, 0)) + increment
                )
                cumulative[name] = min(
                    2**63 - 1, int(cumulative.get(name, 0)) + increment
                )
            counters["first_sequence"] = min(
                int(counters.get("first_sequence", state["meter_sequence"])),
                state["meter_sequence"],
            )
            counters["last_sequence"] = state["meter_sequence"]
            periods = usage["periods"]
            while len(periods) > definition.retention.usage_periods:
                del periods[sorted(periods)[0]]

        try:
            self._mutate_state(mutation)
        except Exception as exc:
            self._persistence_errors += 1
            self._last_persistence_error = str(exc)[:1024]
            LOG.error(
                "control-plane metering persistence failed",
                extra={
                    "operation": "usage_meter",
                    "tenant_namespace": self.namespace_for(tenant_id),
                    "reason": str(exc)[:1024],
                },
            )

    def _audit_action(
        self,
        actor: str,
        action: str,
        tenant_id: Optional[str],
        outcome: str,
        details: Optional[Mapping[str, Any]] = None,
    ) -> None:
        namespace = (
            None if tenant_id is None else self.namespace_for(tenant_id)
        )
        record = self._audit.append(
            actor=actor,
            action=action,
            outcome=outcome,
            tenant_namespace=namespace,
            details=details,
        )

        def mutation(state: MutableMapping[str, Any]) -> None:
            state["audit_sequence"] = record["sequence"]
            state["audit_head"] = record["hash"]

        self._mutate_state(mutation)

    def _reconcile_audit_checkpoint(self) -> None:
        with self._lock:
            expected_sequence = int(self._state.get("audit_sequence", 0))
            expected_head = str(self._state.get("audit_head", _ZERO_HASH))
            actual_sequence = self._audit.sequence
            actual_head = self._audit.head_hash
            if actual_sequence < expected_sequence:
                raise ControlPlaneError(
                    "audit history is missing records referenced by state"
                )
            if (
                actual_sequence == expected_sequence
                and not hmac.compare_digest(actual_head, expected_head)
            ):
                raise ControlPlaneError(
                    "audit checkpoint does not match retained audit history"
                )
            if actual_sequence > expected_sequence:
                def mutation(state: MutableMapping[str, Any]) -> None:
                    state["audit_sequence"] = actual_sequence
                    state["audit_head"] = actual_head

                self._mutate_state_locked(mutation)

    def _ensure_operation_capacity_locked(self, tenant_id: str) -> None:
        if len(self._state["operations"]) < self.max_operations:
            return
        retention = self._runtime(tenant_id).definition.retention
        completed = [
            item
            for item in self._state["operations"]
            if item["status"] in ("succeeded", "failed")
        ]
        if not completed:
            raise ControlPlaneCapacity(
                "control operation history capacity is exhausted"
            )
        remove_count = max(
            1,
            len(self._state["operations"])
            - self.max_operations
            + 1,
        )
        removable_ids = {
            item["operation_id"]
            for item in sorted(
                completed, key=lambda item: float(item["created_at"])
            )[:remove_count]
        }

        def prune(state: MutableMapping[str, Any]) -> None:
            state["operations"] = [
                item
                for item in state["operations"]
                if item["operation_id"] not in removable_ids
            ]
            own_completed = [
                item
                for item in state["operations"]
                if item["tenant_id"] == tenant_id
                and item["status"] in ("succeeded", "failed")
            ]
            if len(own_completed) > retention.completed_operations:
                surplus = len(own_completed) - retention.completed_operations
                extra = {
                    item["operation_id"]
                    for item in sorted(
                        own_completed, key=lambda item: float(item["created_at"])
                    )[:surplus]
                }
                state["operations"] = [
                    item
                    for item in state["operations"]
                    if item["operation_id"] not in extra
                ]

        self._mutate_state_locked(prune)

    def _public_operation(self, operation: Mapping[str, Any]) -> Dict[str, Any]:
        value = {
            key: _json_copy(operation[key])
            if isinstance(operation[key], (dict, list))
            else operation[key]
            for key in (
                "operation_id",
                "kind",
                "tenant_id",
                "status",
                "created_at",
                "started_at",
                "completed_at",
                "result",
                "error",
            )
        }
        return value

    def _update_operation(
        self,
        operation_id: str,
        *,
        status: str,
        result: Optional[Mapping[str, Any]] = None,
        error: Optional[str] = None,
    ) -> None:
        now = self._clock()
        bounded_error = None if error is None else str(error)[:4096]
        bounded_result = None if result is None else _json_copy(dict(result))
        if bounded_result is not None and len(_canonical(bounded_result)) > 65_536:
            bounded_result = {
                "truncated": True,
                "message": "operation result exceeded durable byte limit",
            }

        def mutation(state: MutableMapping[str, Any]) -> None:
            for operation in state["operations"]:
                if operation["operation_id"] != operation_id:
                    continue
                operation["status"] = status
                if status == "running":
                    operation["started_at"] = now
                if status in ("succeeded", "failed"):
                    operation["completed_at"] = now
                operation["result"] = bounded_result
                operation["error"] = bounded_error
                return
            raise ControlPlaneError("operation is absent from durable state")

        self._mutate_state(mutation)

    def _operation_worker(self) -> None:
        while not self._closed.is_set():
            try:
                operation_id = self._operations.get(timeout=0.5)
            except queue.Empty:
                continue
            if operation_id is None:
                self._operations.task_done()
                return
            try:
                self._run_operation(operation_id)
            finally:
                self._operations.task_done()

    def _scheduler_worker(self) -> None:
        while not self._closed.wait(self.scheduler_interval_seconds):
            try:
                self.run_retention()
                self.run_scheduled_backups()
            except Exception:
                LOG.exception("control-plane scheduler iteration failed")

    def _requeue_incomplete(self) -> None:
        with self._lock:
            incomplete = [
                operation["operation_id"]
                for operation in self._state["operations"]
                if operation["status"] in ("pending", "running")
            ]

            def reset(state: MutableMapping[str, Any]) -> None:
                for operation in state["operations"]:
                    if operation["operation_id"] in incomplete:
                        operation["status"] = "pending"
                        operation["started_at"] = None
                        operation["completed_at"] = None
                        operation["error"] = None

            if incomplete:
                self._mutate_state_locked(reset)
        for operation_id in incomplete:
            try:
                self._operations.put_nowait(operation_id)
            except queue.Full:
                self._update_operation(
                    operation_id,
                    status="failed",
                    error="operation queue capacity is exhausted after restart",
                )

    def _run_operation(self, operation_id: str) -> None:
        with self._lock:
            operation = next(
                (
                    _json_copy(item)
                    for item in self._state["operations"]
                    if item["operation_id"] == operation_id
                ),
                None,
            )
        if operation is None:
            return
        tenant_id = operation["tenant_id"]
        kind = operation["kind"]
        actor = operation["actor"]
        try:
            self._update_operation(operation_id, status="running")
            handlers = {
                "backup": self._perform_backup,
                "data_export": self._perform_data_export,
                "restore_validate": self._perform_restore_validation,
                "restore": self._perform_restore,
                "dr_drill": self._perform_drill,
                "tenant_delete": self._perform_delete,
                "audit_export": self._perform_audit_export,
            }
            parameters = dict(operation["parameters"])
            parameters["_operation_id"] = operation_id
            result = handlers[kind](tenant_id, parameters)
            self._update_operation(
                operation_id, status="succeeded", result=result
            )
            self._audit_action(
                actor,
                "operation_completed",
                tenant_id,
                "succeeded",
                {"kind": kind, "operation_id": operation_id},
            )
            LOG.info(
                "control operation completed",
                extra={
                    "operation": kind,
                    "operation_id": operation_id,
                    "tenant_namespace": self.namespace_for(tenant_id),
                    "status": "succeeded",
                },
            )
        except Exception as exc:
            if kind == "tenant_delete":
                runtime = self._runtime(tenant_id)

                def restore(state: MutableMapping[str, Any]) -> None:
                    if state["tenants"][tenant_id]["status"] == "deletion_pending":
                        state["tenants"][tenant_id]["status"] = "suspended"

                try:
                    self._mutate_state(restore)
                finally:
                    runtime.set_accepting(False)
            try:
                self._update_operation(
                    operation_id, status="failed", error=str(exc)
                )
            finally:
                try:
                    self._audit_action(
                        actor,
                        "operation_completed",
                        tenant_id,
                        "failed",
                        {
                            "kind": kind,
                            "operation_id": operation_id,
                            "error": str(exc)[:1024],
                        },
                    )
                except Exception:
                    pass
            LOG.warning(
                "control operation failed",
                extra={
                    "operation": kind,
                    "operation_id": operation_id,
                    "tenant_namespace": self.namespace_for(tenant_id),
                    "status": "failed",
                    "reason": str(exc)[:1024],
                },
            )

    def _perform_backup(
        self, tenant_id: str, parameters: Mapping[str, Any]
    ) -> Dict[str, Any]:
        runtime = self._runtime(tenant_id)
        operation_id = str(parameters.get("_operation_id"))
        with self._lock:
            existing = next(
                (
                    item
                    for item in self._state["tenants"][tenant_id]["backups"]
                    if item.get("operation_id") == operation_id
                ),
                None,
            )
        if existing is not None:
            return {
                "backup": _json_copy(existing),
                "encrypted": True,
                "key_provider": "replaceable",
            }
        entries = runtime.engine.export_entries()
        now = self._clock()
        document = _entries_document(
            runtime.namespace_id,
            entries,
            purpose="backup",
            created_at=now,
        )
        metadata = self._artifacts.write(
            "backup", runtime.namespace_id, document
        )
        record = dict(metadata)
        record.update(
            {
                "status": "succeeded",
                "created_at": now,
                "entry_count": len(entries),
                "region": runtime.definition.primary_region,
                "reason": str(parameters.get("reason", "manual"))[:128],
                "operation_id": operation_id,
            }
        )
        removed: List[Mapping[str, Any]] = []

        def mutation(state: MutableMapping[str, Any]) -> None:
            backups = state["tenants"][tenant_id]["backups"]
            backups.append(record)
            backups.sort(key=lambda item: float(item["created_at"]))
            cutoff = now - runtime.definition.backup.retention_seconds
            while len(backups) > runtime.definition.backup.retention_count:
                removed.append(backups.pop(0))
            while len(backups) > 1 and float(backups[0]["created_at"]) < cutoff:
                removed.append(backups.pop(0))

        try:
            self._mutate_state(mutation)
        except Exception:
            self._artifacts.delete("backup", metadata["artifact_id"])
            raise
        for item in removed:
            self._artifacts.delete("backup", str(item["artifact_id"]))
        return {
            "backup": _json_copy(record),
            "encrypted": True,
            "key_provider": "replaceable",
        }

    def _perform_data_export(
        self, tenant_id: str, parameters: Mapping[str, Any]
    ) -> Dict[str, Any]:
        runtime = self._runtime(tenant_id)
        operation_id = str(parameters.get("_operation_id"))
        with self._lock:
            existing = next(
                (
                    item
                    for item in self._state["tenants"][tenant_id]["exports"]
                    if item.get("operation_id") == operation_id
                ),
                None,
            )
        if existing is not None:
            return {
                "export": _json_copy(existing),
                "encrypted": True,
                "contains_values": True,
            }
        entries = runtime.engine.export_entries()
        now = self._clock()
        document = _entries_document(
            runtime.namespace_id,
            entries,
            purpose="data_export",
            created_at=now,
        )
        metadata = self._artifacts.write(
            "export", runtime.namespace_id, document
        )
        record = dict(metadata)
        record.update(
            {
                "status": "succeeded",
                "created_at": now,
                "entry_count": len(entries),
                "expires_at": now
                + runtime.definition.backup.retention_seconds,
                "operation_id": operation_id,
            }
        )
        removed: List[Mapping[str, Any]] = []

        def mutation(state: MutableMapping[str, Any]) -> None:
            exports = state["tenants"][tenant_id]["exports"]
            exports.append(record)
            exports.sort(key=lambda item: float(item["created_at"]))
            while len(exports) > runtime.definition.retention.export_count:
                removed.append(exports.pop(0))

        try:
            self._mutate_state(mutation)
        except Exception:
            self._artifacts.delete("export", metadata["artifact_id"])
            raise
        for item in removed:
            self._artifacts.delete(
                str(item["kind"]), str(item["artifact_id"])
            )
        return {
            "export": _json_copy(record),
            "encrypted": True,
            "contains_values": True,
        }

    def _perform_restore_validation(
        self, tenant_id: str, parameters: Mapping[str, Any]
    ) -> Dict[str, Any]:
        backup_id = _identifier(
            parameters.get("backup_id"), "backup_id"
        )
        entries, document = self._validated_backup(tenant_id, backup_id)
        expires_at = int(self._clock()) + 3600
        token = self._restore_token(
            tenant_id,
            backup_id,
            hashlib.sha256(_canonical(document)).hexdigest(),
            expires_at,
        )
        return {
            "backup_id": backup_id,
            "entry_count": len(entries),
            "valid": True,
            "validation_token": token,
            "validation_expires_at": expires_at,
        }

    def _perform_restore(
        self, tenant_id: str, parameters: Mapping[str, Any]
    ) -> Dict[str, Any]:
        backup_id = _identifier(parameters.get("backup_id"), "backup_id")
        confirmation = parameters.get("confirmed_tenant_id")
        if confirmation != tenant_id:
            raise ControlPlaneError(
                "restore confirmation must exactly match tenant id"
            )
        token = parameters.get("validation_token")
        if not isinstance(token, str):
            raise ControlPlaneError("restore validation_token is required")
        entries, document = self._validated_backup(tenant_id, backup_id)
        digest = hashlib.sha256(_canonical(document)).hexdigest()
        self._verify_restore_token(tenant_id, backup_id, digest, token)
        runtime = self._runtime(tenant_id)
        operation_id = str(parameters.get("_operation_id"))
        last_restore = self._tenant_state(tenant_id).get("last_restore")
        if (
            isinstance(last_restore, Mapping)
            and last_restore.get("operation_id") == operation_id
        ):
            return _json_copy(last_restore)
        runtime.begin_maintenance(runtime.definition.disaster_recovery.rto_seconds)
        background_paused = False
        try:
            pause = getattr(runtime.engine, "pause_background_work", None)
            if pause is not None:
                pause(runtime.definition.disaster_recovery.rto_seconds)
                background_paused = True
            previous = runtime.engine.export_entries()
            runtime.engine.flush()
            try:
                restored = runtime.engine.restore_entries(entries)
            except Exception:
                runtime.engine.flush()
                runtime.engine.restore_entries(previous)
                raise
        finally:
            if background_paused:
                resume = getattr(
                    runtime.engine, "resume_background_work", None
                )
                if resume is not None:
                    resume()
            runtime.end_maintenance()
        completed = self._clock()

        def mutation(state: MutableMapping[str, Any]) -> None:
            state["tenants"][tenant_id]["last_restore"] = {
                "backup_id": backup_id,
                "completed_at": completed,
                "restored_entries": restored,
                "mode": "replace",
                "operation_id": operation_id,
            }

        self._mutate_state(mutation)
        return {
            "backup_id": backup_id,
            "restored_entries": restored,
            "mode": "replace",
            "completed_at": completed,
            "operation_id": operation_id,
        }

    def _perform_drill(
        self, tenant_id: str, parameters: Mapping[str, Any]
    ) -> Dict[str, Any]:
        backup_id = _identifier(parameters.get("backup_id"), "backup_id")
        runtime = self._runtime(tenant_id)
        operation_id = str(parameters.get("_operation_id"))
        with self._lock:
            existing = next(
                (
                    item
                    for item in self._state["tenants"][tenant_id]["drills"]
                    if item.get("operation_id") == operation_id
                ),
                None,
            )
        if existing is not None:
            return _json_copy(existing)
        started = self._monotonic()
        entries, document = self._validated_backup(tenant_id, backup_id)
        scratch = CacheEngine(
            max_entries=runtime.definition.quotas.max_entries,
            max_memory_bytes=runtime.definition.quotas.max_bytes,
            max_entry_bytes=runtime.definition.quotas.max_entry_bytes,
        )
        restored = scratch.restore_entries(entries)
        elapsed = self._monotonic() - started
        created_at = _number(
            document.get("created_at"), "backup created_at", minimum=0
        )
        age = max(0.0, self._clock() - created_at)
        record = {
            "drill_id": secrets.token_hex(16),
            "backup_id": backup_id,
            "completed_at": self._clock(),
            "restored_entries": restored,
            "duration_seconds": elapsed,
            "backup_age_seconds": age,
            "rpo_met": age <= runtime.definition.disaster_recovery.rpo_seconds,
            "rto_met": elapsed <= runtime.definition.disaster_recovery.rto_seconds,
            "scope": "local_validation_only",
            "operation_id": operation_id,
        }
        record["objectives_met"] = bool(record["rpo_met"] and record["rto_met"])

        def mutation(state: MutableMapping[str, Any]) -> None:
            drills = state["tenants"][tenant_id]["drills"]
            drills.append(record)
            del drills[:-100]

        self._mutate_state(mutation)
        return _json_copy(record)

    def _perform_delete(
        self, tenant_id: str, parameters: Mapping[str, Any]
    ) -> Dict[str, Any]:
        if parameters.get("confirmed_tenant_id") != tenant_id:
            raise ControlPlaneError("tenant deletion was not confirmed")
        runtime = self._runtime(tenant_id)
        if self._tenant_state(tenant_id)["status"] == "deleted":
            return {
                "tenant_id": tenant_id,
                "deleted": True,
                "removed_entries": 0,
                "removed_artifacts": 0,
                "idempotent_replay": True,
                "retained_operational_records": (
                    "bounded usage and tamper-evident audit records"
                ),
            }
        runtime.begin_maintenance(runtime.definition.disaster_recovery.rto_seconds)
        removed = 0
        background_paused = False
        engine_closed = False
        try:
            pause = getattr(runtime.engine, "pause_background_work", None)
            if pause is not None:
                pause(runtime.definition.disaster_recovery.rto_seconds)
                background_paused = True
            removed = runtime.engine.flush()
            begin = getattr(runtime.engine, "begin_shutdown", None)
            if begin is not None:
                begin()
            close = getattr(runtime.engine, "close", None)
            if close is not None:
                close(runtime.definition.disaster_recovery.rto_seconds)
                engine_closed = True
            with self._lock:
                artifacts = list(
                    self._state["tenants"][tenant_id]["backups"]
                ) + list(self._state["tenants"][tenant_id]["exports"])
            for item in artifacts:
                self._artifacts.delete(
                    str(item["kind"]), str(item["artifact_id"])
                )
            orphaned_artifacts = self._artifacts.delete_namespace(
                runtime.namespace_id
            )
            removed_artifacts = len(artifacts) + orphaned_artifacts
            self._remove_tenant_state_files(tenant_id)
            deleted_at = self._clock()

            def mutation(state: MutableMapping[str, Any]) -> None:
                tenant = state["tenants"][tenant_id]
                tenant["status"] = "deleted"
                tenant["deleted_at"] = deleted_at
                tenant["backups"] = []
                tenant["exports"] = []
                tenant["observed_deployments"] = {}
                tenant["desired_deployment"]["drain"] = True
                tenant["desired_deployment"]["generation"] += 1
                tenant["desired_deployment"]["updated_at"] = deleted_at

            self._mutate_state(mutation)
            runtime.set_accepting(False)
        finally:
            if background_paused and not engine_closed:
                resume = getattr(
                    runtime.engine, "resume_background_work", None
                )
                if resume is not None:
                    resume()
            runtime.end_maintenance()
        return {
            "tenant_id": tenant_id,
            "deleted": True,
            "removed_entries": removed,
            "removed_artifacts": removed_artifacts,
            "retained_operational_records": (
                "bounded usage and tamper-evident audit records"
            ),
        }

    def _perform_audit_export(
        self, tenant_id: str, parameters: Mapping[str, Any]
    ) -> Dict[str, Any]:
        after = _integer(
            parameters.get("after_sequence", 0),
            "after_sequence",
            minimum=0,
        )
        limit = _integer(
            parameters.get("limit", 1000),
            "audit limit",
            minimum=1,
            maximum=1000,
        )
        operation_id = str(parameters.get("_operation_id"))
        with self._lock:
            existing = next(
                (
                    item
                    for item in self._state["tenants"][tenant_id]["exports"]
                    if item.get("operation_id") == operation_id
                ),
                None,
            )
        if existing is not None:
            return {
                "audit_export": _json_copy(existing),
                "record_count": int(existing.get("record_count", 0)),
            }
        document = self.export_audit(
            tenant_id=tenant_id, after_sequence=after, limit=limit
        )
        metadata = self._artifacts.write(
            "audit-export", self.namespace_for(tenant_id), document
        )
        runtime = self._runtime(tenant_id)
        record = dict(metadata)
        record.update(
            {
                "status": "succeeded",
                "created_at": self._clock(),
                "record_count": len(document["records"]),
                "contains_values": False,
                "operation_id": operation_id,
            }
        )
        removed: List[Mapping[str, Any]] = []

        def mutation(state: MutableMapping[str, Any]) -> None:
            exports = state["tenants"][tenant_id]["exports"]
            exports.append(record)
            exports.sort(key=lambda item: float(item["created_at"]))
            while len(exports) > runtime.definition.retention.export_count:
                removed.append(exports.pop(0))

        try:
            self._mutate_state(mutation)
        except Exception:
            self._artifacts.delete("audit-export", metadata["artifact_id"])
            raise
        for item in removed:
            self._artifacts.delete(
                str(item["kind"]), str(item["artifact_id"])
            )
        return {
            "audit_export": record,
            "record_count": len(document["records"]),
        }

    def _validated_backup(
        self, tenant_id: str, backup_id: str
    ) -> Tuple[Tuple[StorageEntry, ...], Dict[str, Any]]:
        runtime = self._runtime(tenant_id)
        with self._lock:
            exists = any(
                item["artifact_id"] == backup_id
                for item in self._state["tenants"][tenant_id]["backups"]
            )
        if not exists:
            raise ArtifactError("backup is not available")
        document = self._artifacts.read(
            "backup", backup_id, runtime.namespace_id
        )
        entries = _document_entries(
            document,
            runtime.namespace_id,
            "backup",
            now=self._clock(),
        )
        scratch = CacheEngine(
            max_entries=runtime.definition.quotas.max_entries,
            max_memory_bytes=runtime.definition.quotas.max_bytes,
            max_entry_bytes=runtime.definition.quotas.max_entry_bytes,
        )
        scratch.restore_entries(entries)
        return entries, document

    def _restore_token(
        self,
        tenant_id: str,
        backup_id: str,
        digest: str,
        expires_at: int,
    ) -> str:
        key_id = self.key_provider.active_key_id()
        payload = {
            "version": 1,
            "tenant_namespace": self.namespace_for(tenant_id),
            "backup_id": backup_id,
            "sha256": digest,
            "expires_at": expires_at,
            "key_id": key_id,
        }
        payload["signature"] = hmac.new(
            _derived_key(self.key_provider, key_id, b"restore-validation"),
            _canonical(payload),
            hashlib.sha256,
        ).hexdigest()
        return base64.urlsafe_b64encode(_canonical(payload)).decode(
            "ascii"
        ).rstrip("=")

    def _verify_restore_token(
        self,
        tenant_id: str,
        backup_id: str,
        digest: str,
        token: str,
    ) -> None:
        document = self._decode_capability(token, "restore validation")
        signature = document.pop("signature", None)
        key_id = _identifier(document.get("key_id"), "restore key id")
        expected = hmac.new(
            _derived_key(self.key_provider, key_id, b"restore-validation"),
            _canonical(document),
            hashlib.sha256,
        ).hexdigest()
        if (
            not isinstance(signature, str)
            or not hmac.compare_digest(signature, expected)
            or document.get("version") != 1
            or document.get("tenant_namespace") != self.namespace_for(tenant_id)
            or document.get("backup_id") != backup_id
            or document.get("sha256") != digest
            or not isinstance(document.get("expires_at"), int)
            or document["expires_at"] < int(self._clock())
        ):
            raise ControlPlaneError(
                "restore validation token is invalid or expired"
            )

    def _verify_deletion_challenge(
        self, tenant_id: str, token: str
    ) -> None:
        document = self._decode_capability(token, "deletion challenge")
        signature = document.pop("signature", None)
        key_id = _identifier(document.pop("key_id", None), "deletion key id")
        expected = hmac.new(
            _derived_key(self.key_provider, key_id, b"tenant-deletion"),
            _canonical(document),
            hashlib.sha256,
        ).hexdigest()
        if (
            not isinstance(signature, str)
            or not hmac.compare_digest(signature, expected)
            or document.get("version") != 1
            or document.get("tenant_namespace") != self.namespace_for(tenant_id)
            or not isinstance(document.get("expires_at"), int)
            or document["expires_at"] < int(self._clock())
        ):
            raise ControlPlaneError(
                "tenant deletion challenge is invalid or expired"
            )

    @staticmethod
    def _decode_capability(token: Any, field: str) -> Dict[str, Any]:
        if not isinstance(token, str) or not token or len(token) > 4096:
            raise ControlPlaneError("{} is invalid".format(field))
        try:
            padded = token + "=" * (-len(token) % 4)
            value = json.loads(
                base64.b64decode(
                    padded.encode("ascii"), altchars=b"-_", validate=True
                ).decode("utf-8")
            )
        except (
            ValueError,
            UnicodeDecodeError,
            json.JSONDecodeError,
            binascii.Error,
        ) as exc:
            raise ControlPlaneError("{} is invalid".format(field)) from exc
        if not isinstance(value, dict):
            raise ControlPlaneError("{} is invalid".format(field))
        return value

    def _remove_tenant_state_files(self, tenant_id: str) -> None:
        root = self._state_store.directory + os.sep
        for path in self._tenant_state_paths.get(tenant_id, ()):
            for candidate in (path, path + ".lock"):
                absolute = os.path.abspath(candidate)
                if not absolute.startswith(root):
                    raise ControlPlaneError(
                        "tenant state path escapes control state directory"
                    )
                try:
                    metadata = os.lstat(absolute)
                except FileNotFoundError:
                    continue
                if stat.S_ISLNK(metadata.st_mode) or not stat.S_ISREG(
                    metadata.st_mode
                ):
                    raise ControlPlaneError(
                        "tenant state artifacts must be regular files"
                    )
                os.unlink(absolute)

    def _reconcile_artifacts(self) -> None:
        referenced = set()
        with self._lock:
            for tenant in self._state["tenants"].values():
                for item in tenant["backups"] + tenant["exports"]:
                    referenced.add(str(item.get("filename")))
        for name in os.listdir(self._artifacts.directory):
            if _ARTIFACT_FILE.fullmatch(name) is None or name in referenced:
                continue
            path = os.path.join(self._artifacts.directory, name)
            metadata = os.lstat(path)
            if stat.S_ISREG(metadata.st_mode) and not stat.S_ISLNK(
                metadata.st_mode
            ):
                os.unlink(path)

    def _alerts(
        self,
        tenant_id: str,
        state: Mapping[str, Any],
        stats: Mapping[str, Any],
    ) -> List[Dict[str, Any]]:
        runtime = self._runtime(tenant_id)
        alerts = []
        entries = int(stats.get("entries", 0))
        memory = int(stats.get("memory_bytes", 0))
        if entries >= int(runtime.definition.quotas.max_entries * 0.8):
            alerts.append(
                {
                    "code": "entry_quota_pressure",
                    "severity": "warning",
                    "message": "entry use is at least 80% of quota",
                }
            )
        if memory >= int(runtime.definition.quotas.max_bytes * 0.8):
            alerts.append(
                {
                    "code": "byte_quota_pressure",
                    "severity": "warning",
                    "message": "byte use is at least 80% of quota",
                }
            )
        backups = [
            item for item in state["backups"] if item.get("status") == "succeeded"
        ]
        if runtime.definition.backup.interval_seconds > 0:
            latest = (
                0.0
                if not backups
                else max(float(item["created_at"]) for item in backups)
            )
            if self._clock() - latest > runtime.definition.disaster_recovery.rpo_seconds:
                alerts.append(
                    {
                        "code": "rpo_at_risk",
                        "severity": "critical",
                        "message": "latest validated backup is older than RPO",
                    }
                )
        else:
            alerts.append(
                {
                    "code": "backup_schedule_disabled",
                    "severity": "warning",
                    "message": "automatic backups are disabled",
                }
            )
        desired = state["desired_deployment"]
        observations = list(state["observed_deployments"].values())
        healthy = [
            item
            for item in observations
            if item["health"] == "healthy" and not item["draining"]
        ]
        deployment_drift = (
            not observations
            or any(
                item["health"] != "healthy"
                or item["version"] != desired["version"]
                or bool(item["draining"]) != bool(desired["drain"])
                or int(item["observed_generation"])
                < int(desired["generation"])
                or item["region"] not in desired["regions"]
                for item in observations
            )
            or (
                not desired["drain"]
                and len(healthy) < int(desired["replicas"])
            )
        )
        if deployment_drift:
            alerts.append(
                {
                    "code": "deployment_drift",
                    "severity": "warning",
                    "message": "observed deployment differs from desired state",
                }
            )
        if observations and any(
            self._clock() - float(item["observed_at"])
            > max(60.0, self.scheduler_interval_seconds * 3)
            for item in observations
        ):
            alerts.append(
                {
                    "code": "deployment_observation_stale",
                    "severity": "warning",
                    "message": "deployment observation is stale",
                }
            )
        if self._last_persistence_error is not None:
            alerts.append(
                {
                    "code": "metering_persistence_degraded",
                    "severity": "critical",
                    "message": "usage state persistence has failed",
                }
            )
        return alerts

    @staticmethod
    def _origin_status(engine: StorageBackend) -> Dict[str, Any]:
        method = getattr(engine, "origins", None)
        return {} if method is None else method()

    def _observe_aggregate(
        self,
        protocol: str,
        operation: str,
        duration_seconds: float,
        success: bool,
    ) -> None:
        status = "success" if success else "error"
        duration = max(0.0, float(duration_seconds))
        with self._lock:
            self._request_counts[(protocol, operation, status)] += 1
            self._latency_sums[(protocol, operation)] += duration
            for bucket in self._latency_buckets:
                if duration <= bucket:
                    self._latency_counts[
                        (protocol, operation, bucket)
                    ] += 1

    @staticmethod
    def _prometheus_label(value: str) -> str:
        return (
            str(value)
            .replace("\\", "\\\\")
            .replace("\n", "\\n")
            .replace('"', '\\"')
        )
