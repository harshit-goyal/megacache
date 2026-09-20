"""Thread-safe cache engine with stale reads, tags, leases, and singleflight."""

import copy
import math
import json
import secrets
import threading
import time
from collections import OrderedDict, defaultdict
from dataclasses import dataclass
from typing import Any, Callable, Dict, FrozenSet, Iterable, Optional, Tuple

from .storage import StorageEntry


@dataclass(frozen=True)
class CacheResult:
    state: str
    value: Any = None
    expires_in_seconds: float = 0
    stale_for_seconds: float = 0
    lease_token: Optional[str] = None
    retry_after_seconds: float = 0


@dataclass
class _Entry:
    value: Any
    fresh_until: float
    stale_until: float
    tags: FrozenSet[str]
    size_bytes: int


@dataclass
class _Lease:
    token: str
    until: float
    size_bytes: int


class _Flight:
    def __init__(self) -> None:
        self.event = threading.Event()
        self.result: Any = None
        self.error: Optional[BaseException] = None


@dataclass
class _EngineCheckpoint:
    entries: "OrderedDict[str, _Entry]"
    used_bytes: int
    lease_bytes: int
    tags: Dict[str, set]
    leases: Dict[str, _Lease]
    metrics: Dict[str, int]


@dataclass
class _MutationCheckpoint:
    key: str
    entry: Optional[_Entry]
    predecessor: Optional[str]
    lease: Optional[_Lease]
    used_bytes: int
    lease_bytes: int
    metrics: Dict[str, int]
    evicted_entries: list
    evicted_keys: set


class CacheEngine:
    """Bounded in-memory LRU cache with correctness-oriented primitives."""

    def __init__(
        self,
        max_entries: int = 10_000,
        default_ttl_seconds: int = 300,
        default_stale_seconds: int = 900,
        lease_seconds: int = 30,
        clock: Callable[[], float] = time.monotonic,
        *,
        max_memory_bytes: int = 67_108_864,
        max_entry_bytes: int = 1_048_576,
    ) -> None:
        if min(
            max_entries,
            max_memory_bytes,
            max_entry_bytes,
            default_ttl_seconds,
            default_stale_seconds,
            lease_seconds,
        ) <= 0:
            raise ValueError("cache limits and durations must be greater than zero")
        self._max_entries = max_entries
        self._max_memory_bytes = max_memory_bytes
        self._max_entry_bytes = max_entry_bytes
        self._default_ttl = default_ttl_seconds
        self._default_stale = default_stale_seconds
        self._lease_seconds = lease_seconds
        self._clock = clock
        self._entries: "OrderedDict[str, _Entry]" = OrderedDict()
        self._used_bytes = 0
        self._lease_bytes = 0
        self._tags: Dict[str, set] = defaultdict(set)
        self._leases: Dict[str, _Lease] = {}
        self._flights: Dict[str, _Flight] = {}
        self._coordination_flights: Dict[str, _Flight] = {}
        self._metrics: Dict[str, int] = defaultdict(int)
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
        self._lock = threading.RLock()
        self._active_mutation: Optional[_MutationCheckpoint] = None

    def get(self, key: str) -> CacheResult:
        self._validate_key(key)
        with self._lock:
            return self._lookup(key, count=True)

    def put(
        self,
        key: str,
        value: Any,
        ttl_seconds: Optional[int] = None,
        stale_seconds: Optional[int] = None,
        tags: Iterable[str] = (),
        lease_token: Optional[str] = None,
        persistent: bool = False,
    ) -> CacheResult:
        (
            stored_value,
            ttl,
            stale,
            normalized_tags,
            size_bytes,
        ) = self._prepare_put(
            key,
            value,
            ttl_seconds,
            stale_seconds,
            tags,
            persistent,
            count_rejection=True,
        )
        now = self._clock()

        with self._lock:
            if lease_token is not None:
                lease = self._leases.get(key)
                if lease is None or lease.until <= now:
                    self._remove_lease(key)
                    raise ValueError("lease is missing or expired")
                if not secrets.compare_digest(lease.token, lease_token):
                    raise ValueError("lease token does not match")

            existing_lease = self._leases.get(key)
            reserved_bytes = self._lease_bytes - (
                0 if existing_lease is None else existing_lease.size_bytes
            )
            if size_bytes + reserved_bytes > self._max_memory_bytes:
                self._metrics["rejected_entries_total"] += 1
                raise ValueError("entry exceeds available cache memory")

            self._remove_entry(key)
            self._entries[key] = _Entry(
                value=stored_value,
                fresh_until=now + ttl,
                stale_until=now + ttl + stale,
                tags=normalized_tags,
                size_bytes=size_bytes,
            )
            self._used_bytes += size_bytes
            for tag in normalized_tags:
                self._tags[tag].add(key)
            self._remove_lease(key)
            self._metrics["writes_total"] += 1
            self._evict_if_needed()
            return self._lookup(key, count=False)

    def validate_put(
        self,
        key: str,
        value: Any,
        ttl_seconds: Optional[int] = None,
        stale_seconds: Optional[int] = None,
        tags: Iterable[str] = (),
        lease_token: Optional[str] = None,
        persistent: bool = False,
    ) -> None:
        """Validate a write's deterministic admission checks without mutation."""
        _, _, _, _, size_bytes = self._prepare_put(
            key,
            value,
            ttl_seconds,
            stale_seconds,
            tags,
            persistent,
            count_rejection=False,
        )
        now = self._clock()
        with self._lock:
            if lease_token is not None:
                lease = self._leases.get(key)
                if lease is None or lease.until <= now:
                    raise ValueError("lease is missing or expired")
                if not secrets.compare_digest(lease.token, lease_token):
                    raise ValueError("lease token does not match")
            existing_lease = self._leases.get(key)
            reserved_bytes = self._lease_bytes - (
                0 if existing_lease is None else existing_lease.size_bytes
            )
            if size_bytes + reserved_bytes > self._max_memory_bytes:
                raise ValueError("entry exceeds available cache memory")

    def mget(self, keys: Iterable[str]) -> Tuple[CacheResult, ...]:
        normalized = tuple(keys)
        with self._lock:
            return tuple(self.get(key) for key in normalized)

    def mset(self, values: Iterable[Tuple[str, Any]]) -> None:
        normalized = tuple(values)
        for key, value in normalized:
            self._validate_key(key)
            size_bytes = self._entry_size(key, value, frozenset())
            if (
                size_bytes > self._max_entry_bytes
                or size_bytes > self._max_memory_bytes
            ):
                with self._lock:
                    self._metrics["rejected_entries_total"] += 1
                raise ValueError(
                    "entry requires {} bytes; maximum is {}".format(
                        size_bytes,
                        min(self._max_entry_bytes, self._max_memory_bytes),
                    )
                )
        with self._lock:
            for key, value in normalized:
                self.put(key, value, persistent=True)

    def delete(self, key: str) -> bool:
        self._validate_key(key)
        with self._lock:
            existed = self._remove_entry(key)
            self._remove_lease(key)
            if existed:
                self._metrics["deletes_total"] += 1
            return existed

    def delete_many(self, keys: Iterable[str]) -> int:
        normalized = tuple(keys)
        with self._lock:
            return sum(1 for key in normalized if self.delete(key))

    def exists(self, keys: Iterable[str]) -> int:
        normalized = tuple(keys)
        with self._lock:
            return sum(
                1
                for key in normalized
                if self._lookup(key, count=False).state != "miss"
            )

    def expire(self, key: str, ttl_seconds: int) -> bool:
        self._validate_key(key)
        ttl = self._duration(ttl_seconds, 0, "ttl_seconds")
        with self._lock:
            if self._lookup(key, count=False).state == "miss":
                return False
            entry = self._entries[key]
            deadline = self._clock() + ttl
            entry.fresh_until = deadline
            entry.stale_until = deadline
            self._metrics["expirations_set_total"] += 1
            return True

    def _remove_lease(self, key: str) -> bool:
            lease = self._leases.pop(key, None)
            if lease is None:
                return False
            self._used_bytes -= lease.size_bytes
            self._lease_bytes -= lease.size_bytes
            return True

    def _purge_expired_leases(self, now: float) -> None:
            for key, lease in tuple(self._leases.items()):
                if lease.until <= now:
                    self._remove_lease(key)

    def ttl(self, key: str) -> int:
        self._validate_key(key)
        with self._lock:
            if self._lookup(key, count=False).state == "miss":
                return -2
            deadline = self._entries[key].fresh_until
            if math.isinf(deadline):
                return -1
            return max(0, int(deadline - self._clock()))

    def flush(self) -> int:
        with self._lock:
            removed = len(self._entries)
            self._entries.clear()
            self._tags.clear()
            self._leases.clear()
            self._used_bytes = 0
            self._lease_bytes = 0
            self._metrics["flushes_total"] += 1
            return removed

    def size(self) -> int:
        with self._lock:
            for key in tuple(self._entries):
                self._lookup(key, count=False)
            return len(self._entries)

    def keys(self) -> Tuple[str, ...]:
        """Return live keys without changing their LRU order."""
        with self._lock:
            now = self._clock()
            for key, entry in tuple(self._entries.items()):
                if now >= entry.stale_until:
                    self._remove_entry(key)
            return tuple(self._entries)

    def export_entries(
        self, keys: Optional[Iterable[str]] = None
    ) -> Tuple[StorageEntry, ...]:
        """Export live entries with remaining freshness windows."""
        with self._lock:
            now = self._clock()
            selected = tuple(self._entries) if keys is None else tuple(keys)
            exported = []
            for key in selected:
                entry = self._entries.get(key)
                if entry is None:
                    continue
                if now >= entry.stale_until:
                    self._remove_entry(key)
                    continue
                persistent = math.isinf(entry.fresh_until)
                exported.append(
                    StorageEntry(
                        key=key,
                        value=self._copy_value(entry.value),
                        fresh_for_seconds=(
                            None
                            if persistent
                            else max(0.0, entry.fresh_until - now)
                        ),
                        stale_for_seconds=(
                            None
                            if persistent
                            else max(
                                0.0,
                                entry.stale_until - max(now, entry.fresh_until),
                            )
                        ),
                        tags=tuple(sorted(entry.tags)),
                        persistent=persistent,
                    )
                )
            return tuple(exported)

    def restore_entries(self, entries: Iterable[StorageEntry]) -> int:
        """Atomically restore a validated snapshot into local storage."""
        prepared = self._prepare_restore_entries(entries)

        with self._lock:
            self._purge_expired_entries(self._clock())
            self._validate_prepared_restore_capacity(prepared)
            old_entries = self._entries.copy()
            old_tags = defaultdict(
                set,
                ((tag, set(keys)) for tag, keys in self._tags.items()),
            )
            old_used_bytes = self._used_bytes
            old_metrics = self._metrics.copy()
            try:
                now = self._clock()
                for (
                    key,
                    value,
                    tags,
                    size_bytes,
                    fresh_for,
                    stale_for,
                ) in prepared:
                    self._remove_entry(key)
                    if fresh_for is None:
                        fresh_until = math.inf
                        stale_until = math.inf
                    else:
                        fresh_until = now + fresh_for
                        stale_until = fresh_until + stale_for
                    self._entries[key] = _Entry(
                        value=value,
                        fresh_until=fresh_until,
                        stale_until=stale_until,
                        tags=tags,
                        size_bytes=size_bytes,
                    )
                    self._used_bytes += size_bytes
                    for tag in tags:
                        self._tags[tag].add(key)
            except BaseException:
                self._entries = old_entries
                self._tags = old_tags
                self._used_bytes = old_used_bytes
                self._metrics = old_metrics
                raise
            return len(prepared)

    def validate_restore_entries(
        self, entries: Iterable[StorageEntry]
    ) -> None:
        """Validate an atomic restore without changing cache state."""
        prepared = self._prepare_restore_entries(entries)
        with self._lock:
            self._purge_expired_entries(self._clock())
            self._validate_prepared_restore_capacity(prepared)

    def begin_mutation_checkpoint(self, key: str) -> _MutationCheckpoint:
        """Start a targeted single-key rollback journal.

        The engine lock remains held until the checkpoint is committed or
        restored, so concurrent direct users cannot interleave mutations.
        """
        self._validate_key(key)
        self._lock.acquire()
        try:
            if self._active_mutation is not None:
                raise RuntimeError("a cache mutation checkpoint is already active")
            predecessor = None
            for existing_key in self._entries:
                if existing_key == key:
                    break
                predecessor = existing_key
            checkpoint = _MutationCheckpoint(
                key=key,
                entry=copy.deepcopy(self._entries.get(key)),
                predecessor=predecessor,
                lease=copy.deepcopy(self._leases.get(key)),
                used_bytes=self._used_bytes,
                lease_bytes=self._lease_bytes,
                metrics=dict(self._metrics),
                evicted_entries=[],
                evicted_keys=set(),
            )
            self._active_mutation = checkpoint
            return checkpoint
        except BaseException:
            self._lock.release()
            raise

    def commit_mutation_checkpoint(
        self, checkpoint: _MutationCheckpoint
    ) -> None:
        """Commit a targeted mutation and release its journal."""
        self._validate_active_mutation(checkpoint)
        self._active_mutation = None
        self._lock.release()

    def restore_mutation_checkpoint(
        self, checkpoint: _MutationCheckpoint
    ) -> None:
        """Restore one mutation, including LRU, evictions, leases and metrics."""
        self._validate_active_mutation(checkpoint)
        try:
            self._active_mutation = None
            self._remove_entry(checkpoint.key)
            self._remove_lease(checkpoint.key)
            for evicted_key, evicted_entry in reversed(
                checkpoint.evicted_entries
            ):
                self._remove_entry(evicted_key)
                restored = copy.deepcopy(evicted_entry)
                self._entries[evicted_key] = restored
                self._entries.move_to_end(evicted_key, last=False)
                for tag in restored.tags:
                    self._tags[tag].add(evicted_key)
            if checkpoint.entry is not None:
                restored = copy.deepcopy(checkpoint.entry)
                if checkpoint.predecessor is None:
                    self._entries[checkpoint.key] = restored
                    self._entries.move_to_end(checkpoint.key, last=False)
                else:
                    tail = []
                    while self._entries:
                        tail_key, tail_entry = self._entries.popitem(last=True)
                        if tail_key == checkpoint.predecessor:
                            self._entries[tail_key] = tail_entry
                            break
                        tail.append((tail_key, tail_entry))
                    self._entries[checkpoint.key] = restored
                    for tail_key, tail_entry in reversed(tail):
                        self._entries[tail_key] = tail_entry
                for tag in restored.tags:
                    self._tags[tag].add(checkpoint.key)
            if checkpoint.lease is not None:
                self._leases[checkpoint.key] = copy.deepcopy(checkpoint.lease)
            self._used_bytes = checkpoint.used_bytes
            self._lease_bytes = checkpoint.lease_bytes
            self._metrics = defaultdict(int, checkpoint.metrics)
        finally:
            self._active_mutation = None
            self._lock.release()

    def checkpoint(self) -> _EngineCheckpoint:
        """Capture exact mutable storage state for coordinator rollback."""
        with self._lock:
            return _EngineCheckpoint(
                entries=copy.deepcopy(self._entries),
                used_bytes=self._used_bytes,
                lease_bytes=self._lease_bytes,
                tags={tag: set(keys) for tag, keys in self._tags.items()},
                leases=copy.deepcopy(self._leases),
                metrics=dict(self._metrics),
            )

    def restore_checkpoint(self, checkpoint: _EngineCheckpoint) -> None:
        """Restore a checkpoint without producing cache operation side effects."""
        if not isinstance(checkpoint, _EngineCheckpoint):
            raise ValueError("invalid cache checkpoint")
        with self._lock:
            self._entries = copy.deepcopy(checkpoint.entries)
            self._used_bytes = checkpoint.used_bytes
            self._lease_bytes = checkpoint.lease_bytes
            self._tags = defaultdict(
                set,
                (
                    (tag, set(keys))
                    for tag, keys in checkpoint.tags.items()
                ),
            )
            self._leases = copy.deepcopy(checkpoint.leases)
            self._metrics = defaultdict(int, checkpoint.metrics)

    def invalidate_tags(self, tags: Iterable[str]) -> int:
        normalized = self._normalize_tags(tags)
        with self._lock:
            keys = set()
            for tag in normalized:
                keys.update(self._tags.get(tag, set()))
            removed = 0
            for key in keys:
                removed += int(self._remove_entry(key))
                self._remove_lease(key)
            self._metrics["invalidations_total"] += removed
            return removed

    def acquire_lease(self, key: str, force: bool = False) -> CacheResult:
        self._validate_key(key)
        if not isinstance(force, bool):
            raise ValueError("force must be a boolean")
        now = self._clock()
        with self._lock:
            self._purge_expired_leases(now)
            cached = self._lookup(key, count=True)
            if cached.state == "fresh" and not force:
                return cached

            lease = self._leases.get(key)
            if lease is not None and lease.until > now:
                if cached.state == "stale":
                    self._metrics["coalesced_total"] += 1
                    return cached
                self._metrics["coalesced_total"] += 1
                return CacheResult(
                    state="loading",
                    retry_after_seconds=max(0, lease.until - now),
                )

            token = secrets.token_urlsafe(24)
            size_bytes = (
                len(key.encode("utf-8")) + len(token.encode("ascii")) + 64
            )
            if (
                len(self._leases) >= self._max_entries
                or self._used_bytes + size_bytes > self._max_memory_bytes
            ):
                self._metrics["rejected_leases_total"] += 1
                raise ValueError("lease capacity exhausted")
            self._leases[key] = _Lease(
                token=token,
                until=now + self._lease_seconds,
                size_bytes=size_bytes,
            )
            self._used_bytes += size_bytes
            self._lease_bytes += size_bytes
            self._metrics["leases_total"] += 1
            if cached.state == "stale":
                return CacheResult(
                    state="stale_lease",
                    value=cached.value,
                    stale_for_seconds=cached.stale_for_seconds,
                    lease_token=token,
                    expires_in_seconds=self._lease_seconds,
                )
            return CacheResult(
                state="lease",
                lease_token=token,
                expires_in_seconds=self._lease_seconds,
            )

    def release_lease(self, key: str, lease_token: str) -> bool:
        self._validate_key(key)
        if not isinstance(lease_token, str):
            raise ValueError("lease token must be a string")
        with self._lock:
            lease = self._leases.get(key)
            if lease is None or not secrets.compare_digest(
                lease.token, lease_token
            ):
                return False
            return self._remove_lease(key)

    def renew_lease(self, key: str, lease_token: str) -> bool:
        self._validate_key(key)
        if not isinstance(lease_token, str):
            raise ValueError("lease token must be a string")
        now = self._clock()
        with self._lock:
            lease = self._leases.get(key)
            if lease is None or lease.until <= now:
                self._remove_lease(key)
                return False
            if not secrets.compare_digest(lease.token, lease_token):
                return False
            lease.until = now + self._lease_seconds
            self._metrics["lease_renewals_total"] += 1
            return True

    def get_or_load(
        self,
        key: str,
        loader: Callable[[], Any],
        ttl_seconds: Optional[int] = None,
        stale_seconds: Optional[int] = None,
        tags: Iterable[str] = (),
    ) -> CacheResult:
        """Load one missing key once while concurrent callers wait."""
        self._validate_key(key)
        with self._lock:
            cached = self._lookup(key, count=True)
            if cached.state != "miss":
                return cached
            flight = self._flights.get(key)
            leader = flight is None
            if leader:
                flight = _Flight()
                self._flights[key] = flight
            else:
                self._metrics["coalesced_total"] += 1

        assert flight is not None
        if not leader:
            flight.event.wait()
            if flight.error is not None:
                raise flight.error
            assert flight.result is not None
            return flight.result

        try:
            value = loader()
            flight.result = self.put(
                key, value, ttl_seconds, stale_seconds, tags
            )
            return flight.result
        except BaseException as exc:
            flight.error = exc
            with self._lock:
                self._metrics["load_errors_total"] += 1
            raise
        finally:
            with self._lock:
                self._flights.pop(key, None)
                flight.event.set()

    def coordinate(self, key: str, loader: Callable[[], Any]) -> Any:
        """Run one operation per coordination key while followers wait."""
        self._validate_key(key)
        with self._lock:
            flight = self._coordination_flights.get(key)
            leader = flight is None
            if leader:
                flight = _Flight()
                self._coordination_flights[key] = flight
            else:
                self._metrics["coalesced_total"] += 1
        assert flight is not None
        if not leader:
            flight.event.wait()
            if flight.error is not None:
                raise flight.error
            return flight.result
        try:
            flight.result = loader()
            return flight.result
        except BaseException as exc:
            flight.error = exc
            raise
        finally:
            with self._lock:
                self._coordination_flights.pop(key, None)
                flight.event.set()

    def stats(self) -> Dict[str, int]:
        with self._lock:
            self._purge_expired_leases(self._clock())
            snapshot = dict(self._metrics)
            snapshot["entries"] = self.size()
            snapshot["memory_bytes"] = self._used_bytes
            snapshot["lease_memory_bytes"] = self._lease_bytes
            snapshot["memory_limit_bytes"] = self._max_memory_bytes
            snapshot["tags"] = len(self._tags)
            snapshot["active_leases"] = sum(
                1 for lease in self._leases.values() if lease.until > self._clock()
            )
            snapshot["protocol_requests_total"] = sum(
                self._request_counts.values()
            )
            snapshot["request_errors_total"] = sum(
                count
                for (_, _, status), count in self._request_counts.items()
                if status == "error"
            )
            return snapshot

    def observe_request(
        self,
        protocol: str,
        operation: str,
        duration_seconds: float,
        success: bool,
    ) -> None:
        status = "success" if success else "error"
        with self._lock:
            self._request_counts[(protocol, operation, status)] += 1
            self._latency_sums[(protocol, operation)] += duration_seconds
            for bucket in self._latency_buckets:
                if duration_seconds <= bucket:
                    self._latency_counts[(protocol, operation, bucket)] += 1

    def prometheus_metrics(self) -> str:
        with self._lock:
            stats = self.stats()
            lines = []
            for name, value in sorted(stats.items()):
                metric_type = (
                    "gauge"
                    if name in (
                        "entries",
                        "tags",
                        "active_leases",
                        "memory_bytes",
                        "lease_memory_bytes",
                        "memory_limit_bytes",
                    )
                    else "counter"
                )
                lines.extend(
                    [
                        "# HELP megacache_{} MegaCache metric.".format(name),
                        "# TYPE megacache_{} {}".format(name, metric_type),
                        "megacache_{} {}".format(name, value),
                    ]
                )
            lines.extend(
                [
                    "# HELP megacache_requests_total Requests by protocol, operation, and status.",
                    "# TYPE megacache_requests_total counter",
                ]
            )
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
                [
                    "# HELP megacache_request_duration_seconds Request latency.",
                    "# TYPE megacache_request_duration_seconds histogram",
                ]
            )
            operations = sorted(self._latency_sums)
            for protocol, operation in operations:
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
                    for (item_protocol, item_operation, _), value
                    in self._request_counts.items()
                    if item_protocol == protocol and item_operation == operation
                )
                lines.extend(
                    [
                        'megacache_request_duration_seconds_bucket{{{},le="+Inf"}} {}'.format(
                            label, count
                        ),
                        "megacache_request_duration_seconds_sum{{{}}} {}".format(
                            label, self._latency_sums[(protocol, operation)]
                        ),
                        "megacache_request_duration_seconds_count{{{}}} {}".format(
                            label, count
                        ),
                    ]
                )
            return "\n".join(lines) + "\n"

    @staticmethod
    def _prometheus_label(value: str) -> str:
        return value.replace("\\", "\\\\").replace("\n", "\\n").replace('"', '\\"')

    def _lookup(self, key: str, count: bool) -> CacheResult:
        now = self._clock()
        entry = self._entries.get(key)
        if entry is None:
            if count:
                self._metrics["misses_total"] += 1
            return CacheResult(state="miss")
        if now >= entry.stale_until:
            self._remove_entry(key)
            if count:
                self._metrics["misses_total"] += 1
                self._metrics["expirations_total"] += 1
            return CacheResult(state="miss")

        self._entries.move_to_end(key)
        if now < entry.fresh_until:
            if count:
                self._metrics["hits_total"] += 1
            return CacheResult(
                state="fresh",
                value=self._copy_value(entry.value),
                expires_in_seconds=max(0, entry.fresh_until - now),
                stale_for_seconds=max(
                    0, entry.stale_until - entry.fresh_until
                ),
            )
        if count:
            self._metrics["stale_hits_total"] += 1
        return CacheResult(
            state="stale",
            value=self._copy_value(entry.value),
            stale_for_seconds=max(0, entry.stale_until - now),
        )

    def _remove_entry(self, key: str) -> bool:
        entry = self._entries.pop(key, None)
        if entry is None:
            return False
        mutation = self._active_mutation
        if (
            mutation is not None
            and key != mutation.key
            and key not in mutation.evicted_keys
        ):
            mutation.evicted_entries.append((key, copy.deepcopy(entry)))
            mutation.evicted_keys.add(key)
        self._used_bytes -= entry.size_bytes
        for tag in entry.tags:
            keys = self._tags.get(tag)
            if keys is not None:
                keys.discard(key)
                if not keys:
                    self._tags.pop(tag, None)
        return True

    def _purge_expired_entries(self, now: float) -> None:
        for key, entry in tuple(self._entries.items()):
            if now >= entry.stale_until:
                self._remove_entry(key)

    def _validate_active_mutation(
        self, checkpoint: _MutationCheckpoint
    ) -> None:
        if (
            not isinstance(checkpoint, _MutationCheckpoint)
            or self._active_mutation is not checkpoint
        ):
            raise ValueError("invalid cache mutation checkpoint")

    def _evict_if_needed(self) -> None:
        while (
            len(self._entries) > self._max_entries
            or self._used_bytes > self._max_memory_bytes
        ):
            key = next(iter(self._entries))
            self._remove_entry(key)
            self._metrics["evictions_total"] += 1

    def _prepare_put(
        self,
        key: str,
        value: Any,
        ttl_seconds: Optional[int],
        stale_seconds: Optional[int],
        tags: Iterable[str],
        persistent: bool,
        count_rejection: bool,
    ) -> Tuple[Any, float, int, FrozenSet[str], int]:
        self._validate_key(key)
        if persistent:
            if ttl_seconds is not None or stale_seconds is not None:
                raise ValueError(
                    "persistent entries cannot have expiration windows"
                )
            ttl = math.inf
            stale = 0
        else:
            ttl = self._duration(
                ttl_seconds, self._default_ttl, "ttl_seconds"
            )
            stale = self._duration(
                stale_seconds,
                self._default_stale,
                "stale_seconds",
                allow_zero=True,
            )
        normalized_tags = frozenset(self._normalize_tags(tags))
        stored_value = self._snapshot_value(value)
        size_bytes = self._entry_size(key, stored_value, normalized_tags)
        if size_bytes > self._max_entry_bytes:
            if count_rejection:
                with self._lock:
                    self._metrics["rejected_entries_total"] += 1
            raise ValueError(
                "entry requires {} bytes; maximum is {}".format(
                    size_bytes, self._max_entry_bytes
                )
            )
        if size_bytes > self._max_memory_bytes:
            if count_rejection:
                with self._lock:
                    self._metrics["rejected_entries_total"] += 1
            raise ValueError("entry exceeds total cache memory limit")
        return stored_value, ttl, stale, normalized_tags, size_bytes

    def _prepare_restore_entries(
        self, entries: Iterable[StorageEntry]
    ) -> list:
        normalized = tuple(entries)
        for item in normalized:
            if not isinstance(item, StorageEntry):
                raise ValueError("snapshot entries must be StorageEntry values")
            self._validate_key(item.key)
        if len({item.key for item in normalized}) != len(normalized):
            raise ValueError("snapshot contains duplicate keys")
        prepared = []
        for item in normalized:
            tags = frozenset(self._normalize_tags(item.tags))
            value = self._snapshot_value(item.value)
            if item.persistent:
                fresh_for = None
                stale_for = None
            else:
                fresh_for = self._snapshot_duration(
                    item.fresh_for_seconds, "fresh_for_seconds"
                )
                stale_for = self._snapshot_duration(
                    item.stale_for_seconds, "stale_for_seconds"
                )
                if fresh_for + stale_for <= 0:
                    continue
            size_bytes = self._entry_size(item.key, value, tags)
            if (
                size_bytes > self._max_entry_bytes
                or size_bytes > self._max_memory_bytes
            ):
                raise ValueError("snapshot entry exceeds configured limits")
            prepared.append(
                (item.key, value, tags, size_bytes, fresh_for, stale_for)
            )
        return prepared

    def _validate_prepared_restore_capacity(self, prepared: list) -> None:
        replaced_bytes = sum(
            self._entries[key].size_bytes
            for key, *_ in prepared
            if key in self._entries
        )
        replaced_count = sum(
            1 for key, *_ in prepared if key in self._entries
        )
        final_count = len(self._entries) - replaced_count + len(prepared)
        final_bytes = (
            self._used_bytes
            - replaced_bytes
            + sum(item[3] for item in prepared)
        )
        if (
            final_count > self._max_entries
            or final_bytes > self._max_memory_bytes
        ):
            raise ValueError("snapshot exceeds available cache capacity")

    @staticmethod
    def _entry_size(key: str, value: Any, tags: FrozenSet[str]) -> int:
        if isinstance(value, bytes):
            value_bytes = len(value)
        elif isinstance(value, str):
            value_bytes = len(value.encode("utf-8"))
        else:
            try:
                value_bytes = len(
                    json.dumps(
                        value, separators=(",", ":"), ensure_ascii=False
                    ).encode("utf-8")
                )
            except (TypeError, ValueError) as exc:
                raise ValueError("value must be JSON serializable or bytes") from exc
        metadata_bytes = (
            len(key.encode("utf-8"))
            + sum(len(tag.encode("utf-8")) for tag in tags)
            + 128
        )
        return value_bytes + metadata_bytes

    @staticmethod
    def _snapshot_value(value: Any) -> Any:
        if isinstance(value, bytes):
            return bytes(value)
        if isinstance(value, str):
            return value
        try:
            encoded = json.dumps(
                value, separators=(",", ":"), ensure_ascii=False
            )
            return json.loads(encoded)
        except (TypeError, ValueError) as exc:
            raise ValueError("value must be JSON serializable or bytes") from exc

    @staticmethod
    def _copy_value(value: Any) -> Any:
        if isinstance(value, (bytes, str)):
            return value
        return copy.deepcopy(value)

    @staticmethod
    def _validate_key(key: str) -> None:
        if not isinstance(key, str) or not key or len(key) > 1024:
            raise ValueError("key must contain between 1 and 1024 characters")

    @staticmethod
    def _duration(
        value: Optional[int], default: int, name: str, allow_zero: bool = False
    ) -> int:
        selected = default if value is None else value
        minimum = 0 if allow_zero else 1
        if (
            not isinstance(selected, int)
            or isinstance(selected, bool)
            or selected < minimum
        ):
            qualifier = "non-negative" if allow_zero else "positive"
            raise ValueError("{} must be a {} integer".format(name, qualifier))
        return selected

    @staticmethod
    def _snapshot_duration(value: Optional[float], name: str) -> float:
        if (
            value is None
            or isinstance(value, bool)
            or not isinstance(value, (int, float))
            or not math.isfinite(value)
            or value < 0
        ):
            raise ValueError("{} must be a finite non-negative number".format(name))
        return float(value)

    @staticmethod
    def _normalize_tags(tags: Iterable[str]) -> Tuple[str, ...]:
        if isinstance(tags, (str, bytes)):
            raise ValueError("tags must be an array of strings")
        normalized = tuple(tags)
        if len(normalized) > 100:
            raise ValueError("an entry may have at most 100 tags")
        if any(
            not isinstance(tag, str)
            or not tag
            or len(tag.encode("utf-8")) > 256
            for tag in normalized
        ):
            raise ValueError("tags must contain between 1 and 256 UTF-8 bytes")
        return normalized
