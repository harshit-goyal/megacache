"""Thread-safe cache engine with stale reads, tags, leases, and singleflight."""

import math
import secrets
import threading
import time
from collections import OrderedDict, defaultdict
from dataclasses import dataclass
from typing import Any, Callable, Dict, FrozenSet, Iterable, Optional, Tuple


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


@dataclass
class _Lease:
    token: str
    until: float


class _Flight:
    def __init__(self) -> None:
        self.event = threading.Event()
        self.result: Optional[CacheResult] = None
        self.error: Optional[BaseException] = None


class CacheEngine:
    """Bounded in-memory LRU cache with correctness-oriented primitives."""

    def __init__(
        self,
        max_entries: int = 10_000,
        default_ttl_seconds: int = 300,
        default_stale_seconds: int = 900,
        lease_seconds: int = 30,
        clock: Callable[[], float] = time.monotonic,
    ) -> None:
        if min(
            max_entries,
            default_ttl_seconds,
            default_stale_seconds,
            lease_seconds,
        ) <= 0:
            raise ValueError("cache limits and durations must be greater than zero")
        self._max_entries = max_entries
        self._default_ttl = default_ttl_seconds
        self._default_stale = default_stale_seconds
        self._lease_seconds = lease_seconds
        self._clock = clock
        self._entries: "OrderedDict[str, _Entry]" = OrderedDict()
        self._tags: Dict[str, set] = defaultdict(set)
        self._leases: Dict[str, _Lease] = {}
        self._flights: Dict[str, _Flight] = {}
        self._metrics: Dict[str, int] = defaultdict(int)
        self._lock = threading.RLock()

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
        self._validate_key(key)
        if persistent:
            if ttl_seconds is not None or stale_seconds is not None:
                raise ValueError("persistent entries cannot have expiration windows")
            ttl = math.inf
            stale = 0
        else:
            ttl = self._duration(ttl_seconds, self._default_ttl, "ttl_seconds")
            stale = self._duration(
                stale_seconds,
                self._default_stale,
                "stale_seconds",
                allow_zero=True,
            )
        normalized_tags = frozenset(self._normalize_tags(tags))
        now = self._clock()

        with self._lock:
            if lease_token is not None:
                lease = self._leases.get(key)
                if lease is None or lease.until <= now:
                    self._leases.pop(key, None)
                    raise ValueError("lease is missing or expired")
                if not secrets.compare_digest(lease.token, lease_token):
                    raise ValueError("lease token does not match")

            self._remove_entry(key)
            self._entries[key] = _Entry(
                value=value,
                fresh_until=now + ttl,
                stale_until=now + ttl + stale,
                tags=normalized_tags,
            )
            for tag in normalized_tags:
                self._tags[tag].add(key)
            self._leases.pop(key, None)
            self._metrics["writes_total"] += 1
            self._evict_if_needed()
            return self._lookup(key, count=False)

    def mget(self, keys: Iterable[str]) -> Tuple[CacheResult, ...]:
        normalized = tuple(keys)
        with self._lock:
            return tuple(self.get(key) for key in normalized)

    def mset(self, values: Iterable[Tuple[str, Any]]) -> None:
        normalized = tuple(values)
        for key, _ in normalized:
            self._validate_key(key)
        with self._lock:
            for key, value in normalized:
                self.put(key, value, persistent=True)

    def delete(self, key: str) -> bool:
        self._validate_key(key)
        with self._lock:
            existed = self._remove_entry(key)
            self._leases.pop(key, None)
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
            self._metrics["flushes_total"] += 1
            return removed

    def size(self) -> int:
        with self._lock:
            for key in tuple(self._entries):
                self._lookup(key, count=False)
            return len(self._entries)

    def invalidate_tags(self, tags: Iterable[str]) -> int:
        normalized = self._normalize_tags(tags)
        with self._lock:
            keys = set()
            for tag in normalized:
                keys.update(self._tags.get(tag, set()))
            removed = sum(1 for key in keys if self._remove_entry(key))
            self._metrics["invalidations_total"] += removed
            return removed

    def acquire_lease(self, key: str) -> CacheResult:
        self._validate_key(key)
        now = self._clock()
        with self._lock:
            cached = self._lookup(key, count=True)
            if cached.state == "fresh":
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
            self._leases[key] = _Lease(token=token, until=now + self._lease_seconds)
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

    def stats(self) -> Dict[str, int]:
        with self._lock:
            snapshot = dict(self._metrics)
            snapshot["entries"] = self.size()
            snapshot["tags"] = len(self._tags)
            snapshot["active_leases"] = sum(
                1 for lease in self._leases.values() if lease.until > self._clock()
            )
            return snapshot

    def prometheus_metrics(self) -> str:
        stats = self.stats()
        lines = [
            "# HELP megacache_{} MegaCache metric.".format(name)
            for name in sorted(stats)
        ]
        values = [
            "# TYPE megacache_{} gauge\nmegacache_{} {}".format(name, name, value)
            for name, value in sorted(stats.items())
        ]
        return "\n".join(lines + values) + "\n"

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
                value=entry.value,
                expires_in_seconds=max(0, entry.fresh_until - now),
            )
        if count:
            self._metrics["stale_hits_total"] += 1
        return CacheResult(
            state="stale",
            value=entry.value,
            stale_for_seconds=max(0, entry.stale_until - now),
        )

    def _remove_entry(self, key: str) -> bool:
        entry = self._entries.pop(key, None)
        if entry is None:
            return False
        for tag in entry.tags:
            keys = self._tags.get(tag)
            if keys is not None:
                keys.discard(key)
                if not keys:
                    self._tags.pop(tag, None)
        return True

    def _evict_if_needed(self) -> None:
        while len(self._entries) > self._max_entries:
            key = next(iter(self._entries))
            self._remove_entry(key)
            self._metrics["evictions_total"] += 1

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
