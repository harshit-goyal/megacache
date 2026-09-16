"""Storage contracts shared by protocol adapters and cluster coordination."""

from __future__ import annotations

from dataclasses import dataclass
from typing import TYPE_CHECKING, Any, Callable, Iterable, Optional, Protocol, Tuple

if TYPE_CHECKING:
    from .engine import CacheResult


@dataclass(frozen=True)
class StorageEntry:
    """Portable representation of one cache entry."""

    key: str
    value: Any
    fresh_for_seconds: Optional[float]
    stale_for_seconds: Optional[float]
    tags: Tuple[str, ...]
    persistent: bool = False


class StorageBackend(Protocol):
    """Behavior required by HTTP, RESP, and distributed coordinators."""

    def get(self, key: str) -> CacheResult: ...

    def put(
        self,
        key: str,
        value: Any,
        ttl_seconds: Optional[int] = None,
        stale_seconds: Optional[int] = None,
        tags: Iterable[str] = (),
        lease_token: Optional[str] = None,
        persistent: bool = False,
    ) -> CacheResult: ...

    def mget(self, keys: Iterable[str]) -> Tuple[CacheResult, ...]: ...

    def mset(self, values: Iterable[Tuple[str, Any]]) -> None: ...

    def delete(self, key: str) -> bool: ...

    def delete_many(self, keys: Iterable[str]) -> int: ...

    def exists(self, keys: Iterable[str]) -> int: ...

    def expire(self, key: str, ttl_seconds: int) -> bool: ...

    def ttl(self, key: str) -> int: ...

    def flush(self) -> int: ...

    def size(self) -> int: ...

    def invalidate_tags(self, tags: Iterable[str]) -> int: ...

    def acquire_lease(self, key: str, force: bool = False) -> CacheResult: ...

    def release_lease(self, key: str, lease_token: str) -> bool: ...

    def renew_lease(self, key: str, lease_token: str) -> bool: ...

    def get_or_load(
        self,
        key: str,
        loader: Callable[[], Any],
        ttl_seconds: Optional[int] = None,
        stale_seconds: Optional[int] = None,
        tags: Iterable[str] = (),
    ) -> CacheResult: ...

    def coordinate(self, key: str, loader: Callable[[], Any]) -> Any: ...

    def stats(self) -> dict: ...

    def observe_request(
        self,
        protocol: str,
        operation: str,
        duration_seconds: float,
        success: bool,
    ) -> None: ...

    def prometheus_metrics(self) -> str: ...

    def keys(self) -> Tuple[str, ...]: ...

    def export_entries(
        self, keys: Optional[Iterable[str]] = None
    ) -> Tuple[StorageEntry, ...]: ...

    def restore_entries(self, entries: Iterable[StorageEntry]) -> int: ...
