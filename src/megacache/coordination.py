"""Client coordination helpers shared by protocol adapters."""

import threading
from typing import Any, Iterable, Tuple

from .storage import StorageBackend, StorageEntry


class MutationClock:
    """Storage facade exposing a monotonic L1 invalidation cursor.

    The cursor deliberately represents all mutations rather than retaining
    keys or values. SDKs poll it and clear their bounded L1 cache when it
    changes, so the mechanism remains bounded and never leaks cache metadata.
    """

    def __init__(self, storage: StorageBackend) -> None:
        self.storage = storage
        self._generation = 0
        self._generation_lock = threading.Lock()

    def invalidation_cursor(self) -> int:
        with self._generation_lock:
            return self._generation

    def _changed(self) -> None:
        with self._generation_lock:
            self._generation += 1

    def put(self, *args: Any, **kwargs: Any) -> Any:
        result = self.storage.put(*args, **kwargs)
        self._changed()
        return result

    def mset(self, values: Iterable[Tuple[str, Any]]) -> None:
        self.storage.mset(values)
        self._changed()

    def delete(self, *args: Any, **kwargs: Any) -> bool:
        deleted = self.storage.delete(*args, **kwargs)
        if deleted:
            self._changed()
        return deleted

    def delete_many(self, keys: Iterable[str]) -> int:
        deleted = self.storage.delete_many(keys)
        if deleted:
            self._changed()
        return deleted

    def expire(self, key: str, ttl_seconds: int) -> bool:
        changed = self.storage.expire(key, ttl_seconds)
        if changed:
            self._changed()
        return changed

    def flush(self) -> int:
        removed = self.storage.flush()
        if removed:
            self._changed()
        return removed

    def invalidate_tags(self, tags: Iterable[str]) -> int:
        removed = self.storage.invalidate_tags(tags)
        if removed:
            self._changed()
        return removed

    def restore_entries(self, entries: Iterable[StorageEntry]) -> int:
        restored = self.storage.restore_entries(entries)
        if restored:
            self._changed()
        return restored

    def __getattr__(self, name: str) -> Any:
        return getattr(self.storage, name)
