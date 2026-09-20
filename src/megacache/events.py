"""Durable, dependency-free freshness event ingestion."""

from __future__ import annotations

import fcntl
import hashlib
import hmac
import json
import logging
import math
import os
import re
import secrets
import stat
import threading
import time
from collections import Counter, defaultdict, deque
from contextlib import contextmanager
from dataclasses import asdict, dataclass, replace
from typing import (
    Any,
    Callable,
    Deque,
    Dict,
    Iterable,
    List,
    Mapping,
    Optional,
    Protocol,
    Sequence,
    Tuple,
)

from .storage import StorageBackend

LOG = logging.getLogger("megacache.events")
_IDENTIFIER = re.compile(r"^[A-Za-z0-9][A-Za-z0-9_.:-]{0,127}$")
_TEMPLATE_FIELD = re.compile(r"\{([A-Za-z0-9_.-]+)\}")


class EventError(ValueError):
    """Base error for invalid or unsupported events."""


class SchemaCompatibilityError(EventError):
    """An event cannot be read or migrated by the configured reader."""


class DependencyGraphError(EventError):
    """A dependency edge violates a graph safety bound."""


class WebhookAuthError(EventError):
    """A webhook signature, timestamp, or delivery identifier is invalid."""


class EventBackpressure(EventError):
    """Durable event state has reached a configured safety bound."""


class EventIngestionDisabled(EventError):
    """Event ingestion has no configured invalidation rules."""


def _encoded_size(value: Any) -> int:
    return len(
        json.dumps(
            value, sort_keys=True, separators=(",", ":"), allow_nan=False
        ).encode("utf-8")
    )


def _length_framed_key(*parts: str) -> str:
    encoded = []
    for part in parts:
        raw = part.encode("utf-8")
        encoded.append("{}:{}".format(len(raw), part))
    return "v2|" + "".join(encoded)


def webhook_signature_payload(
    source: str, timestamp: str, delivery: str, body: bytes
) -> bytes:
    """Return the unambiguous byte sequence covered by webhook HMAC."""
    if not isinstance(body, bytes):
        raise WebhookAuthError("webhook body must be bytes")
    pieces = []
    for value in (source, timestamp, delivery):
        if not isinstance(value, str):
            raise WebhookAuthError("webhook signature fields must be strings")
        raw = value.encode("utf-8")
        pieces.append(str(len(raw)).encode("ascii") + b":" + raw)
    pieces.append(str(len(body)).encode("ascii") + b":" + body)
    return b"megacache-webhook-v2|" + b"".join(pieces)


@dataclass(frozen=True)
class SchemaIdentifier:
    name: str
    version: int

    def __post_init__(self) -> None:
        _validate_identifier(self.name, "schema name")
        if (
            isinstance(self.version, bool)
            or not isinstance(self.version, int)
            or self.version <= 0
        ):
            raise EventError("schema version must be positive")

    @classmethod
    def parse(cls, value: str) -> "SchemaIdentifier":
        if not isinstance(value, str) or "@" not in value:
            raise EventError("schema_id must use name@version")
        name, raw_version = value.rsplit("@", 1)
        try:
            version = int(raw_version)
        except ValueError as exc:
            raise EventError("schema version must be an integer") from exc
        return cls(name, version)

    def __str__(self) -> str:
        return "{}@{}".format(self.name, self.version)


@dataclass(frozen=True)
class ReaderRange:
    name: str
    minimum_version: int
    maximum_version: int

    def __post_init__(self) -> None:
        _validate_identifier(self.name, "schema name")
        if (
            isinstance(self.minimum_version, bool)
            or not isinstance(self.minimum_version, int)
            or isinstance(self.maximum_version, bool)
            or not isinstance(self.maximum_version, int)
            or self.minimum_version <= 0
            or self.maximum_version < self.minimum_version
        ):
            raise EventError("schema reader range is invalid")

    def accepts(self, identifier: SchemaIdentifier) -> bool:
        return (
            identifier.name == self.name
            and self.minimum_version
            <= identifier.version
            <= self.maximum_version
        )


@dataclass(frozen=True)
class VersionedNamespace:
    """Rolling-deployment policy for version-qualified cache keys."""

    name: str
    write_version: int
    reader_minimum: int
    reader_maximum: int
    migration_policy: str = "rolling"

    def __post_init__(self) -> None:
        _validate_identifier(self.name, "namespace name")
        if (
            isinstance(self.write_version, bool)
            or not isinstance(self.write_version, int)
            or isinstance(self.reader_minimum, bool)
            or not isinstance(self.reader_minimum, int)
            or isinstance(self.reader_maximum, bool)
            or not isinstance(self.reader_maximum, int)
            or self.reader_minimum <= 0
            or self.reader_maximum < self.reader_minimum
            or not (
                self.reader_minimum
                <= self.write_version
                <= self.reader_maximum
            )
            or self.reader_maximum - self.reader_minimum >= 100
        ):
            raise EventError(
                "namespace version range is invalid or exceeds 100 versions"
            )
        if self.migration_policy not in (
            "strict",
            "rolling",
            "dual_write",
        ):
            raise EventError(
                "namespace migration_policy must be strict, rolling, or dual_write"
            )

    def qualify(self, key: str, version: Optional[int] = None) -> str:
        selected = self.write_version if version is None else version
        if selected <= 0:
            raise EventError("namespace version must be positive")
        return "{}:v{}:{}".format(self.name, selected, key)

    def write_keys(self, key: str) -> Tuple[str, ...]:
        if self.migration_policy == "dual_write":
            return tuple(
                self.qualify(key, version)
                for version in range(
                    self.reader_minimum, self.reader_maximum + 1
                )
            )
        return (self.qualify(key),)

    def invalidation_keys(self, key: str) -> Tuple[str, ...]:
        if self.migration_policy == "strict":
            return (self.qualify(key),)
        return tuple(
            self.qualify(key, version)
            for version in range(
                self.reader_minimum, self.reader_maximum + 1
            )
        )


@dataclass(frozen=True)
class ChangeEvent:
    event_id: str
    source: str
    stream: str
    position: int
    operation: str
    payload: Mapping[str, Any]
    timestamp: float
    schema_id: Optional[str] = None
    cursor: Optional[str] = None

    def __post_init__(self) -> None:
        if (
            not isinstance(self.event_id, str)
            or not self.event_id
            or len(self.event_id.encode("utf-8")) > 256
            or any(character.isspace() for character in self.event_id)
        ):
            raise EventError(
                "event_id must be a 1-256 character non-whitespace identifier"
            )
        _validate_identifier(self.source, "event source")
        _validate_identifier(self.stream, "event stream")
        if (
            isinstance(self.position, bool)
            or not isinstance(self.position, int)
            or self.position < 0
        ):
            raise EventError("event position must be a non-negative integer")
        if not isinstance(self.operation, str) or not self.operation:
            raise EventError("event operation must be a non-empty string")
        if not isinstance(self.payload, Mapping):
            raise EventError("event payload must be an object")
        try:
            json.dumps(self.payload, allow_nan=False)
        except (TypeError, ValueError) as exc:
            raise EventError("event payload must be JSON-compatible") from exc
        if (
            isinstance(self.timestamp, bool)
            or not isinstance(self.timestamp, (int, float))
            or not math.isfinite(self.timestamp)
        ):
            raise EventError("event timestamp must be finite and numeric")
        if self.schema_id is not None:
            SchemaIdentifier.parse(self.schema_id)
        if self.cursor is not None and not isinstance(self.cursor, str):
            raise EventError("event cursor must be a string")

    @property
    def checkpoint_key(self) -> str:
        return _length_framed_key(self.source, self.stream)

    @classmethod
    def from_dict(cls, value: Mapping[str, Any]) -> "ChangeEvent":
        if not isinstance(value, Mapping):
            raise EventError("event must be an object")
        required = (
            "event_id",
            "source",
            "stream",
            "position",
            "operation",
            "payload",
        )
        missing = [name for name in required if name not in value]
        if missing:
            raise EventError(
                "event is missing required fields: {}".format(
                    ", ".join(missing)
                )
            )
        timestamp = value.get("timestamp", time.time())
        return cls(
            event_id=value["event_id"],
            source=value["source"],
            stream=value["stream"],
            position=value["position"],
            operation=value["operation"],
            payload=value["payload"],
            timestamp=timestamp,
            schema_id=value.get("schema_id"),
            cursor=value.get("cursor"),
        )

    def as_dict(self) -> Dict[str, Any]:
        return dict(asdict(self))


@dataclass(frozen=True)
class EventResult:
    outcome: str
    event_id: str
    checkpoint: int
    invalidated_keys: int = 0
    invalidated_tags: int = 0
    graph_expanded: int = 0
    graph_truncated: bool = False
    error: Optional[str] = None

    def as_dict(self) -> Dict[str, Any]:
        return dict(asdict(self))


class SchemaRegistry:
    """Compatible reader ranges and explicit in-process migrations."""

    def __init__(self, readers: Iterable[ReaderRange] = ()) -> None:
        self._readers: Dict[str, List[ReaderRange]] = defaultdict(list)
        self._migrations: Dict[
            Tuple[str, int, int], Callable[[Mapping[str, Any]], Mapping[str, Any]]
        ] = {}
        for reader in readers:
            self.register_reader(reader)

    def register_reader(self, reader: ReaderRange) -> None:
        self._readers[reader.name].append(reader)

    def register_migration(
        self,
        schema_name: str,
        from_version: int,
        to_version: int,
        migrate: Callable[[Mapping[str, Any]], Mapping[str, Any]],
    ) -> None:
        _validate_identifier(schema_name, "schema name")
        if from_version <= 0 or to_version <= 0 or from_version == to_version:
            raise EventError("schema migration versions are invalid")
        if not callable(migrate):
            raise EventError("schema migration must be callable")
        self._migrations[(schema_name, from_version, to_version)] = migrate

    def prepare(self, event: ChangeEvent) -> ChangeEvent:
        if event.schema_id is None:
            return event
        identifier = SchemaIdentifier.parse(event.schema_id)
        readers = self._readers.get(identifier.name, ())
        if any(reader.accepts(identifier) for reader in readers):
            return event
        candidates = sorted(
            (
                target,
                migration,
            )
            for (name, source, target), migration in self._migrations.items()
            if name == identifier.name
            and source == identifier.version
            and any(
                reader.minimum_version
                <= target
                <= reader.maximum_version
                for reader in readers
            )
        )
        for target, migration in candidates:
            if migration is None:
                continue
            payload = migration(event.payload)
            if not isinstance(payload, Mapping):
                raise SchemaCompatibilityError(
                    "schema migration must return an object"
                )
            return replace(
                event,
                payload=dict(payload),
                schema_id=str(SchemaIdentifier(identifier.name, target)),
            )
        if not readers:
            raise SchemaCompatibilityError(
                "no reader is registered for schema '{}'".format(
                    identifier.name
                )
            )
        raise SchemaCompatibilityError(
            "schema {} is outside compatible reader ranges".format(identifier)
        )

    def status(self) -> Dict[str, Any]:
        return {
            name: [
                {
                    "minimum_version": reader.minimum_version,
                    "maximum_version": reader.maximum_version,
                }
                for reader in ranges
            ]
            for name, ranges in sorted(self._readers.items())
        }


class DependencyGraph:
    """A bounded directed dependency graph with cycle-safe traversal."""

    def __init__(
        self,
        max_nodes: int = 10_000,
        max_edges: int = 50_000,
        max_fanout: int = 100,
        max_depth: int = 16,
        max_invalidation_nodes: int = 10_000,
    ) -> None:
        for value in (
            max_nodes,
            max_edges,
            max_fanout,
            max_depth,
            max_invalidation_nodes,
        ):
            if value <= 0:
                raise DependencyGraphError("graph bounds must be positive")
        self.max_nodes = max_nodes
        self.max_edges = max_edges
        self.max_fanout = max_fanout
        self.max_depth = max_depth
        self.max_invalidation_nodes = max_invalidation_nodes
        self._edges: Dict[str, set] = defaultdict(set)
        self._nodes: set = set()
        self._edge_count = 0
        self._lock = threading.RLock()

    def add_dependency(self, dependency: str, dependent: str) -> None:
        _validate_cache_key(dependency)
        _validate_cache_key(dependent)
        if dependency == dependent:
            raise DependencyGraphError("dependency would create a cycle")
        with self._lock:
            if dependent in self._edges[dependency]:
                return
            new_nodes = {dependency, dependent} - self._nodes
            if len(self._nodes) + len(new_nodes) > self.max_nodes:
                raise DependencyGraphError("dependency graph node limit exceeded")
            if self._edge_count >= self.max_edges:
                raise DependencyGraphError("dependency graph edge limit exceeded")
            if len(self._edges[dependency]) >= self.max_fanout:
                raise DependencyGraphError(
                    "dependency graph fanout limit exceeded"
                )
            if self._reachable_locked(dependent, dependency):
                raise DependencyGraphError("dependency would create a cycle")
            self._nodes.update(new_nodes)
            self._edges[dependency].add(dependent)
            self._edge_count += 1

    def expand(self, seeds: Iterable[str]) -> Tuple[Tuple[str, ...], bool]:
        with self._lock:
            visited = set()
            queue: Deque[Tuple[str, int]] = deque(
                (seed, 0) for seed in seeds
            )
            while queue:
                key, depth = queue.popleft()
                if key in visited:
                    continue
                if len(visited) >= self.max_invalidation_nodes:
                    raise DependencyGraphError(
                        "dependency invalidation node limit exceeded"
                    )
                visited.add(key)
                children = sorted(self._edges.get(key, ()))
                if children and depth >= self.max_depth:
                    raise DependencyGraphError(
                        "dependency invalidation depth limit exceeded"
                    )
                for child in children:
                    if child not in visited:
                        queue.append((child, depth + 1))
            return tuple(sorted(visited)), False

    def status(self) -> Dict[str, int]:
        with self._lock:
            return {
                "nodes": len(self._nodes),
                "edges": self._edge_count,
                "max_nodes": self.max_nodes,
                "max_edges": self.max_edges,
                "max_fanout": self.max_fanout,
                "max_depth": self.max_depth,
                "max_invalidation_nodes": self.max_invalidation_nodes,
            }

    def _reachable_locked(self, start: str, target: str) -> bool:
        pending = [start]
        seen = set()
        while pending:
            node = pending.pop()
            if node == target:
                return True
            if node in seen:
                continue
            seen.add(node)
            pending.extend(self._edges.get(node, ()))
        return False


@dataclass(frozen=True)
class EventRule:
    name: str
    key_templates: Tuple[str, ...] = ()
    tag_templates: Tuple[str, ...] = ()
    sources: Tuple[str, ...] = ()
    streams: Tuple[str, ...] = ()
    operations: Tuple[str, ...] = ()
    schema_names: Tuple[str, ...] = ()
    namespace: Optional[str] = None

    def __post_init__(self) -> None:
        _validate_identifier(self.name, "rule name")
        if not self.key_templates and not self.tag_templates:
            raise EventError("event rule must produce a key or tag")
        for template in self.key_templates + self.tag_templates:
            _validate_template(template)
        for value in (
            self.sources
            + self.streams
            + self.operations
            + self.schema_names
        ):
            if not isinstance(value, str) or not value:
                raise EventError("event rule filters must be non-empty strings")

    def matches(self, event: ChangeEvent) -> bool:
        schema_name = (
            None
            if event.schema_id is None
            else SchemaIdentifier.parse(event.schema_id).name
        )
        return (
            (not self.sources or event.source in self.sources)
            and (not self.streams or event.stream in self.streams)
            and (not self.operations or event.operation in self.operations)
            and (
                not self.schema_names
                or schema_name in self.schema_names
            )
        )

    @classmethod
    def from_dict(cls, value: Mapping[str, Any]) -> "EventRule":
        if not isinstance(value, Mapping):
            raise EventError("event rule must be an object")
        return cls(
            name=value.get("name"),
            key_templates=_string_tuple(value.get("keys", ()), "rule keys"),
            tag_templates=_string_tuple(value.get("tags", ()), "rule tags"),
            sources=_string_tuple(value.get("sources", ()), "rule sources"),
            streams=_string_tuple(value.get("streams", ()), "rule streams"),
            operations=_string_tuple(
                value.get("operations", ()), "rule operations"
            ),
            schema_names=_string_tuple(
                value.get("schema_names", ()), "rule schema_names"
            ),
            namespace=value.get("namespace"),
        )

    def render(self, event: ChangeEvent) -> Tuple[Tuple[str, ...], Tuple[str, ...]]:
        context = {
            "event_id": event.event_id,
            "source": event.source,
            "stream": event.stream,
            "position": event.position,
            "operation": event.operation,
            "schema_id": event.schema_id or "",
            "payload": event.payload,
        }
        keys = tuple(_render_template(value, context) for value in self.key_templates)
        tags = tuple(_render_template(value, context) for value in self.tag_templates)
        for key in keys:
            _validate_cache_key(key)
        for tag in tags:
            if not tag or len(tag.encode("utf-8")) > 1024:
                raise EventError("rendered tag is empty or too long")
        return keys, tags


class AtomicCheckpointStore:
    """Crash-safe event checkpoints, replay claims, deduplication, and DLQ."""

    def __init__(
        self,
        path: str,
        max_seen_events: int = 10_000,
        max_replay_tokens: int = 10_000,
        max_dead_letters: int = 1_000,
        max_dead_letter_bytes: int = 8_388_608,
        max_streams: int = 1_000,
        max_state_bytes: int = 16_777_216,
        max_payload_bytes: int = 1_048_576,
        max_cursor_bytes: int = 4_096,
        max_error_bytes: int = 4_096,
    ) -> None:
        if not isinstance(path, str) or not path:
            raise EventError("event state path must be non-empty")
        if min(
            max_seen_events,
            max_replay_tokens,
            max_dead_letters,
            max_dead_letter_bytes,
            max_streams,
            max_state_bytes,
            max_payload_bytes,
            max_cursor_bytes,
            max_error_bytes,
        ) <= 0:
            raise EventError("event state bounds must be positive")
        self.path = os.path.abspath(path)
        self.lock_path = self.path + ".lock"
        self.max_seen_events = max_seen_events
        self.max_replay_tokens = max_replay_tokens
        self.max_dead_letters = max_dead_letters
        self.max_dead_letter_bytes = max_dead_letter_bytes
        self.max_streams = max_streams
        self.max_state_bytes = max_state_bytes
        self.max_payload_bytes = max_payload_bytes
        self.max_cursor_bytes = max_cursor_bytes
        self.max_error_bytes = max_error_bytes
        self._lock = threading.RLock()
        self._lock_descriptor: Optional[int] = None
        self._closed = False
        self._acquire_file_lock()
        try:
            self._state, changed = self._load()
            if changed:
                self._persist_locked(self._state)
        except Exception:
            self.close()
            raise

    @contextmanager
    def transaction(self) -> Iterable[None]:
        with self._lock:
            self._ensure_open()
            yield

    def close(self) -> None:
        with self._lock:
            if self._closed:
                return
            self._closed = True
            descriptor = self._lock_descriptor
            self._lock_descriptor = None
            if descriptor is not None:
                try:
                    fcntl.flock(descriptor, fcntl.LOCK_UN)
                finally:
                    os.close(descriptor)

    def __enter__(self) -> "AtomicCheckpointStore":
        return self

    def __exit__(self, *unused: Any) -> None:
        self.close()

    def __del__(self) -> None:
        try:
            self.close()
        except Exception:
            pass

    def checkpoint(self, stream: str) -> Optional[Dict[str, Any]]:
        with self._lock:
            self._ensure_open()
            value = self._state["checkpoints"].get(stream)
            return None if value is None else dict(value)

    def is_seen(self, event_id: str) -> bool:
        with self._lock:
            self._ensure_open()
            return event_id in self._state["seen"]

    def validate_event(self, event: ChangeEvent) -> None:
        if _encoded_size(event.payload) > self.max_payload_bytes:
            raise EventError("event payload exceeds configured byte limit")
        if (
            event.cursor is not None
            and len(event.cursor.encode("utf-8")) > self.max_cursor_bytes
        ):
            raise EventError("event cursor exceeds configured byte limit")

    def ensure_complete_capacity(self, event: ChangeEvent) -> None:
        with self._lock:
            self._ensure_open()
            self.validate_event(event)
            candidate = self._copy_state()
            self._advance_checkpoint(candidate, event)
            self._remember_seen(candidate, event.event_id)
            self._validate_state_limits(candidate)

    def complete(self, event: ChangeEvent) -> None:
        def mutation(state: Dict[str, Any]) -> None:
            self.validate_event(event)
            self._advance_checkpoint(state, event)
            self._remember_seen(state, event.event_id)

        self._mutate(mutation)

    def dead_letter(
        self,
        event: ChangeEvent,
        error: str,
        retryable: bool = True,
        now: Optional[float] = None,
    ) -> None:
        failed_at = time.time() if now is None else now
        bounded_error = self._bounded_error(error)

        def mutation(state: Dict[str, Any]) -> None:
            self.validate_event(event)
            letters = state["dead_letters"]
            existing = next(
                (
                    item
                    for item in letters
                    if item["event"]["event_id"] == event.event_id
                ),
                None,
            )
            if existing is None:
                letters.append(
                    {
                        "event": event.as_dict(),
                        "error": bounded_error,
                        "attempts": 1,
                        "first_failed_at": failed_at,
                        "last_failed_at": failed_at,
                        "retry_after": failed_at + 1,
                        "retryable": bool(retryable),
                    }
                )
            else:
                existing["error"] = bounded_error
                existing["attempts"] += 1
                existing["last_failed_at"] = failed_at
                existing["retry_after"] = failed_at + min(
                    3600, 2 ** min(existing["attempts"] - 1, 12)
                )
                existing["retryable"] = bool(retryable)
            self._validate_dead_letters(letters)
            self._advance_checkpoint(state, event)
            self._remember_seen(state, event.event_id)

        self._mutate(mutation)

    def update_dead_letter(
        self, event_id: str, error: str, now: Optional[float] = None
    ) -> None:
        failed_at = time.time() if now is None else now
        bounded_error = self._bounded_error(error)

        def mutation(state: Dict[str, Any]) -> None:
            for item in state["dead_letters"]:
                if item["event"]["event_id"] == event_id:
                    item["error"] = bounded_error
                    item["attempts"] += 1
                    item["last_failed_at"] = failed_at
                    item["retry_after"] = failed_at + min(
                        3600, 2 ** min(item["attempts"] - 1, 12)
                    )
                    self._validate_dead_letters(state["dead_letters"])
                    return

        self._mutate(mutation)

    def remove_dead_letter(self, event_id: str) -> None:
        def mutation(state: Dict[str, Any]) -> None:
            state["dead_letters"] = [
                item
                for item in state["dead_letters"]
                if item["event"]["event_id"] != event_id
            ]

        self._mutate(mutation)

    def dead_letters(self) -> Tuple[Dict[str, Any], ...]:
        with self._lock:
            self._ensure_open()
            return tuple(
                json.loads(json.dumps(item))
                for item in self._state["dead_letters"]
            )

    def claim_replay_token(
        self, token: str, expires_at: float, now: Optional[float] = None
    ) -> bool:
        current = time.time() if now is None else now
        claimed = [False]

        def mutation(state: Dict[str, Any]) -> None:
            replay = state["replay_tokens"]
            replay[:] = [
                item for item in replay if item["expires_at"] > current
            ]
            if any(item["key"] == token for item in replay):
                return
            if len(replay) >= self.max_replay_tokens:
                raise EventBackpressure(
                    "webhook replay claim capacity is exhausted"
                )
            replay.append({"key": token, "expires_at": expires_at})
            claimed[0] = True

        self._mutate(mutation)
        return claimed[0]

    def snapshot(self) -> Dict[str, Any]:
        with self._lock:
            self._ensure_open()
            return {
                "state_version": self._state["version"],
                "checkpoints": json.loads(
                    json.dumps(self._state["checkpoints"])
                ),
                "seen_events": len(self._state["seen"]),
                "replay_tokens": len(self._state["replay_tokens"]),
                "dead_letter_depth": len(self._state["dead_letters"]),
                "dead_letter_dropped": self._state[
                    "dead_letter_dropped"
                ],
                "dead_letter_bytes": self._dead_letter_bytes(
                    self._state["dead_letters"]
                ),
                "state_bytes": len(self._serialize(self._state)),
                "state_limits": {
                    "max_seen_events": self.max_seen_events,
                    "max_replay_tokens": self.max_replay_tokens,
                    "max_dead_letters": self.max_dead_letters,
                    "max_dead_letter_bytes": self.max_dead_letter_bytes,
                    "max_streams": self.max_streams,
                    "max_state_bytes": self.max_state_bytes,
                    "max_payload_bytes": self.max_payload_bytes,
                    "max_cursor_bytes": self.max_cursor_bytes,
                    "max_error_bytes": self.max_error_bytes,
                },
                "path": self.path,
            }

    def _acquire_file_lock(self) -> None:
        directory = os.path.dirname(self.path)
        if directory:
            os.makedirs(directory, mode=0o700, exist_ok=True)
        descriptor = os.open(
            self.lock_path,
            os.O_RDWR
            | os.O_CREAT
            | getattr(os, "O_CLOEXEC", 0)
            | getattr(os, "O_NOFOLLOW", 0),
            0o600,
        )
        os.fchmod(descriptor, 0o600)
        try:
            fcntl.flock(
                descriptor, fcntl.LOCK_EX | fcntl.LOCK_NB
            )
        except (BlockingIOError, OSError) as exc:
            os.close(descriptor)
            raise EventError(
                "event state file is already owned by another process"
            ) from exc
        self._lock_descriptor = descriptor

    def _load(self) -> Tuple[Dict[str, Any], bool]:
        try:
            metadata = os.lstat(self.path)
            if stat.S_ISLNK(metadata.st_mode) or not stat.S_ISREG(
                metadata.st_mode
            ):
                raise EventError("event state must be a regular file")
            if metadata.st_size > self.max_state_bytes:
                raise EventBackpressure(
                    "existing event state exceeds configured byte limit"
                )
            descriptor = os.open(
                self.path,
                os.O_RDONLY
                | getattr(os, "O_CLOEXEC", 0)
                | getattr(os, "O_NOFOLLOW", 0),
            )
            with os.fdopen(descriptor, "r", encoding="utf-8") as source:
                state = json.load(source)
        except FileNotFoundError:
            return self._empty_state(), False
        except EventBackpressure:
            raise
        except (OSError, UnicodeDecodeError, json.JSONDecodeError) as exc:
            raise EventError(
                "unable to load event state '{}': {}".format(self.path, exc)
            ) from exc
        changed = False
        if isinstance(state, dict) and state.get("version") == 1:
            state = self._migrate_v1(state)
            changed = True
        if (
            not isinstance(state, dict)
            or state.get("version") != 2
            or not isinstance(state.get("checkpoints"), dict)
            or not isinstance(state.get("seen"), list)
            or not isinstance(state.get("replay_tokens"), list)
            or not isinstance(state.get("dead_letters"), list)
        ):
            raise EventError("event state file has an unsupported format")
        replay = [
            item
            for item in state["replay_tokens"]
            if isinstance(item, dict)
            and isinstance(item.get("expires_at"), (int, float))
            and item["expires_at"] > time.time()
        ]
        if len(replay) != len(state["replay_tokens"]):
            state["replay_tokens"] = replay
            changed = True
        state.setdefault("dead_letter_dropped", 0)
        self._validate_state_limits(state)
        return state, changed

    def _migrate_v1(self, state: Dict[str, Any]) -> Dict[str, Any]:
        checkpoints = state.get("checkpoints")
        replay_tokens = state.get("replay_tokens")
        if not isinstance(checkpoints, dict) or not isinstance(
            replay_tokens, list
        ):
            raise EventError("event state v1 cannot be migrated")
        migrated_checkpoints = {}
        for key, value in checkpoints.items():
            if not isinstance(key, str) or key.count(":") != 1:
                raise EventError(
                    "event state v1 contains an ambiguous checkpoint key"
                )
            if not isinstance(value, Mapping):
                raise EventError("event state v1 contains an invalid checkpoint")
            source, stream = key.split(":", 1)
            migrated_value = dict(value)
            migrated_value["source"] = source
            migrated_value["stream"] = stream
            migrated_checkpoints[
                _length_framed_key(source, stream)
            ] = migrated_value
        migrated_replay = []
        for item in replay_tokens:
            token = item.get("token") if isinstance(item, dict) else None
            if not isinstance(token, str) or token.count(":") != 1:
                raise EventError(
                    "event state v1 contains an ambiguous replay key"
                )
            source, delivery = token.split(":", 1)
            migrated_replay.append(
                {
                    "key": _length_framed_key(source, delivery),
                    "expires_at": item.get("expires_at"),
                }
            )
        return {
            "version": 2,
            "checkpoints": migrated_checkpoints,
            "seen": state.get("seen", []),
            "replay_tokens": migrated_replay,
            "dead_letters": state.get("dead_letters", []),
            "dead_letter_dropped": state.get("dead_letter_dropped", 0),
        }

    @staticmethod
    def _empty_state() -> Dict[str, Any]:
        return {
            "version": 2,
            "checkpoints": {},
            "seen": [],
            "replay_tokens": [],
            "dead_letters": [],
            "dead_letter_dropped": 0,
        }

    def _mutate(self, mutation: Callable[[Dict[str, Any]], None]) -> None:
        with self._lock:
            self._ensure_open()
            candidate = self._copy_state()
            mutation(candidate)
            self._validate_state_limits(candidate)
            self._persist_locked(candidate)
            self._state = candidate

    def _persist_locked(self, state: Dict[str, Any]) -> None:
        directory = os.path.dirname(self.path)
        if directory:
            os.makedirs(directory, mode=0o700, exist_ok=True)
        temporary = "{}.new-{}".format(self.path, secrets.token_hex(8))
        payload = self._serialize(state)
        if len(payload) > self.max_state_bytes:
            raise EventBackpressure(
                "event state exceeds configured byte limit"
            )
        descriptor = os.open(
            temporary,
            os.O_WRONLY
            | os.O_CREAT
            | os.O_EXCL
            | getattr(os, "O_CLOEXEC", 0)
            | getattr(os, "O_NOFOLLOW", 0),
            0o600,
        )
        try:
            with os.fdopen(descriptor, "wb") as destination:
                destination.write(payload)
                destination.flush()
                os.fsync(destination.fileno())
            os.replace(temporary, self.path)
            try:
                directory_descriptor = os.open(directory or ".", os.O_RDONLY)
                try:
                    os.fsync(directory_descriptor)
                finally:
                    os.close(directory_descriptor)
            except OSError:
                pass
        finally:
            try:
                os.unlink(temporary)
            except FileNotFoundError:
                pass

    def _advance_checkpoint(
        self, state: Dict[str, Any], event: ChangeEvent
    ) -> None:
        current = state["checkpoints"].get(event.checkpoint_key)
        if current is None and len(state["checkpoints"]) >= self.max_streams:
            raise EventBackpressure(
                "event stream capacity is exhausted"
            )
        if current is not None and event.position < current["position"]:
            raise EventError("checkpoint cannot move backwards")
        if current is None or event.position >= current["position"]:
            state["checkpoints"][event.checkpoint_key] = {
                "source": event.source,
                "stream": event.stream,
                "position": event.position,
                "cursor": event.cursor,
                "event_id": event.event_id,
                "committed_at": time.time(),
            }

    def _remember_seen(self, state: Dict[str, Any], event_id: str) -> None:
        seen = state["seen"]
        try:
            seen.remove(event_id)
        except ValueError:
            pass
        seen.append(event_id)
        del seen[:-self.max_seen_events]

    def _validate_state_limits(self, state: Dict[str, Any]) -> None:
        if len(state["checkpoints"]) > self.max_streams:
            raise EventBackpressure(
                "existing event streams exceed configured limit"
            )
        for checkpoint in state["checkpoints"].values():
            if not isinstance(checkpoint, dict):
                raise EventError("event state contains an invalid checkpoint")
            cursor = checkpoint.get("cursor")
            if cursor is not None and not isinstance(cursor, str):
                raise EventError("event state contains an invalid cursor")
            if (
                cursor is not None
                and len(cursor.encode("utf-8")) > self.max_cursor_bytes
            ):
                raise EventBackpressure(
                    "existing event cursor exceeds configured byte limit"
                )
        if len(state["seen"]) > self.max_seen_events:
            raise EventBackpressure(
                "existing seen-event history exceeds configured limit"
            )
        if len(state["replay_tokens"]) > self.max_replay_tokens:
            raise EventBackpressure(
                "existing replay claims exceed configured limit"
            )
        for item in state["replay_tokens"]:
            if (
                not isinstance(item, dict)
                or not isinstance(item.get("key"), str)
                or not isinstance(item.get("expires_at"), (int, float))
            ):
                raise EventError("event state contains an invalid replay claim")
        self._validate_dead_letters(state["dead_letters"])
        if len(self._serialize(state)) > self.max_state_bytes:
            raise EventBackpressure(
                "event state exceeds configured byte limit"
            )

    def _validate_dead_letters(
        self, letters: Sequence[Mapping[str, Any]]
    ) -> None:
        if len(letters) > self.max_dead_letters:
            raise EventBackpressure(
                "dead-letter count capacity is exhausted"
            )
        if self._dead_letter_bytes(letters) > self.max_dead_letter_bytes:
            raise EventBackpressure(
                "dead-letter byte capacity is exhausted"
            )
        for item in letters:
            if not isinstance(item, Mapping):
                raise EventError("event state contains an invalid dead letter")
            event_value = item.get("event")
            error = item.get("error")
            if not isinstance(event_value, Mapping) or not isinstance(
                error, str
            ):
                raise EventError("event state contains an invalid dead letter")
            event = ChangeEvent.from_dict(event_value)
            if _encoded_size(event.payload) > self.max_payload_bytes:
                raise EventBackpressure(
                    "existing DLQ payload exceeds configured byte limit"
                )
            if (
                event.cursor is not None
                and len(event.cursor.encode("utf-8"))
                > self.max_cursor_bytes
            ):
                raise EventBackpressure(
                    "existing DLQ cursor exceeds configured byte limit"
                )
            if len(error.encode("utf-8")) > self.max_error_bytes:
                raise EventBackpressure(
                    "existing DLQ error exceeds configured byte limit"
                )

    @staticmethod
    def _dead_letter_bytes(
        letters: Sequence[Mapping[str, Any]]
    ) -> int:
        return sum(_encoded_size(item) for item in letters)

    def _bounded_error(self, error: str) -> str:
        value = str(error)
        raw = value.encode("utf-8")
        if len(raw) <= self.max_error_bytes:
            return value
        if self.max_error_bytes <= 3:
            return raw[: self.max_error_bytes].decode("utf-8", "ignore")
        suffix = b"..."
        return raw[: self.max_error_bytes - len(suffix)].decode(
            "utf-8", "ignore"
        ) + suffix.decode("ascii")

    @staticmethod
    def _serialize(state: Mapping[str, Any]) -> bytes:
        return json.dumps(
            state, sort_keys=True, separators=(",", ":"), allow_nan=False
        ).encode("utf-8")

    def _copy_state(self) -> Dict[str, Any]:
        return json.loads(self._serialize(self._state).decode("utf-8"))

    def _ensure_open(self) -> None:
        if self._closed:
            raise EventError("event state store is closed")


class EventIngestor:
    """Transforms ordered change events into bounded cache invalidations."""

    def __init__(
        self,
        storage: StorageBackend,
        checkpoint_store: AtomicCheckpointStore,
        rules: Iterable[EventRule] = (),
        graph: Optional[DependencyGraph] = None,
        schemas: Optional[SchemaRegistry] = None,
        namespaces: Iterable[VersionedNamespace] = (),
    ) -> None:
        self.storage = storage
        self.checkpoints = checkpoint_store
        self.rules = tuple(rules)
        self.graph = graph or DependencyGraph()
        self.schemas = schemas or SchemaRegistry()
        namespace_values = tuple(namespaces)
        self.namespaces = {item.name: item for item in namespace_values}
        if len(self.namespaces) != len(namespace_values):
            raise EventError("duplicate namespace name")
        for rule in self.rules:
            if rule.namespace is not None and rule.namespace not in self.namespaces:
                raise EventError(
                    "rule '{}' references unknown namespace '{}'".format(
                        rule.name, rule.namespace
                    )
                )
        self._metrics: Counter = Counter()
        self._lock = threading.RLock()

    @property
    def enabled(self) -> bool:
        return bool(self.rules)

    def ingest(self, event: ChangeEvent) -> EventResult:
        with self._lock:
            if not self.enabled:
                raise EventIngestionDisabled(
                    "event ingestion is disabled because no rules are configured"
                )
            with self.checkpoints.transaction():
                self.checkpoints.validate_event(event)
                if self.checkpoints.is_seen(event.event_id):
                    self._metrics["duplicate_total"] += 1
                    return self._result("duplicate", event)
                checkpoint = self.checkpoints.checkpoint(
                    event.checkpoint_key
                )
                if (
                    checkpoint is not None
                    and event.position <= checkpoint["position"]
                ):
                    self._metrics["replayed_total"] += 1
                    return self._result("replayed", event)
                self.checkpoints.ensure_complete_capacity(event)
                try:
                    prepared = self.schemas.prepare(event)
                    result = self._apply(prepared)
                except DependencyGraphError:
                    self._metrics["graph_rejected_total"] += 1
                    raise
                except EventBackpressure:
                    raise
                except Exception as exc:
                    self.checkpoints.dead_letter(event, str(exc))
                    self._metrics["dead_lettered_total"] += 1
                    LOG.warning(
                        "event dead-lettered",
                        extra={
                            "event_source": event.source,
                            "event_stream": event.stream,
                            "event_outcome": "dead_letter",
                        },
                    )
                    return self._result(
                        "dead_letter", event, error=str(exc)
                    )
                self.checkpoints.complete(event)
            self._metrics["processed_total"] += 1
            self._metrics["invalidated_keys_total"] += result.invalidated_keys
            self._metrics["invalidated_tags_total"] += result.invalidated_tags
            if result.graph_truncated:
                self._metrics["graph_truncated_total"] += 1
            LOG.info(
                "event processed",
                extra={
                    "event_source": event.source,
                    "event_stream": event.stream,
                    "event_outcome": "processed",
                },
            )
            return result

    def retry_dead_letters(
        self, limit: int = 100, now: Optional[float] = None
    ) -> Dict[str, int]:
        if limit <= 0:
            raise EventError("retry limit must be positive")
        current = time.time() if now is None else now
        retried = 0
        succeeded = 0
        failed = 0
        with self._lock:
            if not self.enabled:
                raise EventIngestionDisabled(
                    "event ingestion is disabled because no rules are configured"
                )
            for item in self.checkpoints.dead_letters():
                if retried >= limit:
                    break
                if not item["retryable"] or item["retry_after"] > current:
                    continue
                retried += 1
                event = ChangeEvent.from_dict(item["event"])
                try:
                    prepared = self.schemas.prepare(event)
                    self._apply(prepared)
                except Exception as exc:
                    failed += 1
                    self.checkpoints.update_dead_letter(
                        event.event_id, str(exc), current
                    )
                else:
                    succeeded += 1
                    self.checkpoints.remove_dead_letter(event.event_id)
                    self._metrics["retried_total"] += 1
        return {"retried": retried, "succeeded": succeeded, "failed": failed}

    def status(self) -> Dict[str, Any]:
        with self._lock:
            snapshot = self.checkpoints.snapshot()
            snapshot.update(
                {
                    "enabled": self.enabled,
                    "rules": len(self.rules),
                    "namespaces": {
                        name: {
                            "write_version": policy.write_version,
                            "reader_minimum": policy.reader_minimum,
                            "reader_maximum": policy.reader_maximum,
                            "migration_policy": policy.migration_policy,
                        }
                        for name, policy in sorted(self.namespaces.items())
                    },
                    "schemas": self.schemas.status(),
                    "graph": self.graph.status(),
                    "metrics": dict(self._metrics),
                }
            )
            return snapshot

    def close(self) -> None:
        self.checkpoints.close()

    def metrics(self) -> Dict[str, int]:
        with self._lock:
            values = dict(self._metrics)
            state = self.checkpoints.snapshot()
            values["dead_letter_depth"] = state["dead_letter_depth"]
            values["dead_letter_dropped_total"] = state[
                "dead_letter_dropped"
            ]
            return values

    def _apply(self, event: ChangeEvent) -> EventResult:
        keys = set()
        tags = set()
        for rule in self.rules:
            if not rule.matches(event):
                continue
            rendered_keys, rendered_tags = rule.render(event)
            if rule.namespace is None:
                keys.update(rendered_keys)
            else:
                namespace = self.namespaces[rule.namespace]
                for key in rendered_keys:
                    keys.update(namespace.invalidation_keys(key))
            tags.update(rendered_tags)
        expanded, truncated = self.graph.expand(keys)
        invalidated_keys = (
            self.storage.delete_many(expanded) if expanded else 0
        )
        invalidated_tags = (
            self.storage.invalidate_tags(sorted(tags)) if tags else 0
        )
        return EventResult(
            outcome="processed",
            event_id=event.event_id,
            checkpoint=event.position,
            invalidated_keys=invalidated_keys,
            invalidated_tags=invalidated_tags,
            graph_expanded=max(0, len(expanded) - len(keys)),
            graph_truncated=truncated,
        )

    def _result(
        self,
        outcome: str,
        event: ChangeEvent,
        error: Optional[str] = None,
    ) -> EventResult:
        checkpoint = self.checkpoints.checkpoint(event.checkpoint_key)
        return EventResult(
            outcome=outcome,
            event_id=event.event_id,
            checkpoint=(
                event.position
                if checkpoint is None
                else checkpoint["position"]
            ),
            error=error,
        )


@dataclass(frozen=True)
class WebhookSource:
    name: str
    secret: bytes
    tolerance_seconds: int = 300

    def __post_init__(self) -> None:
        _validate_identifier(self.name, "webhook name")
        if not isinstance(self.secret, bytes) or len(self.secret) < 16:
            raise EventError("webhook secret must contain at least 16 bytes")
        if (
            isinstance(self.tolerance_seconds, bool)
            or not isinstance(self.tolerance_seconds, int)
            or self.tolerance_seconds <= 0
        ):
            raise EventError("webhook tolerance must be positive")


class WebhookAuthenticator:
    def __init__(
        self,
        checkpoint_store: AtomicCheckpointStore,
        sources: Iterable[WebhookSource] = (),
        clock: Callable[[], float] = time.time,
    ) -> None:
        self._store = checkpoint_store
        source_values = tuple(sources)
        self._sources = {source.name: source for source in source_values}
        if len(self._sources) != len(source_values):
            raise EventError("duplicate webhook source")
        self._clock = clock

    def authenticate(
        self, source_name: str, headers: Mapping[str, str], body: bytes
    ) -> None:
        source = self._sources.get(source_name)
        if source is None:
            raise WebhookAuthError("unknown webhook source")
        raw_timestamp = headers.get("X-MegaCache-Timestamp")
        supplied = headers.get("X-MegaCache-Signature")
        delivery = headers.get("X-MegaCache-Delivery")
        if raw_timestamp is None or supplied is None or delivery is None:
            raise WebhookAuthError(
                "webhook timestamp, signature, and delivery are required"
            )
        _validate_identifier(delivery, "webhook delivery")
        try:
            timestamp = int(raw_timestamp)
        except ValueError as exc:
            raise WebhookAuthError(
                "webhook timestamp must be an integer"
            ) from exc
        now = self._clock()
        if abs(now - timestamp) > source.tolerance_seconds:
            raise WebhookAuthError("webhook timestamp is outside tolerance")
        signed = webhook_signature_payload(
            source_name, raw_timestamp, delivery, body
        )
        expected = "sha256=" + hmac.new(
            source.secret, signed, hashlib.sha256
        ).hexdigest()
        if not hmac.compare_digest(supplied, expected):
            raise WebhookAuthError("webhook signature is invalid")
        token = _length_framed_key(source_name, delivery)
        if not self._store.claim_replay_token(
            token, now + source.tolerance_seconds, now
        ):
            raise WebhookAuthError("webhook delivery was already received")

    def configured_sources(self) -> Tuple[str, ...]:
        return tuple(sorted(self._sources))


class EventAutomation:
    """Storage facade adding event ingestion without changing storage APIs."""

    def __init__(
        self,
        storage: StorageBackend,
        ingestor: EventIngestor,
        webhooks: Optional[WebhookAuthenticator] = None,
    ) -> None:
        self.storage = storage
        self.ingestor = ingestor
        self.webhooks = webhooks or WebhookAuthenticator(
            ingestor.checkpoints
        )

    def events_enabled(self) -> bool:
        return self.ingestor.enabled

    def ingest_event(self, value: Mapping[str, Any]) -> Dict[str, Any]:
        if not self.ingestor.enabled:
            raise EventIngestionDisabled(
                "event ingestion is disabled because no rules are configured"
            )
        return self.ingestor.ingest(ChangeEvent.from_dict(value)).as_dict()

    def ingest_webhook(
        self,
        source_name: str,
        headers: Mapping[str, str],
        body: bytes,
    ) -> Dict[str, Any]:
        if not self.ingestor.enabled:
            raise EventIngestionDisabled(
                "event ingestion is disabled because no rules are configured"
            )
        self.webhooks.authenticate(source_name, headers, body)
        try:
            value = json.loads(body.decode("utf-8"))
        except (UnicodeDecodeError, json.JSONDecodeError) as exc:
            raise EventError("webhook body must be valid JSON") from exc
        if not isinstance(value, dict):
            raise EventError("webhook body must be a JSON object")
        supplied_source = value.setdefault("source", source_name)
        if supplied_source != source_name:
            raise EventError(
                "webhook event source must match the configured webhook"
            )
        return self.ingest_event(value)

    def retry_events(self, limit: int = 100) -> Dict[str, int]:
        if not self.ingestor.enabled:
            raise EventIngestionDisabled(
                "event ingestion is disabled because no rules are configured"
            )
        return self.ingestor.retry_dead_letters(limit)

    def event_status(self) -> Dict[str, Any]:
        status = self.ingestor.status()
        status["webhooks"] = self.webhooks.configured_sources()
        return status

    def stats(self) -> Dict[str, int]:
        values = dict(self.storage.stats())
        for name, value in self.ingestor.metrics().items():
            values["event_{}".format(name)] = value
        return values

    def status(self) -> Dict[str, Any]:
        status_method = getattr(self.storage, "status", None)
        value = (
            {
                "healthy_nodes": 1,
                "total_nodes": 1,
                "degraded": False,
                "known_keys": self.storage.size(),
            }
            if status_method is None
            else dict(status_method())
        )
        value["events"] = self.event_status()
        return value

    def prometheus_metrics(self) -> str:
        lines = [self.storage.prometheus_metrics().rstrip("\n")]
        metrics = self.ingestor.metrics()
        lines.extend(
            [
                "# HELP megacache_events_total Freshness events by outcome.",
                "# TYPE megacache_events_total counter",
            ]
        )
        for outcome in (
            "processed",
            "duplicate",
            "replayed",
            "dead_lettered",
            "retried",
        ):
            lines.append(
                'megacache_events_total{{outcome="{}"}} {}'.format(
                    outcome, metrics.get("{}_total".format(outcome), 0)
                )
            )
        lines.extend(
            [
                "# HELP megacache_event_dead_letter_depth Pending dead letters.",
                "# TYPE megacache_event_dead_letter_depth gauge",
                "megacache_event_dead_letter_depth {}".format(
                    metrics["dead_letter_depth"]
                ),
                "# HELP megacache_event_dead_letter_dropped_total Dead letters evicted at the configured bound.",
                "# TYPE megacache_event_dead_letter_dropped_total counter",
                "megacache_event_dead_letter_dropped_total {}".format(
                    metrics["dead_letter_dropped_total"]
                ),
                "# HELP megacache_event_graph_truncated_total Bounded graph traversals that truncated.",
                "# TYPE megacache_event_graph_truncated_total counter",
                "megacache_event_graph_truncated_total {}".format(
                    metrics.get("graph_truncated_total", 0)
                ),
                "# HELP megacache_event_graph_rejected_total Dependency traversals rejected before mutation.",
                "# TYPE megacache_event_graph_rejected_total counter",
                "megacache_event_graph_rejected_total {}".format(
                    metrics.get("graph_rejected_total", 0)
                ),
            ]
        )
        return "\n".join(lines) + "\n"

    def begin_shutdown(self) -> None:
        begin = getattr(self.storage, "begin_shutdown", None)
        if begin is not None:
            begin()

    def close(self, timeout: float = 10.0) -> None:
        try:
            close = getattr(self.storage, "close", None)
            if close is not None:
                close(timeout)
        finally:
            self.ingestor.close()

    def __getattr__(self, name: str) -> Any:
        return getattr(self.storage, name)


@dataclass(frozen=True)
class KafkaRecord:
    topic: str
    partition: int
    offset: int
    value: bytes
    key: Optional[bytes] = None
    timestamp: Optional[float] = None
    headers: Tuple[Tuple[str, bytes], ...] = ()


class RecordConsumer(Protocol):
    """Kafka-style consumer contract; no network client is bundled."""

    def poll(
        self, max_records: int, timeout_seconds: float
    ) -> Sequence[KafkaRecord]: ...

    def commit(self, offsets: Mapping[Tuple[str, int], int]) -> None: ...


class KafkaRecordAdapter:
    """Maps externally supplied Kafka-style records to change events."""

    def __init__(
        self, ingestor: EventIngestor, source_name: str = "kafka"
    ) -> None:
        _validate_identifier(source_name, "Kafka source name")
        self.ingestor = ingestor
        self.source_name = source_name

    def consume_once(
        self,
        consumer: RecordConsumer,
        max_records: int = 100,
        timeout_seconds: float = 1.0,
    ) -> Tuple[EventResult, ...]:
        records = consumer.poll(max_records, timeout_seconds)
        ordered = sorted(
            records, key=lambda item: (item.topic, item.partition, item.offset)
        )
        results = []
        offsets: Dict[Tuple[str, int], int] = {}
        for record in ordered:
            event = self.event_from_record(record)
            results.append(self.ingestor.ingest(event))
            offsets[(record.topic, record.partition)] = record.offset + 1
        if offsets:
            consumer.commit(offsets)
        return tuple(results)

    def event_from_record(self, record: KafkaRecord) -> ChangeEvent:
        try:
            document = json.loads(record.value.decode("utf-8"))
        except (UnicodeDecodeError, json.JSONDecodeError) as exc:
            raise EventError("Kafka record value must be a JSON event") from exc
        if not isinstance(document, dict):
            raise EventError("Kafka record value must be a JSON object")
        document["source"] = self.source_name
        document["stream"] = "{}:{}".format(
            record.topic, record.partition
        )
        document["position"] = record.offset
        document["cursor"] = str(record.offset)
        document.setdefault(
            "event_id",
            "{}:{}:{}".format(record.topic, record.partition, record.offset),
        )
        if record.timestamp is not None:
            document["timestamp"] = record.timestamp
        return ChangeEvent.from_dict(document)


class ConnectorAdapter(Protocol):
    """Common contract for externally fed change-data-capture adapters."""

    def feed(self, records: Iterable[Any]) -> Tuple[EventResult, ...]: ...


@dataclass(frozen=True)
class PostgresLogicalRecord:
    slot: str
    lsn: str
    relation: str
    operation: str
    row: Mapping[str, Any]
    event_id: Optional[str] = None
    schema_id: Optional[str] = None
    timestamp: Optional[float] = None
    ordinal: Optional[int] = None


class PostgresLogicalAdapter:
    def __init__(
        self, ingestor: EventIngestor, source_name: str = "postgres"
    ) -> None:
        _validate_identifier(source_name, "PostgreSQL source name")
        self.ingestor = ingestor
        self.source_name = source_name

    def feed(
        self, records: Iterable[PostgresLogicalRecord]
    ) -> Tuple[EventResult, ...]:
        batch = tuple(records)
        ordered_positions = []
        for record in batch:
            if (
                isinstance(record.ordinal, bool)
                or not isinstance(record.ordinal, int)
                or record.ordinal < 0
                or record.ordinal > 0xFFFFFFFF
            ):
                raise EventError(
                    "PostgreSQL records require a stable 0-based ordinal"
                )
            ordered_positions.append(
                (parse_postgres_lsn(record.lsn), record.ordinal)
            )
        if any(
            current <= previous
            for previous, current in zip(
                ordered_positions, ordered_positions[1:]
            )
        ):
            raise EventError(
                "PostgreSQL records must be a strictly ordered change batch"
            )
        results = []
        for record, (lsn_position, ordinal) in zip(
            batch, ordered_positions
        ):
            position = (lsn_position << 32) | ordinal
            results.append(
                self.ingestor.ingest(
                    ChangeEvent(
                        event_id=record.event_id
                        or "{}:{}:{}".format(
                            record.slot, record.lsn, ordinal
                        ),
                        source=self.source_name,
                        stream=record.slot,
                        position=position,
                        operation=record.operation,
                        payload=dict(record.row),
                        timestamp=(
                            time.time()
                            if record.timestamp is None
                            else record.timestamp
                        ),
                        schema_id=record.schema_id,
                        cursor=record.lsn,
                    )
                )
            )
        return tuple(results)


@dataclass(frozen=True)
class MySQLBinlogRecord:
    server_id: str
    sequence: int
    binlog_file: str
    binlog_position: int
    table: str
    operation: str
    row: Mapping[str, Any]
    event_id: Optional[str] = None
    schema_id: Optional[str] = None
    timestamp: Optional[float] = None


class MySQLBinlogAdapter:
    def __init__(
        self, ingestor: EventIngestor, source_name: str = "mysql"
    ) -> None:
        _validate_identifier(source_name, "MySQL source name")
        self.ingestor = ingestor
        self.source_name = source_name

    def feed(
        self, records: Iterable[MySQLBinlogRecord]
    ) -> Tuple[EventResult, ...]:
        results = []
        for record in records:
            cursor = "{}:{}".format(
                record.binlog_file, record.binlog_position
            )
            results.append(
                self.ingestor.ingest(
                    ChangeEvent(
                        event_id=record.event_id
                        or "{}:{}:{}".format(
                            record.server_id, cursor, record.table
                        ),
                        source=self.source_name,
                        stream=record.server_id,
                        position=record.sequence,
                        operation=record.operation,
                        payload=dict(record.row),
                        timestamp=(
                            time.time()
                            if record.timestamp is None
                            else record.timestamp
                        ),
                        schema_id=record.schema_id,
                        cursor=cursor,
                    )
                )
            )
        return tuple(results)


@dataclass(frozen=True)
class MongoChangeRecord:
    database: str
    collection: str
    sequence: int
    resume_token: str
    operation: str
    document: Mapping[str, Any]
    event_id: Optional[str] = None
    schema_id: Optional[str] = None
    timestamp: Optional[float] = None


class MongoChangeStreamAdapter:
    def __init__(
        self, ingestor: EventIngestor, source_name: str = "mongo"
    ) -> None:
        _validate_identifier(source_name, "MongoDB source name")
        self.ingestor = ingestor
        self.source_name = source_name

    def feed(
        self, records: Iterable[MongoChangeRecord]
    ) -> Tuple[EventResult, ...]:
        results = []
        for record in records:
            stream = "{}.{}".format(record.database, record.collection)
            results.append(
                self.ingestor.ingest(
                    ChangeEvent(
                        event_id=record.event_id
                        or "{}:{}:{}".format(
                            stream, record.sequence, record.resume_token
                        ),
                        source=self.source_name,
                        stream=stream,
                        position=record.sequence,
                        operation=record.operation,
                        payload=dict(record.document),
                        timestamp=(
                            time.time()
                            if record.timestamp is None
                            else record.timestamp
                        ),
                        schema_id=record.schema_id,
                        cursor=record.resume_token,
                    )
                )
            )
        return tuple(results)


def parse_postgres_lsn(value: str) -> int:
    if (
        not isinstance(value, str)
        or re.fullmatch(r"[0-9A-Fa-f]+/[0-9A-Fa-f]+", value) is None
    ):
        raise EventError("PostgreSQL LSN must use HEX/HEX")
    high, low = value.split("/", 1)
    try:
        high_value = int(high, 16)
        low_value = int(low, 16)
    except ValueError as exc:
        raise EventError("PostgreSQL LSN must use HEX/HEX") from exc
    if high_value > 0xFFFFFFFF or low_value > 0xFFFFFFFF:
        raise EventError("PostgreSQL LSN halves must fit 32 bits")
    return (high_value << 32) + low_value


def load_event_automation(
    storage: StorageBackend,
    config_path: Optional[str],
    state_path: str,
    *,
    max_seen_events: int = 10_000,
    max_replay_tokens: int = 10_000,
    max_dead_letters: int = 1_000,
    max_dead_letter_bytes: int = 8_388_608,
    max_streams: int = 1_000,
    max_state_bytes: int = 16_777_216,
    max_payload_bytes: int = 1_048_576,
    max_cursor_bytes: int = 4_096,
    max_error_bytes: int = 4_096,
    graph_max_nodes: int = 10_000,
    graph_max_edges: int = 50_000,
    graph_max_fanout: int = 100,
    graph_max_depth: int = 16,
    graph_max_invalidation_nodes: int = 10_000,
    allowed_webhooks: Optional[Iterable[str]] = None,
) -> EventAutomation:
    document: Dict[str, Any] = {}
    if config_path is not None:
        try:
            with open(config_path, "r", encoding="utf-8") as source:
                document = json.load(source)
        except (OSError, UnicodeDecodeError, json.JSONDecodeError) as exc:
            raise EventError(
                "unable to load MEGACACHE_EVENTS_FILE: {}".format(exc)
            ) from exc
        if not isinstance(document, dict):
            raise EventError("events file must contain a JSON object")

    graph = DependencyGraph(
        max_nodes=graph_max_nodes,
        max_edges=graph_max_edges,
        max_fanout=graph_max_fanout,
        max_depth=graph_max_depth,
        max_invalidation_nodes=graph_max_invalidation_nodes,
    )
    dependencies = document.get("dependencies", [])
    if not isinstance(dependencies, list):
        raise EventError("dependencies must be an array")
    for edge in dependencies:
        if not isinstance(edge, dict):
            raise EventError("each dependency must be an object")
        graph.add_dependency(edge.get("dependency"), edge.get("dependent"))

    raw_readers = document.get("readers", [])
    if not isinstance(raw_readers, list):
        raise EventError("readers must be an array")
    readers = []
    for value in raw_readers:
        if not isinstance(value, dict):
            raise EventError("each reader must be an object")
        readers.append(
            ReaderRange(
                value.get("name"),
                value.get("minimum_version"),
                value.get("maximum_version"),
            )
        )

    raw_namespaces = document.get("namespaces", [])
    if not isinstance(raw_namespaces, list):
        raise EventError("namespaces must be an array")
    namespaces = []
    for value in raw_namespaces:
        if not isinstance(value, dict):
            raise EventError("each namespace must be an object")
        namespaces.append(
            VersionedNamespace(
                name=value.get("name"),
                write_version=value.get("write_version"),
                reader_minimum=value.get("reader_minimum"),
                reader_maximum=value.get("reader_maximum"),
                migration_policy=value.get("migration_policy", "rolling"),
            )
        )

    raw_rules = document.get("rules", [])
    if not isinstance(raw_rules, list):
        raise EventError("rules must be an array")
    rules = [EventRule.from_dict(value) for value in raw_rules]

    raw_webhooks = document.get("webhooks", [])
    if not isinstance(raw_webhooks, list):
        raise EventError("webhooks must be an array")
    allowed = None if allowed_webhooks is None else frozenset(allowed_webhooks)
    webhooks = []
    for value in raw_webhooks:
        if not isinstance(value, dict):
            raise EventError("each webhook must be an object")
        name = value.get("name")
        if allowed is not None and name not in allowed:
            continue
        direct_secret = value.get("secret")
        secret_env = value.get("secret_env")
        if (direct_secret is None) == (secret_env is None):
            raise EventError(
                "webhook must define exactly one of secret or secret_env"
            )
        if secret_env is not None:
            if not isinstance(secret_env, str) or not secret_env:
                raise EventError("webhook secret_env must be non-empty")
            direct_secret = os.getenv(secret_env)
            if direct_secret is None:
                raise EventError(
                    "webhook secret environment variable '{}' is unset".format(
                        secret_env
                    )
                )
        if not isinstance(direct_secret, str):
            raise EventError("webhook secret must be a string")
        webhooks.append(
            WebhookSource(
                name=name,
                secret=direct_secret.encode("utf-8"),
                tolerance_seconds=value.get("tolerance_seconds", 300),
            )
        )
    if allowed is not None:
        configured = {webhook.name for webhook in webhooks}
        missing = allowed - configured
        if missing:
            raise EventError(
                "tenant references unknown webhooks: {}".format(
                    ", ".join(sorted(missing))
                )
            )
    store = AtomicCheckpointStore(
        state_path,
        max_seen_events=max_seen_events,
        max_replay_tokens=max_replay_tokens,
        max_dead_letters=max_dead_letters,
        max_dead_letter_bytes=max_dead_letter_bytes,
        max_streams=max_streams,
        max_state_bytes=max_state_bytes,
        max_payload_bytes=max_payload_bytes,
        max_cursor_bytes=max_cursor_bytes,
        max_error_bytes=max_error_bytes,
    )
    try:
        ingestor = EventIngestor(
            storage,
            store,
            rules=rules,
            graph=graph,
            schemas=SchemaRegistry(readers),
            namespaces=namespaces,
        )
        authenticator = WebhookAuthenticator(store, webhooks)
    except Exception:
        store.close()
        raise
    return EventAutomation(storage, ingestor, authenticator)


def _validate_identifier(value: Any, field: str) -> None:
    if not isinstance(value, str) or _IDENTIFIER.fullmatch(value) is None:
        raise EventError(
            "{} must be a 1-128 character identifier".format(field)
        )


def _validate_cache_key(value: Any) -> None:
    if (
        not isinstance(value, str)
        or not value
        or len(value.encode("utf-8")) > 1024
    ):
        raise EventError("cache key must contain 1 to 1024 UTF-8 bytes")


def _string_tuple(value: Any, field: str) -> Tuple[str, ...]:
    if not isinstance(value, (list, tuple)) or any(
        not isinstance(item, str) or not item for item in value
    ):
        raise EventError("{} must be an array of strings".format(field))
    return tuple(value)


def _validate_template(template: Any) -> None:
    if not isinstance(template, str) or not template:
        raise EventError("event templates must be non-empty strings")
    stripped = _TEMPLATE_FIELD.sub("", template)
    if "{" in stripped or "}" in stripped:
        raise EventError("event template contains an invalid placeholder")


def _render_template(template: str, context: Mapping[str, Any]) -> str:
    def replace_field(match: re.Match) -> str:
        value: Any = context
        for component in match.group(1).split("."):
            if not isinstance(value, Mapping) or component not in value:
                raise EventError(
                    "event template field '{}' is missing".format(
                        match.group(1)
                    )
                )
            value = value[component]
        if isinstance(value, (dict, list, tuple)) or value is None:
            raise EventError(
                "event template field '{}' must be scalar".format(
                    match.group(1)
                )
            )
        return str(value)

    return _TEMPLATE_FIELD.sub(replace_field, template)
