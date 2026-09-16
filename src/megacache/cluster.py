"""Deterministic in-process distributed cache coordination.

The coordinator models node membership, replication, fencing, rebalancing, and
snapshot transfer without pretending to provide a node-to-node RPC transport.
It is suitable for embedded deployments, tests, and as the contract for a
future authenticated transport.
"""

from __future__ import annotations

import base64
import bisect
import hashlib
import json
import math
import secrets
import threading
import time
from collections import defaultdict
from dataclasses import dataclass
from enum import Enum
from typing import Any, Callable, Dict, Iterable, List, Optional, Sequence, Tuple

from .engine import CacheEngine, CacheResult
from .storage import StorageBackend, StorageEntry


class ClusterError(ValueError):
    """Base class for deterministic cluster operation failures."""


class QuorumError(ClusterError):
    """The selected consistency profile cannot be satisfied."""


class FenceError(ClusterError):
    """A mutation used a stale leadership term."""


class RebalanceError(ClusterError):
    """A rebalance plan is stale or cannot be completed safely."""


class SnapshotError(ClusterError):
    """A snapshot is invalid, corrupt, or exceeds configured limits."""


class SnapshotBackpressure(SnapshotError):
    """The sender must wait for acknowledgements before sending more data."""


class ConsistencyProfile(str, Enum):
    ONE = "one"
    MAJORITY = "majority"
    ALL = "all"

    @classmethod
    def parse(cls, value: Any) -> "ConsistencyProfile":
        if isinstance(value, cls):
            return value
        try:
            return cls(str(value).lower())
        except ValueError as exc:
            raise ValueError(
                "consistency must be one, majority, or all"
            ) from exc

    def required(self, replicas: int) -> int:
        if replicas <= 0:
            raise ValueError("replicas must be greater than zero")
        if self is self.ONE:
            return 1
        if self is self.ALL:
            return replicas
        return replicas // 2 + 1


@dataclass(frozen=True, order=True)
class VersionStamp:
    term: int
    sequence: int
    leader_id: str


@dataclass(frozen=True)
class FenceToken:
    term: int
    leader_id: str


@dataclass(frozen=True)
class NodeIdentity:
    node_id: str

    def __post_init__(self) -> None:
        encoded = self.node_id.encode("utf-8")
        if (
            not self.node_id
            or len(encoded) > 128
            or any(character.isspace() for character in self.node_id)
        ):
            raise ValueError(
                "node_id must contain 1 to 128 non-whitespace UTF-8 bytes"
            )


@dataclass
class ClusterNode:
    node_id: str
    storage: StorageBackend
    status: str = "active"
    last_heartbeat: float = 0.0
    incarnation: int = 1

    def __post_init__(self) -> None:
        NodeIdentity(self.node_id)
        if self.status not in {
            "active",
            "joining",
            "draining",
            "down",
            "drained",
            "removing",
            "removed",
        }:
            raise ValueError("invalid node status")
        if self.incarnation <= 0:
            raise ValueError("node incarnation must be positive")


class ConsistentHashRing:
    """A stable SHA-256 ring with deterministic virtual-node placement."""

    _NAMESPACE = b"megacache-ring-v1"

    def __init__(
        self,
        node_ids: Iterable[str],
        virtual_nodes: int = 128,
        version: int = 1,
    ) -> None:
        nodes = tuple(sorted(set(node_ids)))
        if not nodes:
            raise ValueError("a ring requires at least one node")
        for node_id in nodes:
            NodeIdentity(node_id)
        if virtual_nodes <= 0:
            raise ValueError("virtual_nodes must be greater than zero")
        if version <= 0:
            raise ValueError("ring version must be greater than zero")
        self.node_ids = nodes
        self.virtual_nodes = virtual_nodes
        self.version = version
        points = []
        for node_id in nodes:
            for virtual_node in range(virtual_nodes):
                payload = (
                    self._NAMESPACE
                    + b"\0"
                    + node_id.encode("utf-8")
                    + b"\0"
                    + str(virtual_node).encode("ascii")
                )
                token = int.from_bytes(
                    hashlib.sha256(payload).digest()[:8], "big"
                )
                points.append((token, node_id, virtual_node))
        self._points = tuple(sorted(points))
        self._tokens = tuple(point[0] for point in self._points)
        fingerprint_payload = json.dumps(
            {
                "nodes": self.node_ids,
                "virtual_nodes": self.virtual_nodes,
                "points": self._points,
            },
            separators=(",", ":"),
        ).encode("utf-8")
        self.fingerprint = hashlib.sha256(fingerprint_payload).hexdigest()

    def owners(self, key: str, replica_count: int) -> Tuple[str, ...]:
        if not isinstance(key, str) or not key:
            raise ValueError("key must not be empty")
        if replica_count <= 0:
            raise ValueError("replica_count must be greater than zero")
        wanted = min(replica_count, len(self.node_ids))
        token = int.from_bytes(
            hashlib.sha256(key.encode("utf-8")).digest()[:8], "big"
        )
        index = bisect.bisect_right(self._tokens, token)
        owners: List[str] = []
        for offset in range(len(self._points)):
            node_id = self._points[(index + offset) % len(self._points)][1]
            if node_id not in owners:
                owners.append(node_id)
                if len(owners) == wanted:
                    break
        return tuple(owners)

    def primary(self, key: str) -> str:
        return self.owners(key, 1)[0]

    def describe(self) -> Dict[str, Any]:
        return {
            "version": self.version,
            "fingerprint": self.fingerprint,
            "virtual_nodes": self.virtual_nodes,
            "nodes": list(self.node_ids),
            "tokens": len(self._points),
        }


@dataclass(frozen=True)
class _ReplicaMetadata:
    version: VersionStamp
    deleted: bool
    tags: Tuple[str, ...] = ()


@dataclass(frozen=True)
class SnapshotRecord:
    entry: Optional[StorageEntry]
    key: str
    version: VersionStamp
    deleted: bool


@dataclass(frozen=True)
class SnapshotChunk:
    snapshot_id: str
    index: int
    payload: bytes
    checksum: str


@dataclass(frozen=True)
class SnapshotManifest:
    snapshot_id: str
    source_node_id: str
    ring_version: int
    fence: FenceToken
    chunk_count: int
    entry_count: int
    total_bytes: int
    checksum: str


class SnapshotSession:
    """Pull-based snapshot sender with explicit acknowledgement backpressure."""

    def __init__(
        self,
        manifest: SnapshotManifest,
        chunks: Sequence[SnapshotChunk],
        max_in_flight: int,
    ) -> None:
        self.manifest = manifest
        self._chunks = tuple(chunks)
        self._max_in_flight = max_in_flight
        self._next_index = 0
        self._outstanding: set = set()
        self._lock = threading.Lock()

    def next_chunk(self) -> Optional[SnapshotChunk]:
        with self._lock:
            if len(self._outstanding) >= self._max_in_flight:
                raise SnapshotBackpressure(
                    "snapshot sender is waiting for chunk acknowledgements"
                )
            if self._next_index >= len(self._chunks):
                return None
            chunk = self._chunks[self._next_index]
            self._next_index += 1
            self._outstanding.add(chunk.index)
            return chunk

    def acknowledge(self, index: int) -> None:
        with self._lock:
            if index not in self._outstanding:
                raise SnapshotError("unknown or duplicate chunk acknowledgement")
            self._outstanding.remove(index)

    @property
    def complete(self) -> bool:
        with self._lock:
            return (
                self._next_index == len(self._chunks)
                and not self._outstanding
            )


class SnapshotReceiver:
    """Checksum-validating receiver that stages before mutating storage."""

    def __init__(
        self,
        manifest: SnapshotManifest,
        max_payload_bytes: int,
    ) -> None:
        self.manifest = manifest
        self._max_payload_bytes = max_payload_bytes
        self._chunks: Dict[int, bytes] = {}
        self._received_bytes = 0

    def receive(self, chunk: SnapshotChunk) -> None:
        if chunk.snapshot_id != self.manifest.snapshot_id:
            raise SnapshotError("snapshot identifier does not match manifest")
        if chunk.index < 0 or chunk.index >= self.manifest.chunk_count:
            raise SnapshotError("snapshot chunk index is out of range")
        if chunk.index in self._chunks:
            raise SnapshotError("duplicate snapshot chunk")
        checksum = hashlib.sha256(chunk.payload).hexdigest()
        if not secrets.compare_digest(checksum, chunk.checksum):
            raise SnapshotError("snapshot chunk checksum mismatch")
        self._received_bytes += len(chunk.payload)
        if (
            self._received_bytes > self._max_payload_bytes
            or self._received_bytes > self.manifest.total_bytes
        ):
            raise SnapshotError("snapshot exceeds configured payload limit")
        self._chunks[chunk.index] = bytes(chunk.payload)

    def records(self) -> Tuple[SnapshotRecord, ...]:
        if len(self._chunks) != self.manifest.chunk_count:
            raise SnapshotError("snapshot is incomplete")
        payload = b"".join(
            self._chunks[index] for index in range(self.manifest.chunk_count)
        )
        if len(payload) != self.manifest.total_bytes:
            raise SnapshotError("snapshot byte count mismatch")
        checksum = hashlib.sha256(payload).hexdigest()
        if not secrets.compare_digest(checksum, self.manifest.checksum):
            raise SnapshotError("snapshot checksum mismatch")
        records = tuple(
            _decode_snapshot_record(line)
            for line in payload.splitlines()
            if line
        )
        if len(records) != self.manifest.entry_count:
            raise SnapshotError("snapshot entry count mismatch")
        return records


@dataclass(frozen=True)
class RebalanceMove:
    key: str
    source_node_id: str
    target_node_id: str


@dataclass(frozen=True)
class RebalancePlan:
    plan_id: str
    fence: FenceToken
    base_ring_version: int
    target_ring: ConsistentHashRing
    moves: Tuple[RebalanceMove, ...]
    joining_nodes: Tuple[str, ...]
    departing_nodes: Tuple[str, ...]


class _Flight:
    def __init__(self) -> None:
        self.event = threading.Event()
        self.result: Any = None
        self.error: Optional[BaseException] = None


@dataclass
class _ClusterLease:
    token: str
    until: float
    fence: FenceToken


class ClusterStorage:
    """Storage backend coordinating deterministic in-process replicas."""

    def __init__(
        self,
        nodes: Iterable[ClusterNode],
        replica_count: int = 1,
        virtual_nodes: int = 128,
        consistency: Any = ConsistencyProfile.MAJORITY,
        heartbeat_timeout_seconds: float = 15.0,
        lease_seconds: int = 30,
        snapshot_payload_limit_bytes: int = 67_108_864,
        snapshot_chunk_bytes: int = 262_144,
        snapshot_max_in_flight: int = 2,
        max_leases: int = 10_000,
        max_lease_memory_bytes: int = 67_108_864,
        max_retained_tombstones: int = 10_000,
        clock: Callable[[], float] = time.monotonic,
    ) -> None:
        normalized = tuple(nodes)
        if not normalized:
            raise ValueError("a cluster requires at least one node")
        if replica_count <= 0:
            raise ValueError("replica_count must be greater than zero")
        if (
            not math.isfinite(heartbeat_timeout_seconds)
            or heartbeat_timeout_seconds <= 0
            or lease_seconds <= 0
        ):
            raise ValueError("cluster timeouts must be greater than zero")
        if (
            snapshot_payload_limit_bytes <= 0
            or snapshot_chunk_bytes <= 0
            or snapshot_max_in_flight <= 0
            or max_leases <= 0
            or max_lease_memory_bytes <= 0
            or max_retained_tombstones <= 0
        ):
            raise ValueError("cluster capacity limits must be greater than zero")
        if snapshot_chunk_bytes > snapshot_payload_limit_bytes:
            raise ValueError(
                "snapshot chunk size cannot exceed the payload limit"
            )
        self._clock = clock
        now = clock()
        self._nodes: Dict[str, ClusterNode] = {}
        for node in normalized:
            if node.node_id in self._nodes:
                raise ValueError("duplicate node_id {}".format(node.node_id))
            node.last_heartbeat = now
            self._nodes[node.node_id] = node
        ring_nodes = tuple(
            node.node_id
            for node in normalized
            if node.status in ("active", "draining", "down")
        )
        if not ring_nodes:
            raise ValueError("a cluster requires an active ring member")
        self._replica_count = replica_count
        self._virtual_nodes = virtual_nodes
        self._consistency = ConsistencyProfile.parse(consistency)
        self._heartbeat_timeout = float(heartbeat_timeout_seconds)
        self._lease_seconds = lease_seconds
        self._snapshot_payload_limit = snapshot_payload_limit_bytes
        self._snapshot_chunk_bytes = snapshot_chunk_bytes
        self._snapshot_max_in_flight = snapshot_max_in_flight
        self._max_leases = max_leases
        self._max_lease_memory_bytes = max_lease_memory_bytes
        self._max_retained_tombstones = max_retained_tombstones
        self._ring = ConsistentHashRing(
            ring_nodes, virtual_nodes=virtual_nodes, version=1
        )
        healthy = sorted(
            node_id
            for node_id in ring_nodes
            if self._nodes[node_id].status != "down"
        )
        if not healthy:
            raise ValueError("a cluster requires a healthy node")
        self._leader_id = healthy[0]
        self._term = 1
        self._sequence = 0
        self._metadata: Dict[str, Dict[str, _ReplicaMetadata]] = {
            node_id: {} for node_id in self._nodes
        }
        self._known_keys: set = set()
        self._catalog_keys: set = set()
        self._tombstone_gc_watermark: Optional[VersionStamp] = None
        self._key_tags: Dict[str, Tuple[str, ...]] = {}
        self._tag_keys: Dict[str, set] = defaultdict(set)
        self._leases: Dict[str, _ClusterLease] = {}
        self._lease_bytes = 0
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

    @classmethod
    def single_node(
        cls,
        node_id: str,
        storage: Optional[StorageBackend] = None,
        **kwargs: Any
    ) -> "ClusterStorage":
        return cls(
            [
                ClusterNode(
                    node_id,
                    storage if storage is not None else CacheEngine(),
                )
            ],
            replica_count=1,
            **kwargs
        )

    @property
    def ring(self) -> ConsistentHashRing:
        with self._lock:
            return self._ring

    def leadership(self) -> FenceToken:
        with self._lock:
            self._detect_failures_locked()
            return FenceToken(self._term, self._leader_id)

    def assert_fence(self, fence: FenceToken) -> None:
        with self._lock:
            self._assert_fence_locked(fence)

    def heartbeat(self, node_id: str, incarnation: Optional[int] = None) -> None:
        with self._lock:
            node = self._node(node_id)
            if incarnation is not None:
                if incarnation < node.incarnation:
                    raise ClusterError("stale node incarnation")
                node.incarnation = incarnation
            node.last_heartbeat = self._clock()
            if node.status == "down":
                node.status = (
                    "active" if node_id in self._ring.node_ids else "joining"
                )
            self._metrics["heartbeats_total"] += 1
            self._elect_leader_locked()
            self._gc_tombstones_locked()

    def detect_failures(self) -> Tuple[str, ...]:
        with self._lock:
            return self._detect_failures_locked()

    def mark_node_down(self, node_id: str) -> None:
        with self._lock:
            node = self._node(node_id)
            node.status = "down"
            self._metrics["node_failures_total"] += 1
            self._elect_leader_locked()

    def add_node(
        self, node_id: str, storage: Optional[StorageBackend] = None
    ) -> RebalancePlan:
        with self._lock:
            if node_id in self._nodes:
                raise ClusterError("node already exists")
            node = ClusterNode(
                node_id,
                (
                    storage
                    if storage is not None
                    else CacheEngine(
                        lease_seconds=self._lease_seconds,
                        clock=self._clock,
                    )
                ),
                status="joining",
                last_heartbeat=self._clock(),
            )
            self._nodes[node_id] = node
            self._metadata[node_id] = {}
            try:
                return self._plan_rebalance_locked()
            except BaseException:
                self._nodes.pop(node_id, None)
                self._metadata.pop(node_id, None)
                raise

    def drain_node(self, node_id: str) -> RebalancePlan:
        with self._lock:
            node = self._node(node_id)
            if node_id not in self._ring.node_ids:
                raise ClusterError("node is not an active ring member")
            if len(self._ring.node_ids) <= 1:
                raise ClusterError("cannot drain the last ring member")
            previous_status = node.status
            node.status = "draining"
            try:
                return self._plan_rebalance_locked()
            except BaseException:
                node.status = previous_status
                raise

    def remove_node(self, node_id: str) -> RebalancePlan:
        with self._lock:
            node = self._node(node_id)
            if node_id not in self._ring.node_ids:
                raise ClusterError("node is not an active ring member")
            if len(self._ring.node_ids) <= 1:
                raise ClusterError("cannot remove the last ring member")
            previous_status = node.status
            node.status = "removing"
            try:
                return self._plan_rebalance_locked()
            except BaseException:
                node.status = previous_status
                raise

    def plan_rebalance(self) -> RebalancePlan:
        with self._lock:
            return self._plan_rebalance_locked()

    def apply_rebalance(self, plan: RebalancePlan) -> None:
        with self._lock:
            self._detect_failures_locked()
            self._assert_fence_locked(plan.fence)
            if plan.base_ring_version != self._ring.version:
                raise RebalanceError("rebalance plan uses a stale ring version")
            expected = self._plan_rebalance_locked()
            if expected.plan_id != plan.plan_id:
                raise RebalanceError("rebalance plan no longer matches membership")

            old_ring = self._ring
            affected_ids = (
                set(old_ring.node_ids)
                | set(plan.joining_nodes)
                | {move.target_node_id for move in plan.moves}
            )
            checkpoints = self._capture_backend_checkpoints_locked(
                affected_ids
            )
            old_metadata = {
                node_id: dict(self._metadata[node_id])
                for node_id in affected_ids
            }
            old_statuses = {
                node_id: self._nodes[node_id].status
                for node_id in affected_ids
            }
            try:
                for move in plan.moves:
                    source = self._node(move.source_node_id)
                    target = self._node(move.target_node_id)
                    latest = self._latest_metadata_locked(move.key)
                    if (
                        latest is None
                        or not self._healthy(source)
                        or not self._node_has_record_locked(
                            source.node_id, move.key, latest
                        )
                    ):
                        raise RebalanceError(
                            "rebalance has no healthy latest-version source "
                            "for key {}".format(move.key)
                        )
                    if not self._healthy(target):
                        raise RebalanceError(
                            "rebalance target {} is unavailable".format(
                                target.node_id
                            )
                        )
                    session = self.create_snapshot(
                        source.node_id, keys=(move.key,)
                    )
                    self._bootstrap_locked(target.node_id, session)

                for key in tuple(self._catalog_keys):
                    latest = self._latest_metadata_locked(key)
                    if latest is None:
                        continue
                    for target_id in plan.target_ring.owners(
                        key, self._replica_count
                    ):
                        if not self._node_has_record_locked(
                            target_id, key, latest
                        ):
                            raise RebalanceError(
                                "latest version for key {} was not verified "
                                "on target {}".format(key, target_id)
                            )

                self._ring = plan.target_ring
                for node_id in plan.joining_nodes:
                    self._nodes[node_id].status = "active"
                for node_id in plan.departing_nodes:
                    node = self._nodes[node_id]
                    node.status = (
                        "drained"
                        if node.status == "draining"
                        else "removed"
                    )

                for key in tuple(self._catalog_keys):
                    desired = set(
                        self._ring.owners(key, self._replica_count)
                    )
                    for node_id in old_ring.node_ids:
                        if node_id not in desired:
                            self._nodes[node_id].storage.delete(key)
                            self._metadata[node_id].pop(key, None)
            except BaseException:
                self._restore_backend_checkpoints_locked(checkpoints)
                for node_id, metadata in old_metadata.items():
                    self._metadata[node_id] = metadata
                self._ring = old_ring
                for node_id, status in old_statuses.items():
                    self._nodes[node_id].status = status
                raise

            self._gc_tombstones_locked()
            self._metrics["rebalances_total"] += 1
            self._elect_leader_locked()

    def ownership(self, key: str) -> Dict[str, Any]:
        self._validate_key(key)
        with self._lock:
            owners = self._ring.owners(key, self._replica_count)
            return {
                "key": key,
                "ring_version": self._ring.version,
                "primary": owners[0],
                "replicas": list(owners),
            }

    def get(
        self,
        key: str,
        consistency: Optional[Any] = None,
    ) -> CacheResult:
        self._validate_key(key)
        with self._lock:
            result, _, _ = self._read_locked(
                key, self._profile(consistency)
            )
            return result

    def put(
        self,
        key: str,
        value: Any,
        ttl_seconds: Optional[int] = None,
        stale_seconds: Optional[int] = None,
        tags: Iterable[str] = (),
        lease_token: Optional[str] = None,
        persistent: bool = False,
        consistency: Optional[Any] = None,
        fence: Optional[FenceToken] = None,
    ) -> CacheResult:
        self._validate_key(key)
        normalized_tags = self._normalize_tags(tags)
        with self._lock:
            self._detect_failures_locked()
            selected_fence = fence or FenceToken(
                self._term, self._leader_id
            )
            self._assert_fence_locked(selected_fence)
            if lease_token is not None:
                self._validate_lease_locked(key, lease_token)
            result = self._replicate_put_locked(
                key=key,
                value=value,
                ttl_seconds=ttl_seconds,
                stale_seconds=stale_seconds,
                tags=normalized_tags,
                persistent=persistent,
                profile=self._profile(consistency),
            )
            self._remove_lease_locked(key)
            self._replace_key_tags_locked(key, normalized_tags)
            self._known_keys.add(key)
            self._catalog_keys.add(key)
            return result

    def mget(self, keys: Iterable[str]) -> Tuple[CacheResult, ...]:
        with self._lock:
            return tuple(self.get(key) for key in tuple(keys))

    def mset(self, values: Iterable[Tuple[str, Any]]) -> None:
        normalized = tuple(values)
        with self._lock:
            availability = {}
            for key, value in normalized:
                self._validate_key(key)
                owners = self._ring.owners(key, self._replica_count)
                available = self._available_owners_locked(owners)
                required = self._consistency.required(len(owners))
                if len(available) < required:
                    raise QuorumError(
                        self._quorum_message(
                            "write", required, len(available)
                        )
                    )
                availability[key] = available
                for node_id in available:
                    self._prevalidate_put_locked(
                        node_id,
                        key,
                        value,
                        None,
                        None,
                        (),
                        True,
                    )
            node_ids = {
                node_id
                for available in availability.values()
                for node_id in available
            }
            checkpoints = self._capture_backend_checkpoints_locked(node_ids)
            old_metadata = {
                node_id: dict(self._metadata[node_id])
                for node_id in node_ids
            }
            old_sequence = self._sequence
            old_known = set(self._known_keys)
            old_catalog = set(self._catalog_keys)
            old_key_tags = dict(self._key_tags)
            old_tag_keys = defaultdict(
                set,
                (
                    (tag, set(tag_keys))
                    for tag, tag_keys in self._tag_keys.items()
                ),
            )
            old_leases = dict(self._leases)
            old_lease_bytes = self._lease_bytes
            old_metrics = dict(self._metrics)
            try:
                for key, value in normalized:
                    self._replicate_put_locked(
                        key=key,
                        value=value,
                        ttl_seconds=None,
                        stale_seconds=None,
                        tags=(),
                        persistent=True,
                        profile=self._consistency,
                    )
                    self._replace_key_tags_locked(key, ())
                    self._known_keys.add(key)
                    self._catalog_keys.add(key)
                    self._remove_lease_locked(key)
            except BaseException:
                self._restore_backend_checkpoints_locked(checkpoints)
                for node_id, metadata in old_metadata.items():
                    self._metadata[node_id] = metadata
                self._sequence = old_sequence
                self._known_keys = old_known
                self._catalog_keys = old_catalog
                self._key_tags = old_key_tags
                self._tag_keys = old_tag_keys
                self._leases = old_leases
                self._lease_bytes = old_lease_bytes
                self._metrics = defaultdict(int, old_metrics)
                raise

    def delete(
        self,
        key: str,
        consistency: Optional[Any] = None,
        fence: Optional[FenceToken] = None,
    ) -> bool:
        self._validate_key(key)
        with self._lock:
            self._detect_failures_locked()
            selected_fence = fence or FenceToken(
                self._term, self._leader_id
            )
            self._assert_fence_locked(selected_fence)
            return bool(
                self._delete_keys_transaction_locked(
                    (key,), self._profile(consistency)
                )
            )

    def delete_many(self, keys: Iterable[str]) -> int:
        normalized = tuple(keys)
        with self._lock:
            self._detect_failures_locked()
            self._assert_fence_locked(
                FenceToken(self._term, self._leader_id)
            )
            return self._delete_keys_transaction_locked(
                normalized, self._consistency
            )

    def exists(self, keys: Iterable[str]) -> int:
        with self._lock:
            return sum(
                1 for key in tuple(keys) if self.get(key).state != "miss"
            )

    def expire(self, key: str, ttl_seconds: int) -> bool:
        if (
            not isinstance(ttl_seconds, int)
            or isinstance(ttl_seconds, bool)
            or ttl_seconds <= 0
        ):
            raise ValueError("ttl_seconds must be a positive integer")
        with self._lock:
            result, source_id, _ = self._read_locked(
                key, self._consistency
            )
            if result.state == "miss" or source_id is None:
                return False
            source_entries = self._nodes[source_id].storage.export_entries(
                (key,)
            )
            if not source_entries:
                return False
            entry = source_entries[0]
            self._replicate_put_locked(
                key=key,
                value=entry.value,
                ttl_seconds=ttl_seconds,
                stale_seconds=0,
                tags=entry.tags,
                persistent=False,
                profile=self._consistency,
            )
            return True

    def ttl(self, key: str) -> int:
        with self._lock:
            result, source_id, _ = self._read_locked(
                key, self._consistency
            )
            if result.state == "miss" or source_id is None:
                return -2
            return self._nodes[source_id].storage.ttl(key)

    def flush(self) -> int:
        with self._lock:
            self._detect_failures_locked()
            unavailable = [
                node_id
                for node_id in self._ring.node_ids
                if not self._healthy(self._nodes[node_id])
            ]
            if unavailable:
                raise QuorumError(
                    "flush requires every ring member to be available"
                )
            removed = self.size()
            for node in self._nodes.values():
                if node.status != "removed":
                    node.storage.flush()
                    self._metadata[node.node_id].clear()
            self._tombstone_gc_watermark = self._next_version_locked()
            self._known_keys.clear()
            self._catalog_keys.clear()
            self._key_tags.clear()
            self._tag_keys.clear()
            self._leases.clear()
            self._lease_bytes = 0
            return removed

    def size(self) -> int:
        with self._lock:
            return sum(
                1
                for key in tuple(self._known_keys)
                if self.get(key).state != "miss"
            )

    def keys(self) -> Tuple[str, ...]:
        with self._lock:
            live = [
                key
                for key in sorted(self._known_keys)
                if self.get(key).state != "miss"
            ]
            return tuple(live)

    def invalidate_tags(self, tags: Iterable[str]) -> int:
        if isinstance(tags, (str, bytes)):
            raise ValueError("tags must be an array of strings")
        normalized = self._normalize_tags(tags)
        with self._lock:
            keys = set()
            for tag in normalized:
                keys.update(self._tag_keys.get(tag, set()))
            selected = tuple(sorted(keys))
            if not selected:
                return 0
            removed = self._delete_keys_transaction_locked(
                selected, self._consistency
            )
            self._metrics["invalidations_total"] += removed
            return removed

    def acquire_lease(self, key: str, force: bool = False) -> CacheResult:
        self._validate_key(key)
        if not isinstance(force, bool):
            raise ValueError("force must be a boolean")
        now = self._clock()
        with self._lock:
            self._detect_failures_locked()
            self._purge_expired_leases_locked(now)
            result = self.get(key)
            if result.state == "fresh" and not force:
                return result
            owners = self._ring.owners(key, self._replica_count)
            required = self._consistency.required(len(owners))
            available = self._available_owners_locked(owners)
            if len(available) < required:
                raise QuorumError(
                    self._quorum_message("lease", required, len(available))
                )
            existing = self._leases.get(key)
            if (
                existing is not None
                and existing.until > now
                and existing.fence
                == FenceToken(self._term, self._leader_id)
            ):
                if result.state == "stale":
                    self._metrics["coalesced_total"] += 1
                    return result
                self._metrics["coalesced_total"] += 1
                return CacheResult(
                    state="loading",
                    retry_after_seconds=max(0.0, existing.until - now),
                )
            token = secrets.token_urlsafe(24)
            size_bytes = (
                len(key.encode("utf-8"))
                + len(token.encode("ascii"))
                + 96
            )
            if (
                len(self._leases) >= self._max_leases
                or self._lease_bytes + size_bytes
                > self._max_lease_memory_bytes
            ):
                self._metrics["rejected_leases_total"] += 1
                raise ValueError("lease capacity exhausted")
            self._leases[key] = _ClusterLease(
                token=token,
                until=now + self._lease_seconds,
                fence=FenceToken(self._term, self._leader_id),
            )
            self._lease_bytes += size_bytes
            self._metrics["leases_total"] += 1
            if result.state == "stale":
                return CacheResult(
                    state="stale_lease",
                    value=result.value,
                    stale_for_seconds=result.stale_for_seconds,
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
            return self._remove_lease_locked(key)

    def renew_lease(self, key: str, lease_token: str) -> bool:
        self._validate_key(key)
        if not isinstance(lease_token, str):
            raise ValueError("lease token must be a string")
        now = self._clock()
        with self._lock:
            lease = self._leases.get(key)
            if lease is None or lease.until <= now:
                self._remove_lease_locked(key)
                return False
            self._assert_fence_locked(lease.fence)
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
        with self._lock:
            cached = self.get(key)
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
            flight.result = self.put(
                key,
                loader(),
                ttl_seconds=ttl_seconds,
                stale_seconds=stale_seconds,
                tags=tags,
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
        """Single-coordinator singleflight shared by every logical node."""
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

    def export_entries(
        self, keys: Optional[Iterable[str]] = None
    ) -> Tuple[StorageEntry, ...]:
        selected = self.keys() if keys is None else tuple(keys)
        exported = []
        with self._lock:
            for key in selected:
                result, source_id, _ = self._read_locked(
                    key, self._consistency
                )
                if result.state == "miss" or source_id is None:
                    continue
                entries = self._nodes[source_id].storage.export_entries(
                    (key,)
                )
                if entries:
                    exported.append(entries[0])
        return tuple(exported)

    def restore_entries(self, entries: Iterable[StorageEntry]) -> int:
        normalized = tuple(entries)
        with self._lock:
            keys = []
            for entry in normalized:
                if not isinstance(entry, StorageEntry):
                    raise ValueError(
                        "snapshot entries must be StorageEntry values"
                    )
                self._validate_key(entry.key)
                keys.append(entry.key)
            if len(set(keys)) != len(keys):
                raise ValueError("snapshot contains duplicate keys")

            availability = {}
            node_entries: Dict[str, List[StorageEntry]] = defaultdict(list)
            for entry in normalized:
                owners = self._ring.owners(
                    entry.key, self._replica_count
                )
                available = self._available_owners_locked(owners)
                required = self._consistency.required(len(owners))
                if len(available) < required:
                    raise QuorumError(
                        self._quorum_message(
                            "restore", required, len(available)
                        )
                    )
                availability[entry.key] = (available, required)
                for node_id in available:
                    node_entries[node_id].append(entry)
            for node_id, selected in node_entries.items():
                self._prevalidate_restore_locked(node_id, tuple(selected))

            checkpoints = self._capture_backend_checkpoints_locked(
                node_entries
            )
            old_metadata = {
                node_id: dict(self._metadata[node_id])
                for node_id in node_entries
            }
            old_sequence = self._sequence
            old_known = set(self._known_keys)
            old_catalog = set(self._catalog_keys)
            old_key_tags = dict(self._key_tags)
            old_tag_keys = defaultdict(
                set,
                (
                    (tag, set(tag_keys))
                    for tag, tag_keys in self._tag_keys.items()
                ),
            )
            old_metrics = dict(self._metrics)
            try:
                versions = {
                    entry.key: self._next_version_locked()
                    for entry in normalized
                }
                successes = {
                    entry.key: set() for entry in normalized
                }
                for node_id, selected in node_entries.items():
                    try:
                        self._nodes[node_id].storage.restore_entries(
                            tuple(selected)
                        )
                        for entry in selected:
                            self._metadata[node_id][
                                entry.key
                            ] = _ReplicaMetadata(
                                versions[entry.key],
                                False,
                                tuple(sorted(set(entry.tags))),
                            )
                            successes[entry.key].add(node_id)
                    except (ValueError, TypeError):
                        self._restore_one_backend_checkpoint_locked(
                            node_id, checkpoints[node_id]
                        )
                        self._metadata[node_id] = old_metadata[node_id]
                        raise
                    except Exception:
                        self._restore_one_backend_checkpoint_locked(
                            node_id, checkpoints[node_id]
                        )
                        self._metadata[node_id] = old_metadata[node_id]
                for entry in normalized:
                    required = availability[entry.key][1]
                    if len(successes[entry.key]) < required:
                        raise QuorumError(
                            self._quorum_message(
                                "restore",
                                required,
                                len(successes[entry.key]),
                            )
                        )
                for entry in normalized:
                    self._replace_key_tags_locked(entry.key, entry.tags)
                    self._known_keys.add(entry.key)
                    self._catalog_keys.add(entry.key)
            except BaseException:
                self._restore_backend_checkpoints_locked(checkpoints)
                for node_id, metadata in old_metadata.items():
                    self._metadata[node_id] = metadata
                self._sequence = old_sequence
                self._known_keys = old_known
                self._catalog_keys = old_catalog
                self._key_tags = old_key_tags
                self._tag_keys = old_tag_keys
                self._metrics = defaultdict(int, old_metrics)
                raise
        return len(normalized)

    def create_snapshot(
        self,
        node_id: str,
        keys: Optional[Iterable[str]] = None,
    ) -> SnapshotSession:
        with self._lock:
            node = self._node(node_id)
            selected = (
                tuple(
                    sorted(
                        set(node.storage.keys())
                        | set(self._metadata[node_id])
                    )
                )
                if keys is None
                else tuple(dict.fromkeys(keys))
            )
            entries = {
                entry.key: entry
                for entry in node.storage.export_entries(selected)
            }
            records = []
            for key in selected:
                metadata = self._metadata[node_id].get(key)
                if metadata is None:
                    if key not in entries:
                        continue
                    metadata = _ReplicaMetadata(
                        VersionStamp(0, 0, node_id), False, entries[key].tags
                    )
                records.append(
                    SnapshotRecord(
                        entry=entries.get(key),
                        key=key,
                        version=metadata.version,
                        deleted=metadata.deleted or key not in entries,
                    )
                )
            return self._snapshot_session_locked(node_id, records)

    def bootstrap_node(
        self, node_id: str, session: SnapshotSession
    ) -> None:
        with self._lock:
            self._bootstrap_locked(node_id, session)

    def topology(self) -> Dict[str, Any]:
        with self._lock:
            self._detect_failures_locked()
            ownership = defaultdict(lambda: {"primary": 0, "replicas": 0})
            lag = defaultdict(int)
            for key in self._catalog_keys:
                owners = self._ring.owners(key, self._replica_count)
                latest = self._latest_metadata_locked(key)
                for index, node_id in enumerate(owners):
                    if key in self._known_keys:
                        ownership[node_id]["replicas"] += 1
                        if index == 0:
                            ownership[node_id]["primary"] += 1
                    metadata = self._metadata[node_id].get(key)
                    if latest is not None and (
                        metadata is None or metadata.version < latest.version
                    ):
                        lag[node_id] += 1
            nodes = []
            now = self._clock()
            for node_id in sorted(self._nodes):
                node = self._nodes[node_id]
                nodes.append(
                    {
                        "node_id": node_id,
                        "status": node.status,
                        "incarnation": node.incarnation,
                        "heartbeat_age_seconds": max(
                            0.0, now - node.last_heartbeat
                        ),
                        "primary_keys": ownership[node_id]["primary"],
                        "replica_keys": ownership[node_id]["replicas"],
                        "replication_lag_entries": lag[node_id],
                    }
                )
            return {
                "mode": "in-process",
                "transport": "local coordinator; authenticated node RPC is not implemented",
                "leader": {
                    "node_id": self._leader_id,
                    "term": self._term,
                },
                "ring": self._ring.describe(),
                "replica_count": self._replica_count,
                "consistency": self._consistency.value,
                "nodes": nodes,
                "degraded": self._degraded_locked(),
            }

    def status(self) -> Dict[str, Any]:
        with self._lock:
            topology = self.topology()
            return {
                "leader": topology["leader"],
                "ring_version": topology["ring"]["version"],
                "healthy_nodes": sum(
                    1
                    for node in topology["nodes"]
                    if node["status"]
                    in ("active", "joining", "draining")
                    and node["heartbeat_age_seconds"]
                    <= self._heartbeat_timeout
                ),
                "total_nodes": len(topology["nodes"]),
                "degraded": topology["degraded"],
                "replication_lag_entries": sum(
                    node["replication_lag_entries"]
                    for node in topology["nodes"]
                ),
                "known_keys": len(self._known_keys),
            }

    def stats(self) -> Dict[str, int]:
        with self._lock:
            self._purge_expired_leases_locked(self._clock())
            snapshot: Dict[str, int] = defaultdict(int)
            for node in self._nodes.values():
                if node.status == "removed":
                    continue
                for name, value in node.storage.stats().items():
                    if name not in {
                        "entries",
                        "tags",
                        "active_leases",
                        "protocol_requests_total",
                        "request_errors_total",
                    }:
                        snapshot[name] += value
            for name, value in self._metrics.items():
                snapshot[name] += value
            snapshot.update(
                {
                    "entries": len(self._known_keys),
                    "tags": len(self._tag_keys),
                    "lease_memory_bytes": self._lease_bytes,
                    "cluster_nodes": len(self._nodes),
                    "cluster_healthy_nodes": sum(
                        1
                        for node in self._nodes.values()
                        if self._healthy(node)
                    ),
                    "cluster_ring_version": self._ring.version,
                    "cluster_leadership_term": self._term,
                    "cluster_degraded": int(self._degraded_locked()),
                    "retained_tombstones": self._tombstone_count_locked(),
                    "retained_tombstone_limit": self._max_retained_tombstones,
                    "active_leases": sum(
                        1
                        for lease in self._leases.values()
                        if lease.until > self._clock()
                    ),
                    "protocol_requests_total": sum(
                        self._request_counts.values()
                    ),
                }
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
                    self._latency_counts[
                        (protocol, operation, bucket)
                    ] += 1

    def prometheus_metrics(self) -> str:
        with self._lock:
            lines = []
            for name, value in sorted(self.stats().items()):
                metric_type = (
                    "gauge"
                    if name
                    in {
                        "entries",
                        "tags",
                        "memory_bytes",
                        "lease_memory_bytes",
                        "memory_limit_bytes",
                        "cluster_nodes",
                        "cluster_healthy_nodes",
                        "cluster_ring_version",
                        "cluster_leadership_term",
                        "cluster_degraded",
                        "active_leases",
                    }
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
            for (protocol, operation, status), value in sorted(
                self._request_counts.items()
            ):
                lines.append(
                    'megacache_requests_total{{protocol="{}",operation="{}",status="{}"}} {}'.format(
                        self._label(protocol),
                        self._label(operation),
                        self._label(status),
                        value,
                    )
                )
            lines.extend(
                [
                    "# HELP megacache_request_duration_seconds Request latency.",
                    "# TYPE megacache_request_duration_seconds histogram",
                ]
            )
            for protocol, operation in sorted(self._latency_sums):
                label = 'protocol="{}",operation="{}"'.format(
                    self._label(protocol), self._label(operation)
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
                    for (
                        item_protocol,
                        item_operation,
                        _,
                    ), value in self._request_counts.items()
                    if item_protocol == protocol
                    and item_operation == operation
                )
                lines.extend(
                    [
                        'megacache_request_duration_seconds_bucket{{{},le="+Inf"}} {}'.format(
                            label, count
                        ),
                        "megacache_request_duration_seconds_sum{{{}}} {}".format(
                            label,
                            self._latency_sums[(protocol, operation)],
                        ),
                        "megacache_request_duration_seconds_count{{{}}} {}".format(
                            label, count
                        ),
                    ]
                )
            return "\n".join(lines) + "\n"

    def _replicate_put_locked(
        self,
        key: str,
        value: Any,
        ttl_seconds: Optional[int],
        stale_seconds: Optional[int],
        tags: Tuple[str, ...],
        persistent: bool,
        profile: ConsistencyProfile,
    ) -> CacheResult:
        owners = self._ring.owners(key, self._replica_count)
        available = self._available_owners_locked(owners)
        required = profile.required(len(owners))
        if len(available) < required:
            raise QuorumError(
                self._quorum_message("write", required, len(available))
            )
        for node_id in available:
            self._prevalidate_put_locked(
                node_id,
                key,
                value,
                ttl_seconds,
                stale_seconds,
                tags,
                persistent,
            )
        checkpoints = self._capture_mutation_checkpoints_locked(
            available, (key,)
        )
        old_metadata = {
            node_id: self._metadata[node_id].get(key)
            for node_id in available
        }
        old_sequence = self._sequence
        version = self._next_version_locked()
        successes = []
        result = None
        for node_id in available:
            try:
                written = self._nodes[node_id].storage.put(
                    key=key,
                    value=value,
                    ttl_seconds=ttl_seconds,
                    stale_seconds=stale_seconds,
                    tags=tags,
                    persistent=persistent,
                )
                self._metadata[node_id][key] = _ReplicaMetadata(
                    version, False, tuple(sorted(set(tags)))
                )
                successes.append(node_id)
                if result is None:
                    result = written
            except BaseException as exc:
                checkpoint = checkpoints.pop(node_id)
                self._restore_one_backend_checkpoint_locked(
                    node_id, checkpoint
                )
                metadata = old_metadata[node_id]
                if metadata is None:
                    self._metadata[node_id].pop(key, None)
                else:
                    self._metadata[node_id][key] = metadata
                if isinstance(exc, (ValueError, TypeError)) or not isinstance(
                    exc, Exception
                ):
                    self._restore_backend_checkpoints_locked(checkpoints)
                    checkpoints.clear()
                    for restore_id, metadata in old_metadata.items():
                        if metadata is None:
                            self._metadata[restore_id].pop(key, None)
                        else:
                            self._metadata[restore_id][key] = metadata
                    self._sequence = old_sequence
                    raise
        if len(successes) < required:
            self._restore_backend_checkpoints_locked(checkpoints)
            checkpoints.clear()
            for node_id, metadata in old_metadata.items():
                if metadata is None:
                    self._metadata[node_id].pop(key, None)
                else:
                    self._metadata[node_id][key] = metadata
            self._sequence = old_sequence
            raise QuorumError(
                self._quorum_message("write", required, len(successes))
            )
        self._commit_backend_checkpoints_locked(checkpoints)
        checkpoints.clear()
        self._metrics["replicated_writes_total"] += 1
        assert result is not None
        return result

    def _replicate_storage_entry_locked(
        self,
        entry: StorageEntry,
        profile: ConsistencyProfile,
    ) -> None:
        owners = self._ring.owners(entry.key, self._replica_count)
        available = self._available_owners_locked(owners)
        required = profile.required(len(owners))
        if len(available) < required:
            raise QuorumError(
                self._quorum_message("restore", required, len(available))
            )
        version = self._next_version_locked()
        previous = self._capture_previous_locked(
            available, entry.key
        )
        successes = []
        for node_id in available:
            try:
                self._nodes[node_id].storage.restore_entries((entry,))
                self._metadata[node_id][entry.key] = _ReplicaMetadata(
                    version, False, entry.tags
                )
                successes.append(node_id)
            except (ValueError, TypeError):
                continue
        if len(successes) < required:
            self._rollback_locked(previous, successes, entry.key)
            raise QuorumError(
                self._quorum_message(
                    "restore", required, len(successes)
                )
            )

    def _delete_keys_transaction_locked(
        self,
        keys: Iterable[str],
        profile: ConsistencyProfile,
    ) -> int:
        normalized = tuple(keys)
        for key in normalized:
            self._validate_key(key)
        selected = tuple(dict.fromkeys(normalized))
        if not selected:
            return 0

        self._gc_tombstones_locked()
        availability = {}
        retained = self._tombstone_count_locked()
        risky_new_tombstones = 0
        for key in selected:
            owners = self._ring.owners(key, self._replica_count)
            available = self._available_owners_locked(owners)
            required = profile.required(len(owners))
            if len(available) < required:
                raise QuorumError(
                    self._quorum_message(
                        "delete", required, len(available)
                    )
                )
            availability[key] = (owners, available, required)
            latest = self._latest_metadata_locked(key)
            if (
                (latest is None or not latest.deleted)
                and len(available) < len(owners)
            ):
                risky_new_tombstones += 1
        if (
            retained + risky_new_tombstones
            > self._max_retained_tombstones
        ):
            raise ClusterError(
                "retained tombstone limit reached; recover or remove "
                "unavailable replicas before deleting more keys"
            )

        node_ids = tuple(
            sorted(
                {
                    node_id
                    for _, available, _ in availability.values()
                    for node_id in available
                }
            )
        )
        old_metadata = {
            node_id: {
                key: self._metadata[node_id].get(key)
                for key in selected
            }
            for node_id in self._metadata
        }
        old_sequence = self._sequence
        old_tombstone_gc_watermark = self._tombstone_gc_watermark
        old_known = set(self._known_keys)
        old_catalog = set(self._catalog_keys)
        old_key_tags = dict(self._key_tags)
        old_tag_keys = defaultdict(
            set,
            (
                (tag, set(tag_keys))
                for tag, tag_keys in self._tag_keys.items()
            ),
        )
        old_leases = dict(self._leases)
        old_lease_bytes = self._lease_bytes
        old_metrics = dict(self._metrics)
        checkpoints = self._capture_mutation_checkpoints_locked(
            node_ids, selected
        )
        try:
            existed = {
                key: self._read_locked(key, profile)[0].state != "miss"
                for key in selected
            }
            versions = {
                key: self._next_version_locked() for key in selected
            }
            successes = {key: set() for key in selected}
            for node_id in node_ids:
                node_keys = tuple(
                    key
                    for key in selected
                    if node_id in availability[key][1]
                )
                try:
                    for key in node_keys:
                        self._nodes[node_id].storage.delete(key)
                    for key in node_keys:
                        self._metadata[node_id][key] = _ReplicaMetadata(
                            versions[key], True
                        )
                        successes[key].add(node_id)
                except BaseException as exc:
                    checkpoint = checkpoints.pop(node_id)
                    self._restore_one_backend_checkpoint_locked(
                        node_id, checkpoint
                    )
                    for key, metadata in old_metadata[node_id].items():
                        if metadata is None:
                            self._metadata[node_id].pop(key, None)
                        else:
                            self._metadata[node_id][key] = metadata
                    if isinstance(
                        exc, (ValueError, TypeError)
                    ) or not isinstance(exc, Exception):
                        raise
            for key in selected:
                required = availability[key][2]
                if len(successes[key]) < required:
                    raise QuorumError(
                        self._quorum_message(
                            "delete", required, len(successes[key])
                        )
                    )
            for key in selected:
                self._replace_key_tags_locked(key, ())
                self._known_keys.discard(key)
                self._catalog_keys.add(key)
                self._remove_lease_locked(key)
            self._gc_tombstones_locked(selected)
            if (
                self._tombstone_count_locked()
                > self._max_retained_tombstones
            ):
                raise ClusterError(
                    "retained tombstone limit reached; recover or remove "
                    "unavailable replicas before deleting more keys"
                )
        except BaseException:
            self._restore_backend_checkpoints_locked(checkpoints)
            checkpoints.clear()
            for node_id, metadata_by_key in old_metadata.items():
                for key, metadata in metadata_by_key.items():
                    if metadata is None:
                        self._metadata[node_id].pop(key, None)
                    else:
                        self._metadata[node_id][key] = metadata
            self._sequence = old_sequence
            self._tombstone_gc_watermark = old_tombstone_gc_watermark
            self._known_keys = old_known
            self._catalog_keys = old_catalog
            self._key_tags = old_key_tags
            self._tag_keys = old_tag_keys
            self._leases = old_leases
            self._lease_bytes = old_lease_bytes
            self._metrics = defaultdict(int, old_metrics)
            raise
        self._commit_backend_checkpoints_locked(checkpoints)
        checkpoints.clear()
        self._metrics["replicated_deletes_total"] += len(selected)
        return sum(existed.values())

    def _read_locked(
        self, key: str, profile: ConsistencyProfile
    ) -> Tuple[CacheResult, Optional[str], Optional[_ReplicaMetadata]]:
        self._detect_failures_locked()
        owners = self._ring.owners(key, self._replica_count)
        available = self._available_owners_locked(owners)
        required = profile.required(len(owners))
        if len(available) < required:
            raise QuorumError(
                self._quorum_message("read", required, len(available))
            )
        responses = []
        for node_id in available:
            try:
                result = self._nodes[node_id].storage.get(key)
                metadata = self._metadata[node_id].get(key)
                responses.append((node_id, result, metadata))
                if profile is ConsistencyProfile.ONE:
                    break
            except (ValueError, TypeError):
                continue
        if len(responses) < required:
            raise QuorumError(
                self._quorum_message("read", required, len(responses))
            )
        winner = max(
            responses,
            key=lambda item: (
                (
                    VersionStamp(0, 0, item[0])
                    if item[2] is None
                    else item[2].version
                ),
                (
                    2
                    if item[2] is not None and item[2].deleted
                    else 1
                    if item[1].state != "miss"
                    else 0
                ),
            ),
        )
        node_id, result, metadata = winner
        if metadata is not None and metadata.deleted:
            result = CacheResult(state="miss")
        if profile is not ConsistencyProfile.ONE:
            self._read_repair_locked(key, winner, responses)
            self._gc_tombstones_locked((key,))
        if (
            result.state == "miss"
            and profile is not ConsistencyProfile.ONE
        ):
            self._known_keys.discard(key)
            self._replace_key_tags_locked(key, ())
        return result, node_id, metadata

    def _read_repair_locked(
        self,
        key: str,
        winner: Tuple[str, CacheResult, Optional[_ReplicaMetadata]],
        responses: Sequence[
            Tuple[str, CacheResult, Optional[_ReplicaMetadata]]
        ],
    ) -> None:
        winner_id, winner_result, winner_metadata = winner
        if winner_metadata is None:
            return
        entry = None
        if not winner_metadata.deleted and winner_result.state != "miss":
            exported = self._nodes[winner_id].storage.export_entries((key,))
            if exported:
                entry = exported[0]
        for node_id, replica_result, metadata in responses:
            if node_id == winner_id:
                continue
            if metadata is not None and metadata.version > winner_metadata.version:
                continue
            if metadata is not None and metadata.version == winner_metadata.version:
                if winner_metadata.deleted and replica_result.state == "miss":
                    continue
                if (
                    not winner_metadata.deleted
                    and replica_result.state != "miss"
                ):
                    continue
            try:
                if entry is None:
                    self._nodes[node_id].storage.delete(key)
                else:
                    self._nodes[node_id].storage.restore_entries((entry,))
                self._metadata[node_id][key] = winner_metadata
                self._metrics["read_repairs_total"] += 1
            except (ValueError, TypeError):
                self._metrics["read_repair_errors_total"] += 1

    def _plan_rebalance_locked(self) -> RebalancePlan:
        self._detect_failures_locked()
        self._reconcile_absent_catalog_keys_locked()
        joining = tuple(
            sorted(
                node_id
                for node_id, node in self._nodes.items()
                if node.status == "joining"
            )
        )
        departing = tuple(
            sorted(
                node_id
                for node_id, node in self._nodes.items()
                if node.status in ("draining", "removing")
            )
        )
        target_ids = tuple(
            sorted(
                (
                    set(self._ring.node_ids)
                    | set(joining)
                )
                - set(departing)
            )
        )
        if not target_ids:
            raise RebalanceError("rebalance would remove every ring member")
        target_ring = ConsistentHashRing(
            target_ids,
            virtual_nodes=self._virtual_nodes,
            version=self._ring.version + 1,
        )
        moves = []
        for key in sorted(self._catalog_keys):
            old_owners = self._ring.owners(key, self._replica_count)
            new_owners = target_ring.owners(key, self._replica_count)
            latest = self._latest_metadata_locked(key)
            candidates = [
                node_id
                for node_id in old_owners
                if self._healthy(self._nodes[node_id])
                and latest is not None
                and self._node_has_record_locked(node_id, key, latest)
            ]
            if not candidates:
                raise RebalanceError(
                    "no healthy latest-version source for key {}".format(
                        key
                    )
                )
            for target_id in new_owners:
                target_metadata = self._metadata[target_id].get(key)
                target_is_current = (
                    latest is not None
                    and target_metadata is not None
                    and target_metadata.version == latest.version
                    and self._node_has_record_locked(
                        target_id, key, latest
                    )
                )
                if target_id not in old_owners or not target_is_current:
                    moves.append(
                        RebalanceMove(key, candidates[0], target_id)
                    )
        payload = json.dumps(
            {
                "base": self._ring.version,
                "target": target_ring.fingerprint,
                "joining": joining,
                "departing": departing,
                "moves": [
                    (move.key, move.source_node_id, move.target_node_id)
                    for move in moves
                ],
                "term": self._term,
                "leader": self._leader_id,
            },
            separators=(",", ":"),
        ).encode("utf-8")
        return RebalancePlan(
            plan_id=hashlib.sha256(payload).hexdigest(),
            fence=FenceToken(self._term, self._leader_id),
            base_ring_version=self._ring.version,
            target_ring=target_ring,
            moves=tuple(moves),
            joining_nodes=joining,
            departing_nodes=departing,
        )

    def _reconcile_absent_catalog_keys_locked(self) -> None:
        for key in tuple(sorted(self._catalog_keys)):
            latest = self._latest_metadata_locked(key)
            if latest is None:
                self._catalog_keys.discard(key)
                continue
            if latest.deleted:
                continue
            owners = self._ring.owners(key, self._replica_count)
            available = self._available_owners_locked(owners)
            required = self._consistency.required(len(owners))
            if len(available) < required:
                continue
            absent = 0
            for node_id in available:
                metadata = self._metadata[node_id].get(key)
                if (
                    metadata is None
                    or metadata.version != latest.version
                    or metadata.deleted
                ):
                    continue
                try:
                    if self._nodes[node_id].storage.get(key).state == "miss":
                        absent += 1
                except Exception:
                    continue
            if absent >= required:
                self._delete_keys_transaction_locked(
                    (key,), self._consistency
                )

    def _gc_tombstones_locked(
        self, keys: Optional[Iterable[str]] = None
    ) -> int:
        selected = (
            tuple(self._catalog_keys)
            if keys is None
            else tuple(dict.fromkeys(keys))
        )
        removed = 0
        for key in selected:
            latest = self._latest_metadata_locked(key)
            if latest is None or not latest.deleted:
                continue
            owners = self._ring.owners(key, self._replica_count)
            if not all(
                self._healthy(self._nodes[node_id])
                and self._node_has_record_locked(node_id, key, latest)
                for node_id in owners
            ):
                continue
            for metadata in self._metadata.values():
                metadata.pop(key, None)
            if (
                self._tombstone_gc_watermark is None
                or latest.version > self._tombstone_gc_watermark
            ):
                self._tombstone_gc_watermark = latest.version
            self._catalog_keys.discard(key)
            self._known_keys.discard(key)
            self._replace_key_tags_locked(key, ())
            self._remove_lease_locked(key)
            removed += 1
        if removed:
            self._metrics["tombstones_collected_total"] += removed
        return removed

    def _tombstone_count_locked(self) -> int:
        count = 0
        for key in self._catalog_keys:
            latest = self._latest_metadata_locked(key)
            if latest is not None and latest.deleted:
                count += 1
        return count

    def _snapshot_session_locked(
        self, source_node_id: str, records: Sequence[SnapshotRecord]
    ) -> SnapshotSession:
        encoded_records = [
            _encode_snapshot_record(record) + b"\n"
            for record in records
        ]
        payload = b"".join(encoded_records)
        total_bytes = len(payload)
        if total_bytes > self._snapshot_payload_limit:
            raise SnapshotError(
                "snapshot exceeds configured payload limit"
            )
        chunks_payload = [
            payload[index : index + self._snapshot_chunk_bytes]
            for index in range(0, len(payload), self._snapshot_chunk_bytes)
        ]
        snapshot_id = hashlib.sha256(
            source_node_id.encode("utf-8")
            + str(self._ring.version).encode("ascii")
            + payload
        ).hexdigest()
        chunks = tuple(
            SnapshotChunk(
                snapshot_id=snapshot_id,
                index=index,
                payload=chunk,
                checksum=hashlib.sha256(chunk).hexdigest(),
            )
            for index, chunk in enumerate(chunks_payload)
        )
        manifest = SnapshotManifest(
            snapshot_id=snapshot_id,
            source_node_id=source_node_id,
            ring_version=self._ring.version,
            fence=FenceToken(self._term, self._leader_id),
            chunk_count=len(chunks),
            entry_count=len(records),
            total_bytes=len(payload),
            checksum=hashlib.sha256(payload).hexdigest(),
        )
        return SnapshotSession(
            manifest, chunks, self._snapshot_max_in_flight
        )

    def _bootstrap_locked(
        self, node_id: str, session: SnapshotSession
    ) -> None:
        node = self._node(node_id)
        if session.manifest.ring_version != self._ring.version:
            raise SnapshotError("snapshot uses a stale ring version")
        if session.manifest.fence != FenceToken(
            self._term, self._leader_id
        ):
            raise SnapshotError("snapshot uses a stale leadership fence")
        receiver = SnapshotReceiver(
            session.manifest, self._snapshot_payload_limit
        )
        while True:
            chunk = session.next_chunk()
            if chunk is None:
                break
            receiver.receive(chunk)
            session.acknowledge(chunk.index)
        records = receiver.records()
        if len({record.key for record in records}) != len(records):
            raise SnapshotError("snapshot contains duplicate keys")
        accepted = []
        for record in records:
            if (
                record.version.term > session.manifest.fence.term
                or (
                    record.version.term == session.manifest.fence.term
                    and record.version.leader_id
                    != session.manifest.fence.leader_id
                )
            ):
                raise SnapshotError(
                    "snapshot record is not bound to its leadership fence"
                )
            incoming_deleted = record.deleted or record.entry is None
            current = self._metadata[node_id].get(record.key)
            latest = self._latest_metadata_locked(record.key)
            if (
                current is None
                and latest is None
                and self._tombstone_gc_watermark is not None
                and record.version <= self._tombstone_gc_watermark
            ):
                continue
            if current is not None and current.version > record.version:
                continue
            if latest is not None and latest.version > record.version:
                continue
            if (
                latest is not None
                and latest.version == record.version
                and latest.deleted != incoming_deleted
            ):
                continue
            if current is not None and current.version == record.version:
                if current.deleted != incoming_deleted:
                    continue
                if current.deleted or self._node_has_record_locked(
                    node_id, record.key, current
                ):
                    if not current.deleted:
                        continue
            accepted.append(record)
        if not accepted:
            self._metrics["snapshot_bootstraps_total"] += 1
            return
        checkpoints = self._capture_backend_checkpoints_locked((node_id,))
        old_metadata = dict(self._metadata[node_id])
        try:
            live_entries = tuple(
                record.entry
                for record in accepted
                if not record.deleted and record.entry is not None
            )
            if live_entries:
                self._prevalidate_restore_locked(node_id, live_entries)
                node.storage.restore_entries(live_entries)
            for record in accepted:
                if record.deleted or record.entry is None:
                    node.storage.delete(record.key)
                self._metadata[node_id][record.key] = _ReplicaMetadata(
                    record.version,
                    record.deleted or record.entry is None,
                    () if record.entry is None else record.entry.tags,
                )
        except BaseException:
            self._restore_backend_checkpoints_locked(checkpoints)
            self._metadata[node_id] = old_metadata
            raise
        self._gc_tombstones_locked(
            record.key for record in accepted if record.deleted
        )
        self._metrics["snapshot_bootstraps_total"] += 1

    def _capture_previous_locked(
        self, node_ids: Iterable[str], *keys: str
    ) -> Dict[Tuple[str, str], Tuple[Tuple[StorageEntry, ...], Optional[_ReplicaMetadata]]]:
        selected_keys = keys
        if not selected_keys:
            raise ValueError("at least one key is required")
        previous = {}
        for node_id in node_ids:
            for key in selected_keys:
                previous[(node_id, key)] = (
                    self._nodes[node_id].storage.export_entries((key,)),
                    self._metadata[node_id].get(key),
                )
        return previous

    def _prevalidate_put_locked(
        self,
        node_id: str,
        key: str,
        value: Any,
        ttl_seconds: Optional[int],
        stale_seconds: Optional[int],
        tags: Tuple[str, ...],
        persistent: bool,
    ) -> None:
        validator = getattr(
            self._nodes[node_id].storage, "validate_put", None
        )
        if validator is not None:
            validator(
                key=key,
                value=value,
                ttl_seconds=ttl_seconds,
                stale_seconds=stale_seconds,
                tags=tags,
                persistent=persistent,
            )

    def _prevalidate_restore_locked(
        self, node_id: str, entries: Tuple[StorageEntry, ...]
    ) -> None:
        validator = getattr(
            self._nodes[node_id].storage,
            "validate_restore_entries",
            None,
        )
        if validator is not None:
            validator(entries)

    def _capture_backend_checkpoints_locked(
        self, node_ids: Iterable[str]
    ) -> Dict[str, Tuple[str, Any]]:
        checkpoints = {}
        for node_id in node_ids:
            storage = self._nodes[node_id].storage
            checkpoint = getattr(storage, "checkpoint", None)
            restore_checkpoint = getattr(
                storage, "restore_checkpoint", None
            )
            if checkpoint is not None and restore_checkpoint is not None:
                checkpoints[node_id] = ("native", checkpoint())
            else:
                checkpoints[node_id] = (
                    "portable",
                    tuple(storage.export_entries()),
                )
        return checkpoints

    def _capture_mutation_checkpoints_locked(
        self, node_ids: Iterable[str], keys: Tuple[str, ...]
    ) -> Dict[str, Tuple[str, Any]]:
        if len(keys) != 1:
            return self._capture_backend_checkpoints_locked(node_ids)
        checkpoints: Dict[str, Tuple[str, Any]] = {}
        try:
            for node_id in node_ids:
                storage = self._nodes[node_id].storage
                begin = getattr(storage, "begin_mutation_checkpoint", None)
                commit = getattr(storage, "commit_mutation_checkpoint", None)
                restore = getattr(storage, "restore_mutation_checkpoint", None)
                if (
                    callable(begin)
                    and callable(commit)
                    and callable(restore)
                ):
                    checkpoints[node_id] = (
                        "mutation",
                        begin(keys[0]),
                    )
                else:
                    checkpoints.update(
                        self._capture_backend_checkpoints_locked((node_id,))
                    )
        except BaseException:
            self._restore_backend_checkpoints_locked(checkpoints)
            raise
        return checkpoints

    def _restore_one_backend_checkpoint_locked(
        self, node_id: str, checkpoint: Tuple[str, Any]
    ) -> None:
        kind, state = checkpoint
        storage = self._nodes[node_id].storage
        if kind == "mutation":
            storage.restore_mutation_checkpoint(state)
            return
        if kind == "native":
            storage.restore_checkpoint(state)
            return
        storage.flush()
        if state:
            storage.restore_entries(state)

    def _restore_backend_checkpoints_locked(
        self, checkpoints: Dict[str, Tuple[str, Any]]
    ) -> None:
        for node_id, checkpoint in checkpoints.items():
            self._restore_one_backend_checkpoint_locked(
                node_id, checkpoint
            )

    def _commit_backend_checkpoints_locked(
        self, checkpoints: Dict[str, Tuple[str, Any]]
    ) -> None:
        for node_id, checkpoint in checkpoints.items():
            kind, state = checkpoint
            if kind == "mutation":
                self._nodes[node_id].storage.commit_mutation_checkpoint(
                    state
                )

    def _rollback_locked(
        self,
        previous: Dict[
            Tuple[str, str],
            Tuple[Tuple[StorageEntry, ...], Optional[_ReplicaMetadata]],
        ],
        node_ids: Iterable[str],
        key: str,
    ) -> None:
        for node_id in node_ids:
            self._restore_previous_key_locked(
                previous[(node_id, key)], node_id, key
            )

    def _restore_previous_key_locked(
        self,
        previous: Tuple[
            Tuple[StorageEntry, ...], Optional[_ReplicaMetadata]
        ],
        node_id: str,
        key: str,
    ) -> None:
        entries, metadata = previous
        self._nodes[node_id].storage.delete(key)
        if entries:
            self._nodes[node_id].storage.restore_entries(entries)
        if metadata is None:
            self._metadata[node_id].pop(key, None)
        else:
            self._metadata[node_id][key] = metadata

    def _available_owners_locked(
        self, owners: Iterable[str]
    ) -> Tuple[str, ...]:
        return tuple(
            node_id
            for node_id in owners
            if self._healthy(self._nodes[node_id])
        )

    def _healthy(self, node: ClusterNode) -> bool:
        return (
            node.status in ("active", "joining", "draining")
            and self._clock() - node.last_heartbeat
            <= self._heartbeat_timeout
        )

    def _detect_failures_locked(self) -> Tuple[str, ...]:
        failed = []
        now = self._clock()
        for node in self._nodes.values():
            if (
                node.status in ("active", "joining", "draining")
                and now - node.last_heartbeat > self._heartbeat_timeout
            ):
                node.status = "down"
                failed.append(node.node_id)
                self._metrics["node_failures_total"] += 1
        if failed:
            self._elect_leader_locked()
        return tuple(sorted(failed))

    def _elect_leader_locked(self) -> None:
        candidates = sorted(
            node_id
            for node_id in self._ring.node_ids
            if self._healthy(self._nodes[node_id])
            and self._nodes[node_id].status != "removing"
        )
        if self._leader_id in candidates:
            return
        self._term += 1
        self._leases.clear()
        self._lease_bytes = 0
        self._metrics["leadership_changes_total"] += 1
        if candidates:
            self._leader_id = candidates[0]

    def _assert_fence_locked(self, fence: FenceToken) -> None:
        if fence != FenceToken(self._term, self._leader_id):
            raise FenceError(
                "stale leadership fence; current term is {} led by {}".format(
                    self._term, self._leader_id
                )
            )

    def _validate_lease_locked(self, key: str, token: str) -> None:
        lease = self._leases.get(key)
        now = self._clock()
        if lease is None or lease.until <= now:
            self._remove_lease_locked(key)
            raise ValueError("lease is missing or expired")
        self._assert_fence_locked(lease.fence)
        if not secrets.compare_digest(lease.token, token):
            raise ValueError("lease token does not match")

    def _next_version_locked(self) -> VersionStamp:
        self._sequence += 1
        return VersionStamp(self._term, self._sequence, self._leader_id)

    def _remove_lease_locked(self, key: str) -> bool:
        lease = self._leases.pop(key, None)
        if lease is None:
            return False
        self._lease_bytes -= (
            len(key.encode("utf-8"))
            + len(lease.token.encode("ascii"))
            + 96
        )
        return True

    def _purge_expired_leases_locked(self, now: float) -> None:
        for key, lease in tuple(self._leases.items()):
            if lease.until <= now:
                self._remove_lease_locked(key)

    def _latest_metadata_locked(
        self, key: str
    ) -> Optional[_ReplicaMetadata]:
        values = [
            metadata[key]
            for metadata in self._metadata.values()
            if key in metadata
        ]
        return max(values, key=lambda item: item.version) if values else None

    def _node_has_record_locked(
        self,
        node_id: str,
        key: str,
        expected: _ReplicaMetadata,
    ) -> bool:
        metadata = self._metadata[node_id].get(key)
        if metadata is None or metadata.version != expected.version:
            return False
        if metadata.deleted:
            return not self._nodes[node_id].storage.export_entries((key,))
        return bool(
            self._nodes[node_id].storage.export_entries((key,))
        )

    def _replace_key_tags_locked(
        self, key: str, tags: Iterable[str]
    ) -> None:
        for tag in self._key_tags.get(key, ()):
            keys = self._tag_keys.get(tag)
            if keys is not None:
                keys.discard(key)
                if not keys:
                    self._tag_keys.pop(tag, None)
        normalized = tuple(sorted(set(tags)))
        if normalized:
            self._key_tags[key] = normalized
            for tag in normalized:
                self._tag_keys[tag].add(key)
        else:
            self._key_tags.pop(key, None)

    def _degraded_locked(self) -> bool:
        for key in self._catalog_keys:
            owners = self._ring.owners(key, self._replica_count)
            if len(self._available_owners_locked(owners)) < self._consistency.required(
                len(owners)
            ):
                return True
            latest = self._latest_metadata_locked(key)
            if latest is not None:
                for node_id in owners:
                    metadata = self._metadata[node_id].get(key)
                    if metadata is None or metadata.version < latest.version:
                        return True
        return any(
            not self._healthy(self._nodes[node_id])
            for node_id in self._ring.node_ids
        )

    def _profile(
        self, consistency: Optional[Any]
    ) -> ConsistencyProfile:
        return (
            self._consistency
            if consistency is None
            else ConsistencyProfile.parse(consistency)
        )

    def _node(self, node_id: str) -> ClusterNode:
        try:
            return self._nodes[node_id]
        except KeyError as exc:
            raise ClusterError(
                "unknown node {}".format(node_id)
            ) from exc

    @staticmethod
    def _quorum_message(
        operation: str, required: int, available: int
    ) -> str:
        return "{} quorum unavailable: required {}, received {}".format(
            operation, required, available
        )

    @staticmethod
    def _validate_key(key: str) -> None:
        if not isinstance(key, str) or not key or len(key) > 1024:
            raise ValueError("key must contain between 1 and 1024 characters")

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
            raise ValueError(
                "tags must contain between 1 and 256 UTF-8 bytes"
            )
        return normalized

    @staticmethod
    def _label(value: str) -> str:
        return value.replace("\\", "\\\\").replace("\n", "\\n").replace('"', '\\"')


def _encode_snapshot_record(record: SnapshotRecord) -> bytes:
    entry = record.entry
    document: Dict[str, Any] = {
        "key": record.key,
        "version": [
            record.version.term,
            record.version.sequence,
            record.version.leader_id,
        ],
        "deleted": record.deleted,
        "entry": None,
    }
    if entry is not None:
        if isinstance(entry.value, bytes):
            value = {
                "type": "bytes",
                "data": base64.b64encode(entry.value).decode("ascii"),
            }
        else:
            value = {"type": "json", "data": entry.value}
        document["entry"] = {
            "value": value,
            "fresh_for_seconds": entry.fresh_for_seconds,
            "stale_for_seconds": entry.stale_for_seconds,
            "tags": list(entry.tags),
            "persistent": entry.persistent,
        }
    try:
        return json.dumps(
            document,
            separators=(",", ":"),
            ensure_ascii=False,
            sort_keys=True,
        ).encode("utf-8")
    except (TypeError, ValueError) as exc:
        raise SnapshotError("snapshot value is not serializable") from exc


def _decode_snapshot_record(payload: bytes) -> SnapshotRecord:
    try:
        document = json.loads(payload.decode("utf-8"))
        key = document["key"]
        version_data = document["version"]
        version = VersionStamp(
            int(version_data[0]),
            int(version_data[1]),
            str(version_data[2]),
        )
        deleted = bool(document["deleted"])
        entry_data = document["entry"]
        entry = None
        if entry_data is not None:
            value_data = entry_data["value"]
            if value_data["type"] == "bytes":
                value = base64.b64decode(
                    value_data["data"], validate=True
                )
            elif value_data["type"] == "json":
                value = value_data["data"]
            else:
                raise ValueError("unknown snapshot value type")
            entry = StorageEntry(
                key=key,
                value=value,
                fresh_for_seconds=entry_data["fresh_for_seconds"],
                stale_for_seconds=entry_data["stale_for_seconds"],
                tags=tuple(entry_data["tags"]),
                persistent=bool(entry_data["persistent"]),
            )
        return SnapshotRecord(entry, key, version, deleted)
    except (
        KeyError,
        IndexError,
        TypeError,
        ValueError,
        UnicodeDecodeError,
    ) as exc:
        raise SnapshotError("snapshot record is malformed") from exc
