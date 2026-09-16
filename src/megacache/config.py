"""Environment-backed configuration."""

import math
import os
import socket
from dataclasses import dataclass
from typing import Optional, Tuple


def _positive_int(name: str, default: int) -> int:
    raw = os.getenv(name, str(default))
    try:
        value = int(raw)
    except ValueError as exc:
        raise ValueError("{} must be an integer".format(name)) from exc
    if value <= 0:
        raise ValueError("{} must be greater than zero".format(name))
    return value


def _optional_path(name: str) -> Optional[str]:
    return os.getenv(name) or None


def _positive_float(name: str, default: float) -> float:
    raw = os.getenv(name, str(default))
    try:
        value = float(raw)
    except ValueError as exc:
        raise ValueError("{} must be a number".format(name)) from exc
    if not math.isfinite(value) or value <= 0:
        raise ValueError(
            "{} must be a finite number greater than zero".format(name)
        )
    return value


def _choice(name: str, default: str, choices: Tuple[str, ...]) -> str:
    value = os.getenv(name, default).lower()
    if value not in choices:
        raise ValueError(
            "{} must be one of {}".format(name, ", ".join(choices))
        )
    return value


def _node_ids(local_node_id: str) -> Tuple[str, ...]:
    raw = os.getenv("MEGACACHE_CLUSTER_NODES", local_node_id)
    values = tuple(
        item.strip() for item in raw.split(",") if item.strip()
    )
    if not values:
        raise ValueError("MEGACACHE_CLUSTER_NODES must not be empty")
    if len(set(values)) != len(values):
        raise ValueError("MEGACACHE_CLUSTER_NODES contains duplicates")
    if local_node_id not in values:
        raise ValueError(
            "MEGACACHE_CLUSTER_NODES must include MEGACACHE_NODE_ID"
        )
    return values


@dataclass(frozen=True)
class Config:
    host: str
    port: int
    resp_host: str
    resp_port: int
    max_entries: int
    max_memory_bytes: int
    max_entry_bytes: int
    max_body_bytes: int
    default_ttl_seconds: int
    default_stale_seconds: int
    lease_seconds: int
    shutdown_grace_seconds: int
    api_key: Optional[str]
    tls_cert_file: Optional[str]
    tls_key_file: Optional[str]
    users_file: Optional[str]
    log_format: str
    node_id: str = "node-1"
    cluster_nodes: Tuple[str, ...] = ("node-1",)
    replica_count: int = 1
    virtual_nodes: int = 128
    consistency: str = "majority"
    heartbeat_interval_seconds: float = 2.0
    heartbeat_timeout_seconds: float = 10.0
    snapshot_payload_limit_bytes: int = 67_108_864
    snapshot_chunk_bytes: int = 262_144
    snapshot_max_in_flight: int = 2
    max_retained_tombstones: int = 10_000

    def __post_init__(self) -> None:
        if (
            not self.node_id
            or len(self.node_id.encode("utf-8")) > 128
            or any(character.isspace() for character in self.node_id)
        ):
            raise ValueError("node_id must be a non-whitespace identifier")
        if not self.cluster_nodes:
            raise ValueError("cluster_nodes must not be empty")
        if self.node_id not in self.cluster_nodes:
            raise ValueError("cluster_nodes must include node_id")
        if len(set(self.cluster_nodes)) != len(self.cluster_nodes):
            raise ValueError("cluster_nodes must not contain duplicates")
        if self.replica_count <= 0 or self.virtual_nodes <= 0:
            raise ValueError("replica_count and virtual_nodes must be positive")
        if self.consistency not in ("one", "majority", "all"):
            raise ValueError("consistency must be one, majority, or all")
        if (
            not math.isfinite(self.heartbeat_interval_seconds)
            or not math.isfinite(self.heartbeat_timeout_seconds)
            or self.heartbeat_interval_seconds <= 0
            or self.heartbeat_timeout_seconds <= 0
        ):
            raise ValueError(
                "heartbeat durations must be finite and positive"
            )
        if self.heartbeat_interval_seconds >= self.heartbeat_timeout_seconds:
            raise ValueError(
                "heartbeat interval must be less than heartbeat timeout"
            )
        if self.snapshot_chunk_bytes > self.snapshot_payload_limit_bytes:
            raise ValueError(
                "snapshot chunk size cannot exceed payload limit"
            )
        if (
            self.snapshot_payload_limit_bytes <= 0
            or self.snapshot_chunk_bytes <= 0
            or self.snapshot_max_in_flight <= 0
            or self.max_retained_tombstones <= 0
        ):
            raise ValueError("snapshot and tombstone limits must be positive")

    @classmethod
    def from_env(cls) -> "Config":
        node_id = os.getenv("MEGACACHE_NODE_ID") or socket.gethostname()
        return cls(
            host=os.getenv("MEGACACHE_HOST", "0.0.0.0"),
            port=_positive_int("MEGACACHE_PORT", 8080),
            resp_host=os.getenv("MEGACACHE_RESP_HOST", "0.0.0.0"),
            resp_port=_positive_int("MEGACACHE_RESP_PORT", 6380),
            max_entries=_positive_int("MEGACACHE_MAX_ENTRIES", 10_000),
            max_memory_bytes=_positive_int(
                "MEGACACHE_MAX_MEMORY_BYTES", 67_108_864
            ),
            max_entry_bytes=_positive_int(
                "MEGACACHE_MAX_ENTRY_BYTES", 1_048_576
            ),
            max_body_bytes=_positive_int("MEGACACHE_MAX_BODY_BYTES", 1_048_576),
            default_ttl_seconds=_positive_int(
                "MEGACACHE_DEFAULT_TTL_SECONDS", 300
            ),
            default_stale_seconds=_positive_int(
                "MEGACACHE_DEFAULT_STALE_SECONDS", 900
            ),
            lease_seconds=_positive_int("MEGACACHE_LEASE_SECONDS", 30),
            shutdown_grace_seconds=_positive_int(
                "MEGACACHE_SHUTDOWN_GRACE_SECONDS", 10
            ),
            api_key=os.getenv("MEGACACHE_API_KEY") or None,
            tls_cert_file=_optional_path("MEGACACHE_TLS_CERT_FILE"),
            tls_key_file=_optional_path("MEGACACHE_TLS_KEY_FILE"),
            users_file=_optional_path("MEGACACHE_USERS_FILE"),
            log_format=os.getenv("MEGACACHE_LOG_FORMAT", "json"),
            node_id=node_id,
            cluster_nodes=_node_ids(node_id),
            replica_count=_positive_int("MEGACACHE_REPLICA_COUNT", 1),
            virtual_nodes=_positive_int("MEGACACHE_VIRTUAL_NODES", 128),
            consistency=_choice(
                "MEGACACHE_CONSISTENCY",
                "majority",
                ("one", "majority", "all"),
            ),
            heartbeat_interval_seconds=_positive_float(
                "MEGACACHE_HEARTBEAT_INTERVAL_SECONDS", 2.0
            ),
            heartbeat_timeout_seconds=_positive_float(
                "MEGACACHE_HEARTBEAT_TIMEOUT_SECONDS", 10.0
            ),
            snapshot_payload_limit_bytes=_positive_int(
                "MEGACACHE_SNAPSHOT_PAYLOAD_LIMIT_BYTES", 67_108_864
            ),
            snapshot_chunk_bytes=_positive_int(
                "MEGACACHE_SNAPSHOT_CHUNK_BYTES", 262_144
            ),
            snapshot_max_in_flight=_positive_int(
                "MEGACACHE_SNAPSHOT_MAX_IN_FLIGHT", 2
            ),
            max_retained_tombstones=_positive_int(
                "MEGACACHE_MAX_RETAINED_TOMBSTONES", 10_000
            ),
        )
