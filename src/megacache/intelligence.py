"""Bounded, deterministic cache policy intelligence.

This module uses transparent counters and fixed formulas.  It deliberately
does not claim machine learning, and automated policy remains opt-in.
"""

from __future__ import annotations

import hashlib
import json
import logging
import math
import os
import secrets
import threading
import time
from collections import OrderedDict, defaultdict
from dataclasses import dataclass, replace
from typing import Any, Callable, Dict, Iterable, Mapping, Optional, Tuple

from .engine import CacheResult
from .storage import StorageBackend

LOG = logging.getLogger("megacache.intelligence")

@dataclass
class _Telemetry:
    cache_class: str
    accesses: int = 0
    hits: int = 0
    misses: int = 0
    stale_hits: int = 0
    mutations: int = 0
    loads: int = 0
    changes: int = 0
    load_latency_ms: float = 0.0
    last_access: float = 0.0
    last_load: float = 0.0
    last_fingerprint: Optional[str] = None
    last_load_fingerprint: Optional[str] = None
    last_replication: float = 0.0
    base_ttl_seconds: Optional[int] = None


class CacheIntelligence:
    """Storage facade providing bounded policy telemetry and explanations."""

    def __init__(
        self,
        storage: StorageBackend,
        *,
        enabled: bool = False,
        adaptive_ttl: bool = False,
        min_ttl_seconds: int = 5,
        max_ttl_seconds: int = 3600,
        telemetry_max_keys: int = 10_000,
        telemetry_max_classes: int = 128,
        hot_key_threshold: int = 100,
        hot_key_window_seconds: int = 60,
        hot_key_extra_replicas: int = 1,
        experiment_enabled: bool = False,
        experiment_id: str = "adaptive-ttl-v1",
        experiment_allocation_percent: int = 0,
        experiment_min_samples: int = 100,
        experiment_max_miss_regression: float = 0.05,
        state_file: Optional[str] = None,
        eviction_policy: Optional[str] = None,
        clock: Callable[[], float] = time.monotonic,
    ) -> None:
        if (
            min_ttl_seconds <= 0
            or max_ttl_seconds < min_ttl_seconds
            or telemetry_max_keys <= 0
            or telemetry_max_classes <= 0
            or hot_key_threshold <= 0
            or hot_key_window_seconds <= 0
            or hot_key_extra_replicas < 0
            or not 0 <= experiment_allocation_percent <= 100
            or experiment_min_samples <= 0
            or not math.isfinite(experiment_max_miss_regression)
            or experiment_max_miss_regression < 0
        ):
            raise ValueError("intelligence limits are invalid")
        if (
            not isinstance(experiment_id, str)
            or not experiment_id
            or len(experiment_id.encode("utf-8")) > 128
        ):
            raise ValueError("experiment_id must contain 1 to 128 UTF-8 bytes")
        if eviction_policy not in (None, "lru", "cost"):
            raise ValueError("eviction_policy must be lru or cost")
        self.storage = storage
        self.enabled = bool(enabled)
        self.adaptive_ttl_enabled = bool(adaptive_ttl and enabled)
        self.min_ttl_seconds = min_ttl_seconds
        self.max_ttl_seconds = max_ttl_seconds
        self.telemetry_max_keys = telemetry_max_keys
        self.telemetry_max_classes = telemetry_max_classes
        self.hot_key_threshold = hot_key_threshold
        self.hot_key_window_seconds = hot_key_window_seconds
        self.hot_key_extra_replicas = hot_key_extra_replicas
        self.experiment_enabled = bool(experiment_enabled and enabled)
        self.experiment_id = experiment_id
        self.experiment_allocation_percent = experiment_allocation_percent
        self.experiment_min_samples = experiment_min_samples
        self.experiment_max_miss_regression = experiment_max_miss_regression
        self.state_file = state_file
        self._configured_eviction_policy = eviction_policy
        self._clock = clock
        self._telemetry: "OrderedDict[str, _Telemetry]" = OrderedDict()
        self._classes: "OrderedDict[str, Dict[str, int]]" = OrderedDict()
        self._metrics: Dict[str, int] = defaultdict(int)
        for name in (
            "intelligence_telemetry_evictions_total",
            "intelligence_class_evictions_total",
            "intelligence_mutations_total",
            "intelligence_hot_replications_total",
            "intelligence_hot_replication_errors_total",
            "intelligence_hot_keys",
            "intelligence_experiment_rollbacks_total",
            "intelligence_state_load_errors_total",
            "intelligence_state_write_errors_total",
        ):
            self._metrics[name] = 0
        self._experiment = {
            "status": "running" if self.experiment_enabled else "disabled",
            "control_hits": 0,
            "control_misses": 0,
            "candidate_hits": 0,
            "candidate_misses": 0,
            "rollback_reason": None,
            "persistence_error": None,
        }
        self._audit = []
        self._lock = threading.RLock()
        self._load_state()

    def get(self, key: str) -> CacheResult:
        result = self.storage.get(key)
        if self.enabled:
            self._record_access(key, result.state)
        return result

    def mget(self, keys: Iterable[str]) -> Tuple[CacheResult, ...]:
        return tuple(self.get(key) for key in tuple(keys))

    def acquire_lease(self, key: str, force: bool = False) -> CacheResult:
        result = self.storage.acquire_lease(key, force=force)
        if self.enabled:
            self._record_access(key, result.state)
        return result

    def get_or_load(
        self,
        key: str,
        loader: Callable[[], Any],
        ttl_seconds: Optional[int] = None,
        stale_seconds: Optional[int] = None,
        tags: Iterable[str] = (),
    ) -> CacheResult:
        result = self.storage.get_or_load(
            key,
            loader,
            ttl_seconds=ttl_seconds,
            stale_seconds=stale_seconds,
            tags=tags,
        )
        if self.enabled:
            self._record_access(key, result.state)
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
        **kwargs: Any
    ) -> CacheResult:
        result = self.storage.put(
            key,
            value,
            ttl_seconds=ttl_seconds,
            stale_seconds=stale_seconds,
            tags=tags,
            lease_token=lease_token,
            persistent=persistent,
            **kwargs
        )
        if self.enabled:
            self._record_mutation(key, value)
        return result

    def put_origin_value(self, key: str, value: Any, **kwargs: Any) -> CacheResult:
        """Store an internal origin envelope without generic mutation telemetry."""
        return self.storage.put(key, value, **kwargs)

    def mset(self, values: Iterable[Tuple[str, Any]]) -> None:
        normalized = tuple(values)
        self.storage.mset(normalized)
        if self.enabled:
            for key, value in normalized:
                self._record_mutation(key, value)

    def delete(self, key: str, *args: Any, **kwargs: Any) -> bool:
        deleted = self.storage.delete(key, *args, **kwargs)
        if self.enabled and deleted:
            self._record_mutation(key, None)
        return deleted

    def delete_many(self, keys: Iterable[str]) -> int:
        normalized = tuple(keys)
        deleted = self.storage.delete_many(normalized)
        if self.enabled and deleted:
            for key in normalized:
                self._record_mutation(key, None)
        return deleted

    def adaptive_ttl(
        self, key: str, cache_class: str, configured_ttl: int
    ) -> int:
        """Return an opt-in TTL that never exceeds the configured freshness bound."""
        if not self.adaptive_ttl_enabled:
            return configured_ttl
        with self._lock:
            item = self._item(key, cache_class)
            item.base_ttl_seconds = configured_ttl
            if not self._candidate_for(key):
                return configured_ttl
            aggregate = self._classes.get(self._bounded_class(cache_class))
            loads = item.loads
            changes = item.changes
            accesses = item.accesses
            if loads < 2 and aggregate is not None:
                loads = aggregate.get("loads", 0)
                changes = aggregate.get("changes", 0)
                accesses = aggregate.get("accesses", 0)
            if loads < 2:
                return configured_ttl
            return self._calculate_adaptive_ttl(
                configured_ttl,
                loads,
                changes,
                accesses,
                self.min_ttl_seconds,
                self.max_ttl_seconds,
            )

    def record_origin_load(
        self,
        key: str,
        cache_class: str,
        duration_seconds: float,
        value: bytes,
    ) -> None:
        if not self.enabled:
            return
        fingerprint = hashlib.sha256(value).hexdigest()
        with self._lock:
            item = self._item(key, cache_class)
            item.cache_class = self._bounded_class(cache_class)
            changed = (
                item.last_load_fingerprint is not None
                and item.last_load_fingerprint != fingerprint
            )
            item.loads += 1
            item.changes += int(changed)
            latency_ms = max(0.0, float(duration_seconds) * 1000.0)
            item.load_latency_ms = (
                latency_ms
                if item.loads == 1
                else item.load_latency_ms * 0.8 + latency_ms * 0.2
            )
            item.last_load_fingerprint = fingerprint
            item.last_load = self._clock()
            aggregate = self._class(item.cache_class)
            aggregate["loads"] += 1
            aggregate["changes"] += int(changed)
        recorder = getattr(self.storage, "record_load_cost", None)
        if recorder is not None:
            recorder(key, latency_ms)

    def refresh_priority(self, key: str, cache_class: str) -> int:
        """Lower values are refreshed first; the result is fully deterministic."""
        if not self.enabled:
            return 1000
        with self._lock:
            item = self._telemetry.get(key)
            if item is None:
                return 1000
            freshness = max(0.0, self._clock() - item.last_access)
            popularity = min(500, item.accesses)
            cost = min(400, int(item.load_latency_ms))
            recency = max(0, 100 - int(freshness))
            return max(0, 1000 - popularity - cost - recency)

    def explain(self, key: str) -> Dict[str, Any]:
        descriptor = getattr(self.storage, "explain_entry", None)
        current = (
            {"state": self.storage.get(key).state}
            if descriptor is None
            else descriptor(key)
        )
        with self._lock:
            item = self._telemetry.get(key)
            telemetry = (
                {
                    "tracked": False,
                    "accesses": 0,
                    "hits": 0,
                    "misses": 0,
                    "stale_hits": 0,
                    "mutations": 0,
                    "loads": 0,
                    "changes": 0,
                    "change_rate": 0.0,
                    "load_latency_ms": 0.0,
                    "base_ttl_seconds": None,
                }
                if item is None
                else {
                    "tracked": True,
                    "class": item.cache_class,
                    "accesses": item.accesses,
                    "hits": item.hits,
                    "misses": item.misses,
                    "stale_hits": item.stale_hits,
                    "mutations": item.mutations,
                    "loads": item.loads,
                    "changes": item.changes,
                    "change_rate": round(
                        item.changes / max(1, item.loads - 1), 6
                    ),
                    "load_latency_ms": round(item.load_latency_ms, 3),
                    "base_ttl_seconds": item.base_ttl_seconds,
                }
            )
        state = current.get("state", "unknown")
        reasons = []
        if state == "miss":
            reasons.append("key is absent, expired, or evicted")
        elif state == "stale":
            reasons.append("fresh TTL elapsed but stale safety window remains")
        else:
            reasons.append("entry is inside its current freshness policy")
        if telemetry["tracked"] and telemetry["change_rate"] >= 0.25:
            reasons.append("observed origin changes favor a shorter TTL")
        if telemetry["accesses"] >= self.hot_key_threshold:
            reasons.append("bounded access count meets the hot-key threshold")
        if current.get("eviction_policy") == "cost":
            reasons.append(
                "eviction rank combines size, idle time, reuse, and measured load cost"
            )
        recommended_ttl = None
        configured_ttl = (
            telemetry.get("base_ttl_seconds")
            or current.get("configured_ttl_seconds")
        )
        if (
            isinstance(configured_ttl, (int, float))
            and not isinstance(configured_ttl, bool)
            and math.isfinite(configured_ttl)
            and configured_ttl > 0
        ):
            recommended_ttl = self.adaptive_ttl(
                key,
                telemetry.get("class", "default"),
                max(1, int(configured_ttl)),
            )
        lineage = current.get("lineage")
        return {
            "key": key,
            "current": current,
            "evidence": telemetry,
            "reasons": reasons,
            "recommended_policy": {
                "ttl_seconds": recommended_ttl,
                "eviction": current.get("eviction_policy", "lru"),
                "hot_replication": (
                    self.enabled
                    and telemetry["accesses"] >= self.hot_key_threshold
                    and self.hot_key_extra_replicas > 0
                ),
                "lineage": lineage,
                "refresh_priority": self.refresh_priority(
                    key, telemetry.get("class", "default")
                ),
                "automatic": self.adaptive_ttl_enabled,
                "experiment_arm": self._arm(key),
            },
        }

    def recommendations(
        self,
        limit: int = 100,
        key_filter: Optional[Callable[[str], bool]] = None,
    ) -> Dict[str, Any]:
        if isinstance(limit, bool) or not isinstance(limit, int) or limit <= 0:
            raise ValueError("limit must be a positive integer")
        limit = min(limit, 1000)
        with self._lock:
            ranked = [
                (key, replace(item))
                for key, item in sorted(
                    self._telemetry.items(),
                    key=lambda pair: (
                        -(pair[1].misses + pair[1].stale_hits),
                        -pair[1].load_latency_ms,
                        pair[0],
                    ),
                )
                if key_filter is None or key_filter(key)
            ]
            tracked_keys = len(ranked)
            ranked = ranked[:limit]
        values = []
        for key, item in ranked:
            reasons = []
            if item.loads >= 2 and item.changes / max(1, item.loads - 1) >= 0.25:
                reasons.append("shorten TTL because origin values changed often")
            if item.accesses >= self.hot_key_threshold:
                reasons.append("consider selective in-process replication")
            if item.load_latency_ms >= 100:
                reasons.append("retain longer under cost-aware eviction")
            if reasons:
                change_rate = item.changes / max(1, item.loads - 1)
                ttl_factor = (
                    0.25
                    if change_rate >= 0.5
                    else 0.5
                    if change_rate >= 0.25
                    else 0.75
                    if change_rate >= 0.1
                    else 1.0
                )
                values.append(
                    {
                        "key": key,
                        "class": item.cache_class,
                        "reasons": reasons,
                        "recommended_policy": {
                            "ttl_factor": ttl_factor,
                            "eviction": (
                                "retain"
                                if item.load_latency_ms >= 100
                                else "default"
                            ),
                            "hot_replication": (
                                item.accesses >= self.hot_key_threshold
                                and self.hot_key_extra_replicas > 0
                            ),
                        },
                    }
                )
        return {
            "generated_from": "bounded counters and deterministic formulas",
            "recommendations": values,
            "tracked_keys": tracked_keys,
            "key_limit": self.telemetry_max_keys,
        }

    def simulate(self, document: Mapping[str, Any]) -> Dict[str, Any]:
        if not isinstance(document, Mapping):
            raise ValueError("simulation input must be a JSON object")
        records = document.get("records", ())
        if not isinstance(records, list) or len(records) > self.telemetry_max_keys:
            raise ValueError("records must be a bounded JSON array")
        proposed = document.get("policy", {})
        if not isinstance(proposed, Mapping):
            raise ValueError("policy must be a JSON object")
        min_ttl = self._simulation_int(
            proposed, "min_ttl_seconds", self.min_ttl_seconds
        )
        max_ttl = self._simulation_int(
            proposed, "max_ttl_seconds", self.max_ttl_seconds
        )
        if max_ttl < min_ttl:
            raise ValueError("max_ttl_seconds must be at least min_ttl_seconds")
        eviction_policy = proposed.get("eviction_policy", "lru")
        if eviction_policy not in ("lru", "cost"):
            raise ValueError("eviction_policy must be lru or cost")
        capacity_entries = self._simulation_int(
            proposed,
            "capacity_entries",
            max(1, len(records)),
        )
        origin_load_delta = 0.0
        freshness_violations = 0
        evaluated = []
        for record in records:
            if not isinstance(record, Mapping):
                raise ValueError("simulation records must be JSON objects")
            key = record.get("key")
            if (
                key is not None
                and (
                    not isinstance(key, str)
                    or not key
                    or len(key.encode("utf-8")) > 1024
                )
            ):
                raise ValueError(
                    "simulation record key must contain 1 to 1024 UTF-8 bytes"
                )
            base = self._simulation_int(record, "base_ttl_seconds", 300)
            loads = self._simulation_int(record, "loads", 0, allow_zero=True)
            changes = self._simulation_int(
                record, "changes", 0, allow_zero=True
            )
            accesses = self._simulation_int(
                record, "accesses", 0, allow_zero=True
            )
            size_bytes = self._simulation_int(
                record, "size_bytes", 1, allow_zero=False
            )
            latency_ms = self._simulation_number(
                record, "load_latency_ms", 0.0
            )
            idle_seconds = self._simulation_number(
                record, "last_access_age_seconds", 0.0
            )
            rate = changes / max(1, loads - 1)
            ttl = self._calculate_adaptive_ttl(
                base,
                loads,
                changes,
                accesses,
                min_ttl,
                max_ttl,
            )
            if ttl > base:
                freshness_violations += 1
            origin_load_delta += accesses * ((1.0 / ttl) - (1.0 / base))
            evaluated.append(
                {
                    "key": key,
                    "base_ttl_seconds": base,
                    "simulated_ttl_seconds": ttl,
                    "change_rate": round(rate, 6),
                    "eviction_score": round(
                        ((1.0 + math.log1p(accesses))
                        * (1.0 + math.log1p(latency_ms)))
                        / (
                            math.sqrt(size_bytes)
                            * (1.0 + idle_seconds)
                        ),
                        6,
                    ),
                }
            )
        eviction_count = max(0, len(evaluated) - capacity_entries)
        baseline_evicted = [
            item["key"] for item in evaluated[:eviction_count]
        ]
        if eviction_policy == "cost":
            proposed_evicted = [
                item["key"]
                for item in sorted(
                    evaluated,
                    key=lambda item: (
                        item["eviction_score"],
                        "" if item["key"] is None else item["key"],
                    ),
                )[:eviction_count]
            ]
        else:
            proposed_evicted = baseline_evicted
        return {
            "dry_run": True,
            "records": evaluated,
            "summary": {
                "evaluated": len(evaluated),
                "configured_freshness_bound_violations": freshness_violations,
                "estimated_relative_origin_load_delta": round(
                    origin_load_delta, 6
                ),
            },
            "eviction": {
                "policy": eviction_policy,
                "capacity_entries": capacity_entries,
                "baseline_lru_evicted": baseline_evicted,
                "proposed_evicted": proposed_evicted,
            },
            "activated": False,
        }

    def experiment_status(self) -> Dict[str, Any]:
        with self._lock:
            result = dict(self._experiment)
            result.update(
                {
                    "experiment_id": self.experiment_id,
                    "allocation_percent": self.experiment_allocation_percent,
                    "minimum_samples_per_arm": self.experiment_min_samples,
                    "maximum_miss_rate_regression": self.experiment_max_miss_regression,
                    "allocation": "sha256(experiment_id + NUL + key) modulo 100",
                    "audit": list(self._audit),
                }
            )
            return result

    def stats(self) -> Dict[str, int]:
        result = dict(self.storage.stats())
        with self._lock:
            result.update(self._metrics)
            result["intelligence_tracked_keys"] = len(self._telemetry)
            result["intelligence_tracked_classes"] = len(self._classes)
            result["intelligence_experiment_rolled_back"] = int(
                self._experiment["status"] == "rolled_back"
            )
        return result

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
        with self._lock:
            value["intelligence"] = {
                "enabled": self.enabled,
                "adaptive_ttl": self.adaptive_ttl_enabled,
                "eviction_policy": self._eviction_policy(),
                "tracked_keys": len(self._telemetry),
                "tracked_key_limit": self.telemetry_max_keys,
                "tracked_classes": len(self._classes),
                "tracked_class_limit": self.telemetry_max_classes,
                "hot_replication_scope": "in-process",
                "experiment_status": self._experiment["status"],
            }
        return value

    def prometheus_metrics(self) -> str:
        lines = [self.storage.prometheus_metrics().rstrip("\n")]
        for name, value in sorted(self.stats().items()):
            if not name.startswith("intelligence_"):
                continue
            metric_type = (
                "gauge"
                if name in {
                    "intelligence_tracked_keys",
                    "intelligence_tracked_classes",
                    "intelligence_hot_keys",
                    "intelligence_experiment_rolled_back",
                }
                else "counter"
            )
            lines.extend(
                [
                    "# HELP megacache_{} Bounded cache intelligence metric.".format(name),
                    "# TYPE megacache_{} {}".format(name, metric_type),
                    "megacache_{} {}".format(name, value),
                ]
            )
        return "\n".join(lines) + "\n"

    def __getattr__(self, name: str) -> Any:
        return getattr(self.storage, name)

    def _record_access(self, key: str, state: str) -> None:
        replicate = False
        demote = False
        with self._lock:
            item = self._item(key, self._class_for_key(key))
            now = self._clock()
            if now - item.last_access > self.hot_key_window_seconds:
                demote = item.accesses >= self.hot_key_threshold
                item.accesses = 0
            item.accesses += 1
            item.last_access = now
            if state in ("miss", "lease", "loading"):
                item.misses += 1
            elif state in ("stale", "stale_lease"):
                item.stale_hits += 1
            else:
                item.hits += 1
            aggregate = self._class(item.cache_class)
            aggregate["accesses"] += 1
            aggregate[
                "misses"
                if state in ("miss", "lease", "loading")
                else "hits"
            ] += 1
            self._record_experiment_locked(key, state)
            replicate = (
                self.hot_key_extra_replicas > 0
                and item.accesses >= self.hot_key_threshold
                and now - item.last_replication >= self.hot_key_window_seconds
            )
            if replicate:
                item.last_replication = now
        if demote:
            release = getattr(self.storage, "release_hot_key", None)
            if release is not None:
                try:
                    release(key)
                except Exception:
                    with self._lock:
                        self._metrics[
                            "intelligence_hot_replication_errors_total"
                        ] += 1
        if replicate:
            promote = getattr(self.storage, "replicate_hot_key", None)
            if promote is not None:
                try:
                    added = promote(key, self.hot_key_extra_replicas)
                except Exception:
                    with self._lock:
                        self._metrics[
                            "intelligence_hot_replication_errors_total"
                        ] += 1
                    added = 0
                with self._lock:
                    self._metrics["intelligence_hot_replications_total"] += int(
                        added
                    )
                    self._metrics["intelligence_hot_keys"] = sum(
                        1
                        for value in self._telemetry.values()
                        if value.accesses >= self.hot_key_threshold
                    )
                if added:
                    LOG.info(
                        "hot-key replicas updated",
                        extra={
                            "operation": "hot_replication",
                            "replicas_added": int(added),
                            "distribution_scope": "in-process",
                        },
                    )

    def _record_mutation(self, key: str, value: Any) -> None:
        try:
            if isinstance(value, bytes):
                encoded = value
            else:
                encoded = json.dumps(
                    value, sort_keys=True, separators=(",", ":")
                ).encode("utf-8")
        except (TypeError, ValueError):
            encoded = repr(type(value)).encode("ascii", "replace")
        fingerprint = hashlib.sha256(encoded).hexdigest()
        with self._lock:
            item = self._item(key, self._class_for_key(key))
            if (
                item.last_fingerprint is not None
                and item.last_fingerprint != fingerprint
            ):
                item.changes += 1
            item.last_fingerprint = fingerprint
            item.mutations += 1
            self._metrics["intelligence_mutations_total"] += 1

    def _eviction_policy(self) -> str:
        if self._configured_eviction_policy is not None:
            return self._configured_eviction_policy
        value: Any = self.storage
        for _ in range(4):
            policy = getattr(value, "_eviction_policy", None)
            if policy in ("lru", "cost"):
                return policy
            value = getattr(value, "storage", None)
            if value is None:
                break
        return "coordinator-managed"

    def _item(self, key: str, cache_class: str) -> _Telemetry:
        item = self._telemetry.get(key)
        if item is None:
            if len(self._telemetry) >= self.telemetry_max_keys:
                evicted_key, _ = self._telemetry.popitem(last=False)
                release = getattr(self.storage, "release_hot_key", None)
                if release is not None:
                    try:
                        release(evicted_key)
                    except Exception:
                        self._metrics[
                            "intelligence_hot_replication_errors_total"
                        ] += 1
                self._metrics["intelligence_telemetry_evictions_total"] += 1
            item = _Telemetry(self._bounded_class(cache_class))
            self._telemetry[key] = item
        else:
            self._telemetry.move_to_end(key)
        return item

    def _class(self, cache_class: str) -> Dict[str, int]:
        bounded = self._bounded_class(cache_class)
        value = self._classes.get(bounded)
        if value is None:
            if len(self._classes) >= self.telemetry_max_classes:
                self._classes.popitem(last=False)
                self._metrics["intelligence_class_evictions_total"] += 1
            value = {
                "loads": 0,
                "changes": 0,
                "accesses": 0,
                "hits": 0,
                "misses": 0,
            }
            self._classes[bounded] = value
        else:
            self._classes.move_to_end(bounded)
        return value

    @staticmethod
    def _class_for_key(key: str) -> str:
        prefix = key.split(":", 1)[0]
        return prefix if prefix and len(prefix.encode("utf-8")) <= 64 else "other"

    @staticmethod
    def _bounded_class(value: str) -> str:
        if not isinstance(value, str) or not value:
            return "default"
        encoded = value.encode("utf-8")
        return value if len(encoded) <= 64 else hashlib.sha256(encoded).hexdigest()[:16]

    def _arm(self, key: str) -> str:
        if (
            not self.experiment_enabled
            or self._experiment["status"] != "running"
        ):
            return "control"
        digest = hashlib.sha256(
            (self.experiment_id + "\0" + key).encode("utf-8")
        ).digest()
        return (
            "candidate"
            if int.from_bytes(digest[:8], "big") % 100
            < self.experiment_allocation_percent
            else "control"
        )

    def _candidate_for(self, key: str) -> bool:
        if not self.experiment_enabled:
            return True
        return self._arm(key) == "candidate"

    def _record_experiment_locked(self, key: str, state: str) -> None:
        if (
            not self.experiment_enabled
            or self._experiment["status"] != "running"
        ):
            return
        arm = self._arm(key)
        outcome = (
            "misses"
            if state in ("miss", "lease", "loading")
            else "hits"
        )
        field = "{}_{}".format(arm, outcome)
        self._experiment[field] += 1
        control_total = (
            self._experiment["control_hits"]
            + self._experiment["control_misses"]
        )
        candidate_total = (
            self._experiment["candidate_hits"]
            + self._experiment["candidate_misses"]
        )
        if (
            control_total < self.experiment_min_samples
            or candidate_total < self.experiment_min_samples
        ):
            return
        control_rate = self._experiment["control_misses"] / control_total
        candidate_rate = self._experiment["candidate_misses"] / candidate_total
        if candidate_rate > control_rate + self.experiment_max_miss_regression:
            reason = (
                "candidate miss rate {:.6f} exceeded control {:.6f} plus "
                "guardrail {:.6f}"
            ).format(
                candidate_rate,
                control_rate,
                self.experiment_max_miss_regression,
            )
            self._experiment["status"] = "rolled_back"
            self._experiment["rollback_reason"] = reason
            self._audit.append(
                {
                    "decision": "automatic_rollback",
                    "reason": reason,
                    "timestamp": int(time.time()),
                }
            )
            self._audit = self._audit[-100:]
            self._metrics["intelligence_experiment_rollbacks_total"] += 1
            LOG.warning(
                "cache policy experiment rolled back",
                extra={
                    "operation": "experiment_rollback",
                    "experiment_id": self.experiment_id,
                    "reason": reason,
                },
            )
            self._save_state_locked()

    def _load_state(self) -> None:
        if not self.state_file or not os.path.exists(self.state_file):
            return
        try:
            if os.path.getsize(self.state_file) > 1_048_576:
                raise ValueError("intelligence state exceeds size limit")
            with open(self.state_file, "r", encoding="utf-8") as handle:
                document = json.load(handle)
            if (
                isinstance(document, dict)
                and document.get("experiment_id") == self.experiment_id
                and document.get("status") == "rolled_back"
            ):
                self._experiment["status"] = "rolled_back"
                self._experiment["rollback_reason"] = document.get(
                    "rollback_reason"
                )
                audit = document.get("audit", [])
                if isinstance(audit, list):
                    self._audit = audit[-100:]
        except (OSError, ValueError, TypeError):
            self._metrics["intelligence_state_load_errors_total"] += 1

    def _save_state_locked(self) -> None:
        if not self.state_file:
            return
        document = {
            "version": 1,
            "experiment_id": self.experiment_id,
            "status": self._experiment["status"],
            "rollback_reason": self._experiment["rollback_reason"],
            "audit": self._audit,
        }
        path = os.path.abspath(self.state_file)
        parent = os.path.dirname(path)
        pending = None
        descriptor = None
        directory_descriptor = None
        try:
            if parent:
                os.makedirs(parent, mode=0o700, exist_ok=True)
            directory_flags = os.O_RDONLY
            directory_flags |= getattr(os, "O_CLOEXEC", 0)
            directory_flags |= getattr(os, "O_DIRECTORY", 0)
            directory_descriptor = os.open(parent or ".", directory_flags)
            flags = os.O_WRONLY | os.O_CREAT | os.O_TRUNC
            flags |= os.O_EXCL | getattr(os, "O_CLOEXEC", 0)
            flags |= getattr(os, "O_NOFOLLOW", 0)
            for _ in range(10):
                candidate = "{}.{}.next".format(
                    path, secrets.token_hex(16)
                )
                try:
                    descriptor = os.open(candidate, flags, 0o600)
                    pending = candidate
                    break
                except FileExistsError:
                    continue
            if descriptor is None or pending is None:
                raise OSError("could not create a unique state file")
            os.fchmod(descriptor, 0o600)
            with os.fdopen(descriptor, "w", encoding="utf-8") as handle:
                descriptor = None
                json.dump(document, handle, sort_keys=True, separators=(",", ":"))
                handle.flush()
                os.fsync(handle.fileno())
            os.replace(pending, path)
            pending = None
            os.fsync(directory_descriptor)
            self._experiment["persistence_error"] = None
        except Exception:
            self._experiment["persistence_error"] = (
                "rollback remains disabled in this process, but the decision "
                "may not survive restart because state persistence failed"
            )
            if descriptor is not None:
                try:
                    os.close(descriptor)
                except OSError:
                    pass
            self._metrics["intelligence_state_write_errors_total"] += 1
            LOG.exception("failed to persist cache policy rollback state")
        finally:
            if directory_descriptor is not None:
                try:
                    os.close(directory_descriptor)
                except OSError:
                    pass
            if pending is not None:
                try:
                    os.unlink(pending)
                except OSError:
                    pass

    @staticmethod
    def _calculate_adaptive_ttl(
        configured_ttl: int,
        loads: int,
        changes: int,
        accesses: int,
        min_ttl: int,
        max_ttl: int,
    ) -> int:
        if loads < 2:
            return configured_ttl
        change_rate = changes / max(1, loads - 1)
        if change_rate >= 0.5:
            factor = 0.25
        elif change_rate >= 0.25:
            factor = 0.5
        elif change_rate >= 0.1:
            factor = 0.75
        else:
            factor = 1.0
        reuse_per_load = accesses / max(1, loads)
        if reuse_per_load < 1:
            factor = min(factor, 0.5)
        elif reuse_per_load < 3:
            factor = min(factor, 0.75)
        value = int(round(configured_ttl * factor))
        return max(
            1,
            min(
                configured_ttl,
                max_ttl,
                max(min_ttl, value),
            ),
        )

    @staticmethod
    def _simulation_int(
        value: Mapping[str, Any],
        name: str,
        default: int,
        *,
        allow_zero: bool = False,
    ) -> int:
        parsed = value.get(name, default)
        if (
            isinstance(parsed, bool)
            or not isinstance(parsed, int)
            or parsed < (0 if allow_zero else 1)
        ):
            raise ValueError("{} must be a {} integer".format(
                name, "non-negative" if allow_zero else "positive"
            ))
        return parsed

    @staticmethod
    def _simulation_number(
        value: Mapping[str, Any], name: str, default: float
    ) -> float:
        parsed = value.get(name, default)
        if (
            isinstance(parsed, bool)
            or not isinstance(parsed, (int, float))
            or not math.isfinite(parsed)
            or parsed < 0
        ):
            raise ValueError("{} must be a finite non-negative number".format(name))
        return float(parsed)
