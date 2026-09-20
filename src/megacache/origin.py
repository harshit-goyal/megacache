"""Declarative, bounded, SSRF-safe HTTP origin fetching."""

from __future__ import annotations

import http.client
import inspect
import base64
import hashlib
import ipaddress
import json
import math
import posixpath
import queue
import random
import re
import socket
import ssl
import threading
import time
import weakref
from dataclasses import asdict, dataclass
from enum import Enum
from typing import Any, Callable, Dict, Iterable, Mapping, Optional, Sequence, Tuple
from urllib.parse import quote, unquote, urlsplit

from .engine import CacheResult
from .storage import StorageBackend


_NAME = re.compile(r"^[A-Za-z0-9][A-Za-z0-9_.-]{0,63}$")
_HEADER_NAME = re.compile(r"^[!#$%&'*+\-.^_`|~0-9A-Za-z]+$")
_NEGATIVE_MARKER = "$megacache_origin_negative_v1"
_VALUE_MARKER = "$megacache_origin_value_v1"
_FORBIDDEN_HEADERS = {
    "connection",
    "content-length",
    "host",
    "proxy-authorization",
    "transfer-encoding",
}
_RETRYABLE_STATUS = {408, 425, 429, 500, 502, 503, 504}
_TRANSITION_NETWORKS = tuple(
    ipaddress.ip_network(value)
    for value in (
        "::/96",
        "::ffff:0:0/96",
        "::ffff:0:0:0/96",
        "64:ff9b::/96",
        "64:ff9b:1::/48",
        "2001::/32",
        "2002::/16",
    )
)
_LOW32_EMBEDDED_NETWORKS = tuple(
    ipaddress.ip_network(value)
    for value in (
        "::/96",
        "::ffff:0:0/96",
        "::ffff:0:0:0/96",
        "64:ff9b::/96",
        "64:ff9b:1::/48",
    )
)


class OriginError(Exception):
    """Base class for safe origin operation failures."""


class OriginPolicyError(OriginError, ValueError):
    """An origin definition or requested path violates policy."""


class OriginUnavailable(OriginError):
    """The origin could not produce a usable response."""


class OriginOverloaded(OriginUnavailable):
    """Admission control rejected an origin request."""


class CircuitOpen(OriginUnavailable):
    """The origin circuit breaker is open."""


class _NonRetryableResponse(OriginUnavailable):
    pass


class BreakerState(str, Enum):
    CLOSED = "closed"
    OPEN = "open"
    HALF_OPEN = "half_open"


@dataclass(frozen=True)
class HTTPOrigin:
    name: str
    scheme: str
    host: str
    port: int
    allowed_hosts: Tuple[str, ...]
    allowed_ports: Tuple[int, ...]
    allowed_path_prefixes: Tuple[str, ...]
    allowed_ip_networks: Tuple[Any, ...] = ()
    headers: Tuple[Tuple[str, str], ...] = ()
    ttl_seconds: int = 300
    stale_while_revalidate_seconds: int = 0
    stale_if_error_seconds: int = 0
    refresh_ahead_seconds: int = 0
    negative_ttl_seconds: int = 30
    negative_statuses: Tuple[int, ...] = (404, 410)
    timeout_seconds: float = 5.0
    queue_timeout_seconds: float = 1.0
    max_response_bytes: int = 1_048_576
    max_concurrency: int = 8
    max_queue: int = 64
    retry_attempts: int = 2
    retry_backoff_seconds: float = 0.05
    retry_max_backoff_seconds: float = 1.0
    retry_jitter: float = 0.2
    retry_budget_capacity: int = 32
    retry_budget_refill_per_second: float = 1.0
    breaker_failure_threshold: int = 5
    breaker_open_seconds: float = 30.0
    breaker_half_open_requests: int = 1
    tags: Tuple[str, ...] = ()

    @classmethod
    def from_dict(cls, value: Mapping[str, Any]) -> "HTTPOrigin":
        if not isinstance(value, Mapping):
            raise OriginPolicyError("origin definitions must be JSON objects")
        name = value.get("name")
        base_url = value.get("base_url")
        if not isinstance(name, str) or not _NAME.match(name):
            raise OriginPolicyError(
                "origin name must contain 1 to 64 safe identifier characters"
            )
        if not isinstance(base_url, str):
            raise OriginPolicyError(
                "origin {} requires base_url".format(name)
            )
        parsed = urlsplit(base_url)
        if (
            parsed.scheme.lower() not in ("http", "https")
            or parsed.hostname is None
            or parsed.username is not None
            or parsed.password is not None
            or parsed.query
            or parsed.fragment
        ):
            raise OriginPolicyError(
                "origin {} base_url must be an http(s) authority and path".format(
                    name
                )
            )
        if parsed.path not in ("", "/"):
            raise OriginPolicyError(
                "origin {} base_url must not contain a path; use allowed_path_prefixes".format(
                    name
                )
            )
        scheme = parsed.scheme.lower()
        host = _normalize_host(parsed.hostname)
        try:
            port = parsed.port or (443 if scheme == "https" else 80)
        except ValueError as exc:
            raise OriginPolicyError(
                "origin {} has an invalid port".format(name)
            ) from exc
        allowed_hosts = tuple(
            _normalize_host(item)
            for item in _string_array(
                value.get("allowed_hosts"), "allowed_hosts", required=True
            )
        )
        allowed_ports = tuple(
            _integer_array(
                value.get("allowed_ports"), "allowed_ports", required=True
            )
        )
        if host not in allowed_hosts:
            raise OriginPolicyError(
                "origin {} base host is not explicitly allowed".format(name)
            )
        if port not in allowed_ports:
            raise OriginPolicyError(
                "origin {} base port is not explicitly allowed".format(name)
            )
        prefixes = tuple(
            _validate_prefix(item)
            for item in _string_array(
                value.get("allowed_path_prefixes"),
                "allowed_path_prefixes",
                required=True,
            )
        )
        networks = []
        for item in _string_array(
            value.get("allowed_ip_networks", ()),
            "allowed_ip_networks",
        ):
            try:
                networks.append(ipaddress.ip_network(item, strict=True))
            except ValueError as exc:
                raise OriginPolicyError(
                    "origin {} contains an invalid allowed IP network".format(
                        name
                    )
                ) from exc
        headers_value = value.get("headers", {})
        if not isinstance(headers_value, Mapping):
            raise OriginPolicyError("origin headers must be a JSON object")
        headers = []
        for header, header_value in headers_value.items():
            if (
                not isinstance(header, str)
                or not isinstance(header_value, str)
                or header.lower() in _FORBIDDEN_HEADERS
                or not _HEADER_NAME.match(header)
                or any(
                    ord(character) < 32 or ord(character) == 127
                    for character in header_value
                )
            ):
                raise OriginPolicyError(
                    "origin {} contains an unsafe fixed header".format(name)
                )
            headers.append((header, header_value))
        negative_statuses = tuple(
            _integer_array(
                value.get("negative_statuses", (404, 410)),
                "negative_statuses",
            )
        )
        if any(status < 400 or status > 499 for status in negative_statuses):
            raise OriginPolicyError(
                "negative_statuses must contain HTTP 4xx status codes"
            )
        tags = tuple(_string_array(value.get("tags", ()), "tags"))
        if len(tags) > 100 or any(
            len(tag.encode("utf-8")) > 256 for tag in tags
        ):
            raise OriginPolicyError(
                "tags must contain at most 100 values of 256 UTF-8 bytes"
            )
        origin = cls(
            name=name,
            scheme=scheme,
            host=host,
            port=port,
            allowed_hosts=allowed_hosts,
            allowed_ports=allowed_ports,
            allowed_path_prefixes=prefixes,
            allowed_ip_networks=tuple(networks),
            headers=tuple(headers),
            ttl_seconds=_positive_int(value, "ttl_seconds", 300),
            stale_while_revalidate_seconds=_non_negative_int(
                value, "stale_while_revalidate_seconds", 0
            ),
            stale_if_error_seconds=_non_negative_int(
                value, "stale_if_error_seconds", 0
            ),
            refresh_ahead_seconds=_non_negative_int(
                value, "refresh_ahead_seconds", 0
            ),
            negative_ttl_seconds=_positive_int(
                value, "negative_ttl_seconds", 30
            ),
            negative_statuses=negative_statuses,
            timeout_seconds=_positive_float(value, "timeout_seconds", 5.0),
            queue_timeout_seconds=_positive_float(
                value, "queue_timeout_seconds", 1.0
            ),
            max_response_bytes=_positive_int(
                value, "max_response_bytes", 1_048_576
            ),
            max_concurrency=_positive_int(value, "max_concurrency", 8),
            max_queue=_non_negative_int(value, "max_queue", 64),
            retry_attempts=_non_negative_int(value, "retry_attempts", 2),
            retry_backoff_seconds=_positive_float(
                value, "retry_backoff_seconds", 0.05
            ),
            retry_max_backoff_seconds=_positive_float(
                value, "retry_max_backoff_seconds", 1.0
            ),
            retry_jitter=_bounded_float(value, "retry_jitter", 0.2, 0.0, 1.0),
            retry_budget_capacity=_positive_int(
                value, "retry_budget_capacity", 32
            ),
            retry_budget_refill_per_second=_positive_float(
                value, "retry_budget_refill_per_second", 1.0
            ),
            breaker_failure_threshold=_positive_int(
                value, "breaker_failure_threshold", 5
            ),
            breaker_open_seconds=_positive_float(
                value, "breaker_open_seconds", 30.0
            ),
            breaker_half_open_requests=_positive_int(
                value, "breaker_half_open_requests", 1
            ),
            tags=tags,
        )
        if origin.refresh_ahead_seconds > origin.ttl_seconds:
            raise OriginPolicyError(
                "refresh_ahead_seconds cannot exceed ttl_seconds"
            )
        if origin.retry_backoff_seconds > origin.retry_max_backoff_seconds:
            raise OriginPolicyError(
                "retry_backoff_seconds cannot exceed retry_max_backoff_seconds"
            )
        return origin

    @property
    def authority(self) -> str:
        default = (
            self.scheme == "http" and self.port == 80
        ) or (self.scheme == "https" and self.port == 443)
        host = "[{}]".format(self.host) if ":" in self.host else self.host
        return host if default else "{}:{}".format(host, self.port)


@dataclass(frozen=True)
class OriginResponse:
    status: int
    body: bytes


@dataclass(frozen=True)
class OriginFetchResult:
    state: str
    origin: str
    status_code: Optional[int] = None
    value: Any = None
    attempts: int = 0
    error: Optional[str] = None

    def as_json(self) -> Dict[str, Any]:
        document = asdict(self)
        value = document["value"]
        if isinstance(value, bytes):
            try:
                document["value"] = value.decode("utf-8")
                document["value_encoding"] = "utf-8"
            except UnicodeDecodeError:
                import base64

                document["value"] = base64.b64encode(value).decode("ascii")
                document["value_encoding"] = "base64"
        return document


class _CoalescedFlight:
    def __init__(self) -> None:
        self.event = threading.Event()
        self.result: Optional[OriginFetchResult] = None
        self.error: Optional[BaseException] = None
        self.followers = 0


class _FlightRegistry:
    def __init__(self) -> None:
        self.lock = threading.Lock()
        self.flights: Dict[str, _CoalescedFlight] = {}
        self.followers = 0


_FLIGHT_REGISTRIES_LOCK = threading.Lock()
_FLIGHT_REGISTRIES: Any = weakref.WeakKeyDictionary()


def _flight_registry(storage: StorageBackend) -> _FlightRegistry:
    with _FLIGHT_REGISTRIES_LOCK:
        try:
            registry = _FLIGHT_REGISTRIES.get(storage)
            if registry is None:
                registry = _FlightRegistry()
                _FLIGHT_REGISTRIES[storage] = registry
            return registry
        except TypeError:
            return _FlightRegistry()


class _LeaseGuard:
    def __init__(
        self,
        service: "OriginCache",
        key: str,
        token: str,
        duration: float,
    ) -> None:
        self._service = service
        self._key = key
        self._token = token
        self._renew = getattr(service.storage, "renew_lease", None)
        self._interval = max(0.05, min(5.0, duration / 3.0))
        self._stop = threading.Event()
        self._lock = threading.Lock()
        self._error: Optional[str] = None
        self._thread: Optional[threading.Thread] = None

    @property
    def supported(self) -> bool:
        return self._renew is not None

    def start(self) -> None:
        if self._renew is None:
            return
        self._thread = threading.Thread(
            target=self._run,
            name="megacache-origin-lease",
            daemon=True,
        )
        self._thread.start()

    def ensure_owned(self) -> None:
        with self._lock:
            error = self._error
        if error is not None:
            raise OriginUnavailable(error)
        if self._service._closed.is_set():
            raise OriginUnavailable("origin service is shutting down")

    def close(self) -> None:
        self._stop.set()
        if self._thread is not None:
            self._thread.join(timeout=self._interval + 0.1)

    def _run(self) -> None:
        assert self._renew is not None
        while not self._stop.wait(self._interval):
            if self._service._closed.is_set():
                self._lose("origin service is shutting down")
                return
            try:
                renewed = self._renew(self._key, self._token)
            except BaseException:
                renewed = False
            if not renewed:
                self._lose("origin refresh lease ownership was lost")
                return

    def _lose(self, message: str) -> None:
        with self._lock:
            self._error = message


class _AdmissionBudget:
    def __init__(self, maximum: int, maximum_queue: int) -> None:
        if maximum <= 0 or maximum_queue < 0:
            raise ValueError("admission limits are invalid")
        self.maximum = maximum
        self.maximum_queue = maximum_queue
        self.active = 0
        self.waiting = 0
        self.rejected = 0
        self._closed = False
        self._condition = threading.Condition()

    def acquire(self, timeout: float) -> None:
        deadline = time.monotonic() + timeout
        with self._condition:
            if self._closed:
                raise OriginUnavailable("origin service is shutting down")
            if self.active < self.maximum:
                self.active += 1
                return
            if self.waiting >= self.maximum_queue:
                self.rejected += 1
                raise OriginOverloaded("origin request queue is full")
            self.waiting += 1
            try:
                while self.active >= self.maximum:
                    remaining = deadline - time.monotonic()
                    if remaining <= 0:
                        self.rejected += 1
                        raise OriginOverloaded(
                            "origin request queue wait timed out"
                        )
                    self._condition.wait(remaining)
                    if self._closed:
                        raise OriginUnavailable(
                            "origin service is shutting down"
                        )
                self.active += 1
            finally:
                self.waiting -= 1

    def release(self) -> None:
        with self._condition:
            self.active -= 1
            self._condition.notify()

    def close(self) -> None:
        with self._condition:
            self._closed = True
            self._condition.notify_all()


class _OriginRuntime:
    def __init__(self, origin: HTTPOrigin, clock: Callable[[], float]) -> None:
        self.origin = origin
        self.admission = _AdmissionBudget(
            origin.max_concurrency, origin.max_queue
        )
        self.clock = clock
        self.lock = threading.RLock()
        self.breaker_state = BreakerState.CLOSED
        self.breaker_failures = 0
        self.breaker_open_until = 0.0
        self.half_open_active = 0
        self.retry_tokens = float(origin.retry_budget_capacity)
        self.retry_updated_at = clock()
        self.metrics: Dict[str, int] = {}
        self.latency_seconds = 0.0

    def increment(self, name: str, amount: int = 1) -> None:
        with self.lock:
            self.metrics[name] = self.metrics.get(name, 0) + amount

    def allow_request(self) -> bool:
        with self.lock:
            now = self.clock()
            if (
                self.breaker_state is BreakerState.OPEN
                and now >= self.breaker_open_until
            ):
                self.breaker_state = BreakerState.HALF_OPEN
                self.half_open_active = 0
                self.metrics["breaker_half_open_total"] = (
                    self.metrics.get("breaker_half_open_total", 0) + 1
                )
            if self.breaker_state is BreakerState.OPEN:
                self.metrics["breaker_rejections_total"] = (
                    self.metrics.get("breaker_rejections_total", 0) + 1
                )
                raise CircuitOpen("origin circuit breaker is open")
            half_open = self.breaker_state is BreakerState.HALF_OPEN
            if half_open:
                if (
                    self.half_open_active
                    >= self.origin.breaker_half_open_requests
                ):
                    self.metrics["breaker_rejections_total"] = (
                        self.metrics.get("breaker_rejections_total", 0) + 1
                    )
                    raise CircuitOpen(
                        "origin circuit breaker half-open probe is busy"
                    )
                self.half_open_active += 1
            return half_open

    def success(self, half_open: bool) -> None:
        with self.lock:
            if half_open:
                self.half_open_active = max(0, self.half_open_active - 1)
            self.breaker_state = BreakerState.CLOSED
            self.breaker_failures = 0
            self.breaker_open_until = 0.0

    def failure(self, half_open: bool) -> None:
        with self.lock:
            if half_open:
                self.half_open_active = max(0, self.half_open_active - 1)
            self.breaker_failures += 1
            if (
                half_open
                or self.breaker_failures
                >= self.origin.breaker_failure_threshold
            ):
                self.breaker_state = BreakerState.OPEN
                self.breaker_open_until = (
                    self.clock() + self.origin.breaker_open_seconds
                )
                self.metrics["breaker_opened_total"] = (
                    self.metrics.get("breaker_opened_total", 0) + 1
                )

    def cancel_probe(self, half_open: bool) -> None:
        if not half_open:
            return
        with self.lock:
            self.half_open_active = max(0, self.half_open_active - 1)

    def consume_retry(self) -> bool:
        with self.lock:
            now = self.clock()
            elapsed = max(0.0, now - self.retry_updated_at)
            self.retry_tokens = min(
                float(self.origin.retry_budget_capacity),
                self.retry_tokens
                + elapsed * self.origin.retry_budget_refill_per_second,
            )
            self.retry_updated_at = now
            if self.retry_tokens < 1.0:
                self.metrics["retry_budget_exhausted_total"] = (
                    self.metrics.get("retry_budget_exhausted_total", 0) + 1
                )
                return False
            self.retry_tokens -= 1.0
            return True

    def health(self) -> Dict[str, Any]:
        with self.lock:
            state = self.breaker_state
            if (
                state is BreakerState.OPEN
                and self.clock() >= self.breaker_open_until
            ):
                state = BreakerState.HALF_OPEN
            return {
                "breaker_state": state.value,
                "concurrency_active": self.admission.active,
                "queue_depth": self.admission.waiting,
                "max_concurrency": self.admission.maximum,
                "max_queue": self.admission.maximum_queue,
                "retry_tokens": round(self.retry_tokens, 3),
                "failures": self.breaker_failures,
            }


class OriginCache:
    """Storage facade adding safe declarative read-through origins."""

    def __init__(
        self,
        storage: StorageBackend,
        origins: Iterable[HTTPOrigin],
        *,
        worker_threads: int = 2,
        refresh_queue_size: int = 1_000,
        global_max_concurrency: int = 64,
        global_max_queue: int = 256,
        clock: Callable[[], float] = time.monotonic,
        sleeper: Callable[[float], None] = time.sleep,
        random_source: Callable[[], float] = random.random,
        resolver: Optional[Callable[[str, int], Sequence[str]]] = None,
        transport: Optional[
            Callable[..., OriginResponse]
        ] = None,
    ) -> None:
        if worker_threads <= 0 or refresh_queue_size <= 0:
            raise ValueError("origin worker limits must be positive")
        normalized = tuple(origins)
        if len(normalized) > 1_000:
            raise ValueError("at most 1000 origins may be configured")
        if len({origin.name for origin in normalized}) != len(normalized):
            raise ValueError("origin names must be unique")
        self.storage = storage
        self._clock = clock
        self._sleeper = sleeper
        self._random = random_source
        self._resolver = resolver or _resolve_addresses
        self._transport = transport or _http_transport
        self._transport_accepts_headers = _accepts_headers(self._transport)
        self._runtimes = {
            origin.name: _OriginRuntime(origin, clock) for origin in normalized
        }
        self._global = _AdmissionBudget(
            global_max_concurrency, global_max_queue
        )
        self._refresh_queue: "queue.Queue[Any]" = queue.Queue(
            maxsize=refresh_queue_size
        )
        self._scheduled = set()
        self._scheduled_lock = threading.Lock()
        self._closed = threading.Event()
        self._workers = []
        self._flight_registry = _flight_registry(storage)
        self._global_max_followers = global_max_queue
        self._shutdown_started = threading.Event()
        for index in range(worker_threads):
            worker = threading.Thread(
                target=self._worker,
                name="megacache-origin-{}".format(index),
                daemon=True,
            )
            worker.start()
            self._workers.append(worker)

    @classmethod
    def from_file(
        cls,
        storage: StorageBackend,
        path: str,
        **kwargs: Any
    ) -> "OriginCache":
        return cls(storage, load_origin_definitions(path), **kwargs)

    def fetch(
        self,
        key: str,
        origin_name: str,
        path: str,
        *,
        force_refresh: bool = False,
        traceparent: Optional[str] = None,
    ) -> OriginFetchResult:
        if self._closed.is_set():
            raise OriginUnavailable("origin service is shutting down")
        runtime = self._runtime(origin_name)
        request_path = validate_origin_path(runtime.origin, path)
        cached = self.storage.get(key)
        cached, lineage_matches = _decode_cache_result(
            cached, runtime.origin, request_path
        )
        if not lineage_matches:
            cached = CacheResult(state="miss")
        negative = _negative_value(cached.value)
        if not force_refresh and cached.state == "fresh":
            if negative is not None:
                runtime.increment("negative_hits_total")
                return OriginFetchResult(
                    state="negative",
                    origin=origin_name,
                    status_code=negative,
                )
            if (
                runtime.origin.refresh_ahead_seconds > 0
                and cached.expires_in_seconds
                <= runtime.origin.refresh_ahead_seconds
            ):
                self._schedule(key, origin_name, request_path)
            runtime.increment("cache_hits_total")
            return OriginFetchResult(
                state="fresh",
                origin=origin_name,
                value=cached.value,
            )
        if (
            not force_refresh
            and cached.state == "stale"
            and cached.stale_for_seconds
            > runtime.origin.stale_if_error_seconds
        ):
            self._schedule(key, origin_name, request_path)
            runtime.increment("stale_while_revalidate_total")
            return OriginFetchResult(
                state="stale",
                origin=origin_name,
                value=cached.value,
            )

        coordination_key = "origin:" + hashlib.sha256(
            "{}\0{}\0{}".format(origin_name, key, request_path).encode(
                "utf-8"
            )
        ).hexdigest()
        result = self._coordinate(
            coordination_key,
            runtime,
            lambda: self._leader_fetch(
                key,
                runtime,
                request_path,
                force_refresh,
                traceparent,
            ),
        )
        if result.state != "stale_if_error":
            return result
        current, lineage_matches = _decode_cache_result(
            self.storage.get(key), runtime.origin, request_path
        )
        if lineage_matches and current.state == "stale":
            return OriginFetchResult(
                state="stale_if_error",
                origin=runtime.origin.name,
                value=current.value,
                error=result.error,
            )
        if lineage_matches and current.state == "fresh":
            negative = _negative_value(current.value)
            if negative is not None:
                return OriginFetchResult(
                    state="negative",
                    origin=runtime.origin.name,
                    status_code=negative,
                )
            return OriginFetchResult(
                state="fresh",
                origin=runtime.origin.name,
                value=current.value,
            )
        raise OriginUnavailable(
            result.error or "stale cache entry expired during origin failure"
        )

    def origins(self) -> Dict[str, Any]:
        return {
            name: runtime.health()
            for name, runtime in sorted(self._runtimes.items())
        }

    def stats(self) -> Dict[str, int]:
        snapshot = dict(self.storage.stats())
        for runtime in self._runtimes.values():
            with runtime.lock:
                for name, value in runtime.metrics.items():
                    snapshot["origin_{}".format(name)] = (
                        snapshot.get("origin_{}".format(name), 0) + value
                    )
        snapshot["origin_count"] = len(self._runtimes)
        snapshot["origin_concurrency_active"] = self._global.active
        snapshot["origin_queue_depth"] = self._global.waiting
        with self._flight_registry.lock:
            snapshot["origin_follower_depth"] = (
                self._flight_registry.followers
            )
        snapshot.setdefault("origin_load_shed_total", 0)
        return snapshot

    def prometheus_metrics(self) -> str:
        lines = [self.storage.prometheus_metrics().rstrip("\n")]
        lines.extend(
            [
                "# HELP megacache_origin_requests_total Origin attempts by origin and outcome.",
                "# TYPE megacache_origin_requests_total counter",
                "# HELP megacache_origin_breaker_state Circuit breaker state (1 for current state).",
                "# TYPE megacache_origin_breaker_state gauge",
                "# HELP megacache_origin_concurrency_active Active origin requests.",
                "# TYPE megacache_origin_concurrency_active gauge",
                "# HELP megacache_origin_queue_depth Waiting origin requests.",
                "# TYPE megacache_origin_queue_depth gauge",
                "# HELP megacache_origin_request_duration_seconds Origin attempt latency.",
                "# TYPE megacache_origin_request_duration_seconds summary",
            ]
        )
        for name, runtime in sorted(self._runtimes.items()):
            safe_name = _prometheus_label(name)
            with runtime.lock:
                for outcome in ("success", "failure", "negative", "retry"):
                    value = runtime.metrics.get(
                        "{}_total".format(outcome), 0
                    )
                    lines.append(
                        'megacache_origin_requests_total{{origin="{}",outcome="{}"}} {}'.format(
                            safe_name, outcome, value
                        )
                    )
                state = runtime.health()["breaker_state"]
                for possible in BreakerState:
                    lines.append(
                        'megacache_origin_breaker_state{{origin="{}",state="{}"}} {}'.format(
                            safe_name,
                            possible.value,
                            int(state == possible.value),
                        )
                    )
                lines.append(
                    'megacache_origin_concurrency_active{{origin="{}"}} {}'.format(
                        safe_name, runtime.admission.active
                    )
                )
                lines.append(
                    'megacache_origin_queue_depth{{origin="{}"}} {}'.format(
                        safe_name, runtime.admission.waiting
                    )
                )
                attempt_count = runtime.metrics.get(
                    "success_total", 0
                ) + runtime.metrics.get("failure_total", 0)
                lines.append(
                    'megacache_origin_request_duration_seconds_sum{{origin="{}"}} {}'.format(
                        safe_name, runtime.latency_seconds
                    )
                )
                lines.append(
                    'megacache_origin_request_duration_seconds_count{{origin="{}"}} {}'.format(
                        safe_name, attempt_count
                    )
                )
        return "\n".join(lines) + "\n"

    def status(self) -> Dict[str, Any]:
        status_method = getattr(self.storage, "status", None)
        status = (
            {
                "healthy_nodes": 1,
                "total_nodes": 1,
                "degraded": False,
                "known_keys": self.storage.size(),
            }
            if status_method is None
            else dict(status_method())
        )
        status["origins"] = self.origins()
        return status

    def begin_shutdown(self) -> None:
        if self._shutdown_started.is_set():
            return
        self._shutdown_started.set()
        self._closed.set()
        self._global.close()
        for runtime in self._runtimes.values():
            runtime.admission.close()
        for _ in self._workers:
            try:
                self._refresh_queue.put_nowait(None)
            except queue.Full:
                break

    def close(self, timeout: float = 10.0) -> None:
        self.begin_shutdown()
        deadline = time.monotonic() + timeout
        for worker in self._workers:
            worker.join(max(0.0, deadline - time.monotonic()))

    def get(self, key: str) -> CacheResult:
        result = self.storage.get(key)
        if _negative_value(result.value) is not None:
            return CacheResult(state="miss")
        decoded = _origin_value(result.value)
        if decoded is not None:
            return CacheResult(
                state=result.state,
                value=decoded[3],
                expires_in_seconds=result.expires_in_seconds,
                stale_for_seconds=result.stale_for_seconds,
                lease_token=result.lease_token,
                retry_after_seconds=result.retry_after_seconds,
            )
        return result

    def mget(self, keys: Iterable[str]) -> Tuple[CacheResult, ...]:
        return tuple(self.get(key) for key in tuple(keys))

    def exists(self, keys: Iterable[str]) -> int:
        return sum(
            1
            for key in tuple(keys)
            if self.get(key).state != "miss"
        )

    def ttl(self, key: str) -> int:
        if self.get(key).state == "miss":
            return -2
        return self.storage.ttl(key)

    def keys(self) -> Tuple[str, ...]:
        return tuple(
            key
            for key in self.storage.keys()
            if self.get(key).state != "miss"
        )

    def size(self) -> int:
        return len(self.keys())

    def __getattr__(self, name: str) -> Any:
        return getattr(self.storage, name)

    def _runtime(self, name: str) -> _OriginRuntime:
        if not isinstance(name, str):
            raise OriginPolicyError("origin name must be a string")
        try:
            return self._runtimes[name]
        except KeyError as exc:
            raise OriginPolicyError(
                "unknown origin {!r}; arbitrary URLs are not accepted".format(
                    name
                )
            ) from exc

    def _leader_fetch(
        self,
        key: str,
        runtime: _OriginRuntime,
        path: str,
        force_refresh: bool,
        traceparent: Optional[str],
    ) -> OriginFetchResult:
        if not force_refresh:
            current = self.storage.get(key)
            current, lineage_matches = _decode_cache_result(
                current, runtime.origin, path
            )
            if not lineage_matches:
                current = CacheResult(state="miss")
            negative = _negative_value(current.value)
            if current.state == "fresh":
                if negative is not None:
                    return OriginFetchResult(
                        state="negative",
                        origin=runtime.origin.name,
                        status_code=negative,
                    )
                return OriginFetchResult(
                    state="fresh",
                    origin=runtime.origin.name,
                    value=current.value,
                )
        lease = self.storage.acquire_lease(key, force=True)
        lease_token = lease.lease_token
        if lease_token is None:
            raise OriginOverloaded(
                "refresh ownership is held by another operation"
            )
        lease_duration = max(0.0, lease.expires_in_seconds)
        guard = _LeaseGuard(self, key, lease_token, lease_duration)
        if (
            not guard.supported
            and _request_envelope_seconds(runtime.origin) >= lease_duration
        ):
            self.storage.release_lease(key, lease_token)
            raise OriginPolicyError(
                "origin retry envelope exceeds the refresh lease and "
                "the storage backend does not support lease renewal"
            )
        guard.start()
        try:
            try:
                response, attempts = self._request_with_retries(
                    runtime, path, traceparent
                )
                guard.ensure_owned()
            except OriginUnavailable as exc:
                self.storage.release_lease(key, lease_token)
                fallback, lineage_matches = _decode_cache_result(
                    self.storage.get(key), runtime.origin, path
                )
                if lineage_matches and fallback.state == "fresh":
                    return OriginFetchResult(
                        state="fresh",
                        origin=runtime.origin.name,
                        value=fallback.value,
                        error=str(exc),
                    )
                if lineage_matches and fallback.state == "stale":
                    runtime.increment("stale_if_error_total")
                    return OriginFetchResult(
                        state="stale_if_error",
                        origin=runtime.origin.name,
                        value=fallback.value,
                        error=str(exc),
                    )
                raise
            except BaseException:
                self.storage.release_lease(key, lease_token)
                raise
            origin = runtime.origin
            if response.status in origin.negative_statuses:
                self._put_origin_value(
                    key,
                    {
                        _NEGATIVE_MARKER: True,
                        "origin": origin.name,
                        "path": path,
                        "status": response.status,
                    },
                    ttl_seconds=origin.negative_ttl_seconds,
                    stale_seconds=0,
                    tags=origin.tags,
                    lease_token=lease_token,
                )
                runtime.increment("negative_total")
                return OriginFetchResult(
                    state="negative",
                    origin=origin.name,
                    status_code=response.status,
                    attempts=attempts,
                )
            self._put_origin_value(
                key,
                {
                    _VALUE_MARKER: True,
                    "origin": origin.name,
                    "path": path,
                    "status": response.status,
                    "body": base64.b64encode(response.body).decode("ascii"),
                },
                ttl_seconds=origin.ttl_seconds,
                stale_seconds=(
                    origin.stale_while_revalidate_seconds
                    + origin.stale_if_error_seconds
                ),
                tags=origin.tags,
                lease_token=lease_token,
            )
            return OriginFetchResult(
                state="refreshed",
                origin=origin.name,
                status_code=response.status,
                value=response.body,
                attempts=attempts,
            )
        except BaseException:
            self.storage.release_lease(key, lease_token)
            raise
        finally:
            guard.close()

    def _put_origin_value(self, key: str, value: Any, **kwargs: Any) -> None:
        try:
            self.storage.put(key, value, **kwargs)
        except ValueError as exc:
            if "lease" in str(exc).lower():
                raise OriginUnavailable(
                    "origin refresh lease ownership was lost"
                ) from exc
            raise

    def _request_with_retries(
        self,
        runtime: _OriginRuntime,
        path: str,
        traceparent: Optional[str] = None,
    ) -> Tuple[OriginResponse, int]:
        origin = runtime.origin
        attempts = 0
        while True:
            if self._closed.is_set():
                raise OriginUnavailable("origin service is shutting down")
            attempts += 1
            started = time.perf_counter()
            half_open = runtime.allow_request()
            acquired_global = False
            acquired_origin = False
            breaker_recorded = False
            try:
                self._global.acquire(origin.queue_timeout_seconds)
                acquired_global = True
                runtime.admission.acquire(origin.queue_timeout_seconds)
                acquired_origin = True
                addresses = self._validated_addresses(origin)
                headers = (
                    {}
                    if traceparent is None
                    else {"traceparent": traceparent}
                )
                if self._transport_accepts_headers:
                    response = self._transport(
                        origin, path, addresses, headers
                    )
                else:
                    response = self._transport(origin, path, addresses)
                if self._closed.is_set():
                    raise OriginUnavailable(
                        "origin service is shutting down"
                    )
                if (
                    not isinstance(response, OriginResponse)
                    or not isinstance(response.status, int)
                    or isinstance(response.status, bool)
                    or response.status < 100
                    or response.status > 599
                    or not isinstance(response.body, bytes)
                ):
                    raise OriginUnavailable(
                        "origin transport returned an invalid response"
                    )
                if len(response.body) > origin.max_response_bytes:
                    raise OriginUnavailable(
                        "origin response exceeds configured limit"
                    )
                if response.status in origin.negative_statuses:
                    runtime.success(half_open)
                    breaker_recorded = True
                    runtime.increment("success_total")
                    return response, attempts
                if 200 <= response.status <= 299:
                    runtime.success(half_open)
                    breaker_recorded = True
                    runtime.increment("success_total")
                    return response, attempts
                message = "origin returned HTTP {}".format(response.status)
                if response.status not in _RETRYABLE_STATUS:
                    runtime.success(half_open)
                    breaker_recorded = True
                    raise _NonRetryableResponse(message)
                runtime.failure(half_open)
                breaker_recorded = True
                error = OriginUnavailable(message)
            except CircuitOpen:
                raise
            except OriginOverloaded:
                runtime.cancel_probe(half_open)
                runtime.increment("load_shed_total")
                raise
            except OriginPolicyError:
                runtime.cancel_probe(half_open)
                raise
            except OriginUnavailable as exc:
                if self._closed.is_set():
                    runtime.cancel_probe(half_open)
                    raise
                if not breaker_recorded:
                    runtime.failure(half_open)
                error = exc
            except (OSError, TimeoutError, http.client.HTTPException) as exc:
                runtime.failure(half_open)
                error = OriginUnavailable(
                    "origin request failed: {}".format(exc)
                )
            finally:
                duration = time.perf_counter() - started
                with runtime.lock:
                    runtime.latency_seconds += duration
                if acquired_origin:
                    runtime.admission.release()
                if acquired_global:
                    self._global.release()
            runtime.increment("failure_total")
            if (
                isinstance(error, _NonRetryableResponse)
                or
                attempts > origin.retry_attempts
                or not runtime.consume_retry()
            ):
                raise error
            runtime.increment("retry_total")
            delay = min(
                origin.retry_max_backoff_seconds,
                origin.retry_backoff_seconds * (2 ** (attempts - 1)),
            )
            jitter = 1.0 + (
                (self._random() * 2.0 - 1.0) * origin.retry_jitter
            )
            self._sleep_backoff(max(0.0, delay * jitter))

    def _sleep_backoff(self, delay: float) -> None:
        if self._sleeper is time.sleep:
            if self._closed.wait(delay):
                raise OriginUnavailable("origin service is shutting down")
            return
        self._sleeper(delay)
        if self._closed.is_set():
            raise OriginUnavailable("origin service is shutting down")

    def _validated_addresses(self, origin: HTTPOrigin) -> Tuple[str, ...]:
        if (
            origin.host not in origin.allowed_hosts
            or origin.port not in origin.allowed_ports
        ):
            raise OriginPolicyError("origin authority is not allowed")
        addresses = tuple(dict.fromkeys(self._resolver(origin.host, origin.port)))
        if not addresses:
            raise OriginUnavailable("origin DNS resolution returned no addresses")
        for text in addresses:
            try:
                address = ipaddress.ip_address(text)
            except ValueError as exc:
                raise OriginPolicyError(
                    "origin resolver returned an invalid address"
                ) from exc
            if not _address_is_allowed(
                address, origin.allowed_ip_networks
            ):
                raise OriginPolicyError(
                    "origin resolved to non-public or special-use address {} "
                    "without an explicit network allowlist".format(
                        address
                    )
                )
        return addresses

    def _schedule(self, key: str, origin: str, path: str) -> None:
        job = (key, origin, path)
        with self._scheduled_lock:
            if job in self._scheduled or self._closed.is_set():
                return
            self._scheduled.add(job)
            try:
                self._refresh_queue.put_nowait(job)
            except queue.Full:
                self._scheduled.remove(job)
                self._runtime(origin).increment("refresh_load_shed_total")

    def _worker(self) -> None:
        while True:
            try:
                job = self._refresh_queue.get(timeout=0.1)
            except queue.Empty:
                if self._closed.is_set():
                    return
                continue
            if job is None:
                self._refresh_queue.task_done()
                return
            key, origin, path = job
            try:
                if not self._closed.is_set():
                    self.fetch(
                        key, origin, path, force_refresh=True
                    )
            except Exception:
                self._runtime(origin).increment("refresh_errors_total")
            finally:
                with self._scheduled_lock:
                    self._scheduled.discard(job)
                self._refresh_queue.task_done()

    def _coordinate(
        self,
        key: str,
        runtime: _OriginRuntime,
        loader: Callable[[], OriginFetchResult],
    ) -> OriginFetchResult:
        registry = self._flight_registry
        with registry.lock:
            flight = registry.flights.get(key)
            leader = flight is None
            if leader:
                flight = _CoalescedFlight()
                registry.flights[key] = flight
            else:
                if (
                    flight.followers >= runtime.origin.max_queue
                    or registry.followers >= self._global_max_followers
                ):
                    runtime.increment("load_shed_total")
                    raise OriginOverloaded(
                        "origin coalesced follower limit reached"
                    )
                flight.followers += 1
                registry.followers += 1
                runtime.increment("coalesced_total")
        if not leader:
            deadline = time.monotonic() + runtime.origin.queue_timeout_seconds
            try:
                while not flight.event.wait(
                    min(0.1, max(0.0, deadline - time.monotonic()))
                ):
                    if self._closed.is_set():
                        raise OriginUnavailable(
                            "origin service is shutting down"
                        )
                    if time.monotonic() >= deadline:
                        runtime.increment("load_shed_total")
                        raise OriginOverloaded(
                            "origin coalesced follower wait timed out"
                        )
                if self._closed.is_set():
                    raise OriginUnavailable(
                        "origin service is shutting down"
                    )
                if flight.error is not None:
                    raise flight.error
                assert flight.result is not None
                return flight.result
            finally:
                with registry.lock:
                    flight.followers -= 1
                    registry.followers -= 1
        try:
            coordinator = getattr(self.storage, "coordinate", None)
            flight.result = (
                loader()
                if coordinator is None
                else coordinator(key, loader)
            )
            return flight.result
        except BaseException as exc:
            flight.error = exc
            raise
        finally:
            with registry.lock:
                if registry.flights.get(key) is flight:
                    registry.flights.pop(key, None)
                flight.event.set()


def load_origin_definitions(path: str) -> Tuple[HTTPOrigin, ...]:
    try:
        with open(path, "r", encoding="utf-8") as handle:
            document = json.load(handle)
    except (OSError, json.JSONDecodeError) as exc:
        raise OriginPolicyError(
            "cannot load origin definitions: {}".format(exc)
        ) from exc
    if not isinstance(document, dict) or not isinstance(
        document.get("origins"), list
    ):
        raise OriginPolicyError(
            "origin file must contain an origins JSON array"
        )
    origins = tuple(
        HTTPOrigin.from_dict(item) for item in document["origins"]
    )
    if not origins:
        raise OriginPolicyError("origin file must define at least one origin")
    if len(origins) > 1_000:
        raise OriginPolicyError("origin file defines more than 1000 origins")
    if len({origin.name for origin in origins}) != len(origins):
        raise OriginPolicyError("origin names must be unique")
    return origins


def validate_origin_path(origin: HTTPOrigin, path: str) -> str:
    if not isinstance(path, str) or not path or len(path) > 4096:
        raise OriginPolicyError("origin path must contain 1 to 4096 characters")
    if (
        any(ord(character) < 32 for character in path)
        or "\\" in path
        or re.search(r"%(?:2f|2F|5c|5C|00)", path)
    ):
        raise OriginPolicyError("origin path contains unsafe characters")
    parsed = urlsplit(path)
    if (
        parsed.scheme
        or parsed.netloc
        or parsed.fragment
        or not parsed.path.startswith("/")
    ):
        raise OriginPolicyError(
            "origin fetch accepts only an absolute path, never a URL"
        )
    decoded = unquote(parsed.path)
    decoded_query = unquote(parsed.query)
    if any(
        ord(character) < 32 or ord(character) == 127
        for character in decoded + decoded_query
    ):
        raise OriginPolicyError("origin path contains encoded control characters")
    segments = decoded.split("/")
    if any(segment in (".", "..") for segment in segments):
        raise OriginPolicyError("origin path must not contain dot segments")
    normalized = posixpath.normpath(decoded)
    if decoded.endswith("/") and not normalized.endswith("/"):
        normalized += "/"
    if not normalized.startswith("/"):
        normalized = "/" + normalized
    if not any(
        _matches_prefix(normalized, prefix)
        for prefix in origin.allowed_path_prefixes
    ):
        raise OriginPolicyError("origin path is outside the allowed prefixes")
    encoded = quote(normalized, safe="/:@!$&'()*+,;=-._~")
    if parsed.query:
        encoded += "?" + quote(
            parsed.query, safe="!$&'()*+,;=:@/?-._~%"
        )
    return encoded


def _http_transport(
    origin: HTTPOrigin,
    path: str,
    addresses: Sequence[str],
    propagated_headers: Optional[Dict[str, str]] = None,
) -> OriginResponse:
    address = addresses[0]
    if origin.scheme == "https":
        connection = _PinnedHTTPSConnection(
            origin.host,
            origin.port,
            address,
            timeout=origin.timeout_seconds,
        )
    else:
        connection = _PinnedHTTPConnection(
            origin.host,
            origin.port,
            address,
            timeout=origin.timeout_seconds,
        )
    headers = dict(origin.headers)
    headers["Host"] = origin.authority
    headers.setdefault("Accept-Encoding", "identity")
    headers.setdefault("User-Agent", "MegaCache/0.8")
    if propagated_headers:
        headers.update(propagated_headers)
    try:
        connection.request("GET", path, headers=headers)
        response = connection.getresponse()
        body = response.read(origin.max_response_bytes + 1)
        if len(body) > origin.max_response_bytes:
            raise OriginUnavailable("origin response exceeds configured limit")
        return OriginResponse(response.status, body)
    finally:
        connection.close()


def _accepts_headers(transport: Callable[..., OriginResponse]) -> bool:
    try:
        signature = inspect.signature(transport)
    except (TypeError, ValueError):
        return False
    positional = [
        parameter
        for parameter in signature.parameters.values()
        if parameter.kind
        in (
            inspect.Parameter.POSITIONAL_ONLY,
            inspect.Parameter.POSITIONAL_OR_KEYWORD,
        )
    ]
    return len(positional) >= 4 or any(
        parameter.kind is inspect.Parameter.VAR_POSITIONAL
        for parameter in signature.parameters.values()
    )


class _PinnedHTTPConnection(http.client.HTTPConnection):
    def __init__(
        self, host: str, port: int, address: str, timeout: float
    ) -> None:
        super().__init__(host, port, timeout=timeout)
        self._address = address

    def connect(self) -> None:
        self.sock = socket.create_connection(
            (self._address, self.port), self.timeout
        )


class _PinnedHTTPSConnection(http.client.HTTPSConnection):
    def __init__(
        self, host: str, port: int, address: str, timeout: float
    ) -> None:
        super().__init__(
            host,
            port,
            timeout=timeout,
            context=ssl.create_default_context(),
        )
        self._address = address

    def connect(self) -> None:
        raw = socket.create_connection(
            (self._address, self.port), self.timeout
        )
        self.sock = self._context.wrap_socket(
            raw, server_hostname=self.host
        )


def _resolve_addresses(host: str, port: int) -> Sequence[str]:
    return [
        item[4][0]
        for item in socket.getaddrinfo(
            host, port, type=socket.SOCK_STREAM
        )
    ]


def _address_is_allowed(
    address: Any, allowed_networks: Sequence[Any]
) -> bool:
    if _in_networks(address, allowed_networks):
        return True
    embedded = _embedded_ipv4_addresses(address)
    if embedded:
        explicitly_allowed = tuple(
            _in_networks(item, allowed_networks) for item in embedded
        )
        if any(explicitly_allowed) and all(
            allowed or _is_public_address(item)
            for item, allowed in zip(embedded, explicitly_allowed)
        ):
            return True
    return _is_public_address(address)


def _in_networks(address: Any, networks: Sequence[Any]) -> bool:
    return any(
        address.version == network.version and address in network
        for network in networks
    )


def _is_public_address(address: Any) -> bool:
    if address.version == 6 and _is_transition_address(address):
        return False
    return bool(
        address.is_global
        and not address.is_private
        and not address.is_loopback
        and not address.is_link_local
        and not address.is_multicast
        and not address.is_reserved
        and not address.is_unspecified
    )


def _embedded_ipv4_addresses(address: Any) -> Tuple[Any, ...]:
    if address.version != 6:
        return ()
    embedded = []
    for item in (
        address.ipv4_mapped,
        address.sixtofour,
    ):
        if item is not None:
            embedded.append(item)
    if address.teredo is not None:
        embedded.extend(address.teredo)
    if any(address in network for network in _LOW32_EMBEDDED_NETWORKS):
        embedded.append(ipaddress.IPv4Address(int(address) & 0xFFFFFFFF))
    if _is_isatap_address(address):
        embedded.append(ipaddress.IPv4Address(int(address) & 0xFFFFFFFF))
    return tuple(dict.fromkeys(embedded))


def _is_transition_address(address: Any) -> bool:
    return any(
        address in network for network in _TRANSITION_NETWORKS
    ) or _is_isatap_address(address)


def _is_isatap_address(address: Any) -> bool:
    interface_prefix = (int(address) >> 32) & 0xFFFFFFFF
    return interface_prefix in (0x00005EFE, 0x02005EFE)


def _request_envelope_seconds(origin: HTTPOrigin) -> float:
    attempts = origin.retry_attempts + 1
    total = attempts * (
        origin.timeout_seconds + 2.0 * origin.queue_timeout_seconds
    )
    for retry in range(origin.retry_attempts):
        total += min(
            origin.retry_max_backoff_seconds,
            origin.retry_backoff_seconds * (2 ** retry),
        ) * (1.0 + origin.retry_jitter)
    return total


def _negative_origin_value(
    value: Any,
) -> Optional[Tuple[str, str, int]]:
    if not (
        isinstance(value, dict)
        and value.get(_NEGATIVE_MARKER) is True
        and isinstance(value.get("origin"), str)
        and isinstance(value.get("path"), str)
        and isinstance(value.get("status"), int)
        and not isinstance(value.get("status"), bool)
        and 400 <= value["status"] <= 499
    ):
        return None
    return value["origin"], value["path"], value["status"]


def _negative_value(value: Any) -> Optional[int]:
    decoded = _negative_origin_value(value)
    return None if decoded is None else decoded[2]


def _origin_value(
    value: Any,
) -> Optional[Tuple[str, str, int, bytes]]:
    if not (
        isinstance(value, dict)
        and value.get(_VALUE_MARKER) is True
        and isinstance(value.get("origin"), str)
        and isinstance(value.get("path"), str)
        and isinstance(value.get("status"), int)
        and not isinstance(value.get("status"), bool)
        and 200 <= value["status"] <= 299
        and isinstance(value.get("body"), str)
    ):
        return None
    try:
        body = base64.b64decode(value["body"], validate=True)
    except (ValueError, TypeError):
        return None
    return value["origin"], value["path"], value["status"], body


def _decode_cache_result(
    result: CacheResult, origin: HTTPOrigin, path: str
) -> Tuple[CacheResult, bool]:
    positive = _origin_value(result.value)
    if positive is not None:
        if positive[0] != origin.name or positive[1] != path:
            return result, False
        return (
            CacheResult(
                state=result.state,
                value=positive[3],
                expires_in_seconds=result.expires_in_seconds,
                stale_for_seconds=result.stale_for_seconds,
                lease_token=result.lease_token,
                retry_after_seconds=result.retry_after_seconds,
            ),
            True,
        )
    negative = _negative_origin_value(result.value)
    if negative is not None:
        return (
            result,
            negative[0] == origin.name
            and negative[1] == path
            and negative[2] in origin.negative_statuses,
        )
    return result, False


def _matches_prefix(path: str, prefix: str) -> bool:
    if prefix == "/":
        return True
    if prefix.endswith("/"):
        return path.startswith(prefix) or path == prefix[:-1]
    return path == prefix or path.startswith(prefix + "/")


def _normalize_host(value: str) -> str:
    if not isinstance(value, str) or not value:
        raise OriginPolicyError("allowed hosts must be non-empty strings")
    if value.endswith("."):
        value = value[:-1]
    try:
        normalized = value.encode("idna").decode("ascii").lower()
    except UnicodeError as exc:
        raise OriginPolicyError("origin host is invalid") from exc
    if (
        "*" in normalized
        or "/" in normalized
        or "%" in normalized
        or any(
        character.isspace() for character in normalized
        )
    ):
        raise OriginPolicyError(
            "origin hosts must be explicit exact hostnames"
        )
    try:
        ipaddress.ip_address(normalized)
    except ValueError:
        if len(normalized) > 253 or any(
            not label
            or len(label) > 63
            or not re.match(
                r"^[A-Za-z0-9](?:[A-Za-z0-9-]*[A-Za-z0-9])?$",
                label,
            )
            for label in normalized.split(".")
        ):
            raise OriginPolicyError("origin host is invalid")
    return normalized


def _validate_prefix(value: str) -> str:
    if (
        not isinstance(value, str)
        or not value.startswith("/")
        or "\\" in value
        or "?" in value
        or "#" in value
        or any(segment in (".", "..") for segment in value.split("/"))
    ):
        raise OriginPolicyError(
            "allowed path prefixes must be absolute normalized paths"
        )
    normalized = posixpath.normpath(value)
    if value.endswith("/") and not normalized.endswith("/"):
        normalized += "/"
    return normalized


def _string_array(
    value: Any, name: str, required: bool = False
) -> Tuple[str, ...]:
    if not isinstance(value, (list, tuple)):
        raise OriginPolicyError("{} must be an array".format(name))
    result = tuple(value)
    if required and not result:
        raise OriginPolicyError("{} must not be empty".format(name))
    if any(not isinstance(item, str) or not item for item in result):
        raise OriginPolicyError(
            "{} must contain non-empty strings".format(name)
        )
    return result


def _integer_array(
    value: Any, name: str, required: bool = False
) -> Tuple[int, ...]:
    if not isinstance(value, (list, tuple)):
        raise OriginPolicyError("{} must be an array".format(name))
    result = tuple(value)
    if required and not result:
        raise OriginPolicyError("{} must not be empty".format(name))
    if any(
        not isinstance(item, int)
        or isinstance(item, bool)
        or item <= 0
        or item > 65_535
        for item in result
    ):
        raise OriginPolicyError(
            "{} must contain valid positive integers".format(name)
        )
    return result


def _positive_int(value: Mapping[str, Any], name: str, default: int) -> int:
    selected = value.get(name, default)
    if (
        not isinstance(selected, int)
        or isinstance(selected, bool)
        or selected <= 0
    ):
        raise OriginPolicyError("{} must be a positive integer".format(name))
    return selected


def _non_negative_int(
    value: Mapping[str, Any], name: str, default: int
) -> int:
    selected = value.get(name, default)
    if (
        not isinstance(selected, int)
        or isinstance(selected, bool)
        or selected < 0
    ):
        raise OriginPolicyError(
            "{} must be a non-negative integer".format(name)
        )
    return selected


def _positive_float(
    value: Mapping[str, Any], name: str, default: float
) -> float:
    selected = value.get(name, default)
    if (
        isinstance(selected, bool)
        or not isinstance(selected, (int, float))
        or not math.isfinite(selected)
        or selected <= 0
    ):
        raise OriginPolicyError(
            "{} must be a finite positive number".format(name)
        )
    return float(selected)


def _bounded_float(
    value: Mapping[str, Any],
    name: str,
    default: float,
    minimum: float,
    maximum: float,
) -> float:
    selected = value.get(name, default)
    if (
        isinstance(selected, bool)
        or not isinstance(selected, (int, float))
        or not math.isfinite(selected)
        or selected < minimum
        or selected > maximum
    ):
        raise OriginPolicyError(
            "{} must be between {} and {}".format(name, minimum, maximum)
        )
    return float(selected)


def _prometheus_label(value: str) -> str:
    return value.replace("\\", "\\\\").replace("\n", "\\n").replace('"', '\\"')
