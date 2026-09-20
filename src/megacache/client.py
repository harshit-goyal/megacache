"""Small RESP2 client used by the native MegaCache CLI."""

import socket
import ssl
import json
import threading
import time
from collections import OrderedDict
from contextlib import contextmanager
from contextvars import ContextVar
from dataclasses import dataclass
from typing import (
    Any,
    BinaryIO,
    Callable,
    Dict,
    Iterable,
    List,
    Optional,
    Sequence,
    Tuple,
    Union,
)


class MegaCacheClientError(Exception):
    """Base error for native client failures."""


class MegaCacheCommandError(MegaCacheClientError):
    """Error returned by the MegaCache server."""

    def __init__(self, message: str) -> None:
        super().__init__(message)
        self.code = message.split(" ", 1)[0] if message else "ERR"


class MegaCacheConnectionError(MegaCacheClientError):
    """Network, timeout, or TLS failure."""


class MegaCacheProtocolError(MegaCacheClientError):
    """Malformed or unexpected RESP response."""


CommandPart = Union[str, bytes, int]

_MAX_RESPONSE_BYTES = 16 * 1024 * 1024
_MAX_BULK_BYTES = 16 * 1024 * 1024
_MAX_ARRAY_ITEMS = 100_000
_MAX_RESPONSE_DEPTH = 128
_request_traceparent: ContextVar[Optional[str]] = ContextVar(
    "megacache_request_traceparent", default=None
)


def _elapsed_since(started: float) -> float:
    return max(0.0, time.monotonic() - started)


def _deadline_after(started: float, budget: float) -> float:
    now = time.monotonic()
    return now + max(0.0, budget - max(0.0, now - started))


def _remaining_windows(
    fresh_budget: float, stale_budget: float, elapsed: float
) -> Tuple[float, float]:
    fresh = max(0.0, fresh_budget - elapsed)
    hard = max(0.0, fresh_budget + stale_budget - elapsed)
    return fresh, max(0.0, hard - fresh)


@dataclass(frozen=True)
class LeaseResult:
    state: str
    value: Optional[bytes] = None
    lease_token: Optional[str] = None
    retry_after_seconds: float = 0
    expires_in_seconds: Optional[float] = None
    stale_for_seconds: Optional[float] = None


@dataclass(frozen=True)
class FetchResult:
    state: str
    origin: str
    value: Any = None
    status_code: Optional[int] = None
    attempts: int = 0
    error: Optional[str] = None


@dataclass(frozen=True)
class CachePolicy:
    ttl_seconds: int = 60
    stale_seconds: int = 300
    tags: Tuple[str, ...] = ()
    stale_if_error: bool = True


@dataclass(frozen=True)
class CachedValue:
    value: bytes
    state: str
    stale_deadline: Optional[float] = None


@dataclass
class _LocalEntry:
    value: bytes
    fresh_until: float
    stale_until: float
    size_bytes: int


@dataclass
class _Flight:
    event: threading.Event
    result: Optional[CachedValue] = None
    error: Optional[BaseException] = None


class LocalCache:
    """Thread-safe bounded LRU used by the Python SDK."""

    def __init__(
        self, max_entries: int = 1_000, max_memory_bytes: int = 16_777_216
    ) -> None:
        if max_entries <= 0 or max_memory_bytes <= 0:
            raise ValueError("L1 limits must be positive")
        self.max_entries = max_entries
        self.max_memory_bytes = max_memory_bytes
        self._entries: "OrderedDict[str, _LocalEntry]" = OrderedDict()
        self._used_bytes = 0
        self._lock = threading.RLock()

    def get(self, key: str) -> Optional[CachedValue]:
        now = time.monotonic()
        with self._lock:
            entry = self._entries.get(key)
            if entry is None:
                return None
            if now >= entry.stale_until:
                self._remove(key)
                return None
            self._entries.move_to_end(key)
            return CachedValue(
                entry.value,
                "fresh" if now < entry.fresh_until else "stale",
                entry.stale_until,
            )

    def put(
        self, key: str, value: bytes, ttl_seconds: float, stale_seconds: float
    ) -> bool:
        if (
            ttl_seconds < 0
            or stale_seconds < 0
            or ttl_seconds + stale_seconds <= 0
        ):
            raise ValueError("invalid L1 freshness policy")
        copied = bytes(value)
        size = len(key.encode("utf-8")) + len(copied)
        if size > self.max_memory_bytes:
            return False
        now = time.monotonic()
        with self._lock:
            self._remove(key)
            self._entries[key] = _LocalEntry(
                copied, now + ttl_seconds, now + ttl_seconds + stale_seconds, size
            )
            self._used_bytes += size
            while (
                len(self._entries) > self.max_entries
                or self._used_bytes > self.max_memory_bytes
            ):
                oldest, _ = next(iter(self._entries.items()))
                self._remove(oldest)
        return True

    def clear(self) -> None:
        with self._lock:
            self._entries.clear()
            self._used_bytes = 0

    def _remove(self, key: str) -> None:
        entry = self._entries.pop(key, None)
        if entry is not None:
            self._used_bytes -= entry.size_bytes


class MegaCacheClient:
    def __init__(
        self,
        host: str = "127.0.0.1",
        port: int = 6380,
        password: Optional[str] = None,
        timeout: float = 5,
        *,
        username: Optional[str] = None,
        tls: bool = False,
        ca_file: Optional[str] = None,
        server_name: Optional[str] = None,
        l1_max_entries: int = 1_000,
        l1_max_memory_bytes: int = 16_777_216,
        invalidation_poll_seconds: float = 1.0,
        traceparent_provider: Optional[Callable[[], Optional[str]]] = None,
    ) -> None:
        self.host = host
        self.port = port
        self.password = password
        self.username = username
        self.timeout = timeout
        self.tls = tls
        self.ca_file = ca_file
        self.server_name = server_name
        if invalidation_poll_seconds < 0:
            raise ValueError("invalidation_poll_seconds cannot be negative")
        self.local = LocalCache(l1_max_entries, l1_max_memory_bytes)
        self.invalidation_poll_seconds = invalidation_poll_seconds
        self.traceparent_provider = traceparent_provider
        self._socket: Optional[socket.socket] = None
        self._stream: Optional[BinaryIO] = None
        self._command_lock = threading.RLock()
        self._mutation_lock = threading.Lock()
        self._cursor: Optional[str] = None
        self._cursor_epoch: Optional[str] = None
        self._cursor_generation: Optional[int] = None
        self._last_poll = 0.0
        self._active_traceparent: Optional[str] = None
        self._flights: Dict[str, _Flight] = {}
        self._mutation_generation = 0
        self._flight_lock = threading.Lock()

    def __enter__(self) -> "MegaCacheClient":
        self.connect()
        return self

    def __exit__(self, *args: Any) -> None:
        self.close()

    def connect(self) -> None:
        with self._command_lock:
            if self._socket is not None:
                return
            try:
                connection = socket.create_connection(
                    (self.host, self.port), timeout=self.timeout
                )
                self._socket = connection
                if self.tls:
                    context = ssl.create_default_context(cafile=self.ca_file)
                    connection = context.wrap_socket(
                        connection, server_hostname=self.server_name or self.host
                    )
                self._socket = connection
                self._stream = self._socket.makefile("rwb")
                if self.password is not None:
                    if self.username is None:
                        response = self.command("AUTH", self.password)
                    else:
                        response = self.command(
                            "AUTH", self.username, self.password
                        )
                    self._expect_ok(response, "AUTH")
            except MegaCacheClientError:
                self.close()
                raise
            except (OSError, EOFError) as exc:
                self.close()
                raise MegaCacheConnectionError(
                    "connection to {}:{} failed: {}".format(
                        self.host, self.port, exc
                    )
                ) from exc
            except Exception:
                self.close()
                raise

    def close(self) -> None:
        with self._command_lock:
            if self._stream is not None:
                self._stream.close()
            if self._socket is not None:
                self._socket.close()
            self._stream = None
            self._socket = None

    def command(self, *parts: CommandPart) -> Any:
        with self._command_lock:
            if not parts:
                raise ValueError("a command is required")
            if self._stream is None:
                self.connect()
            assert self._stream is not None
            encoded = [self._encode_part(part) for part in parts]
            payload = b"*" + str(len(encoded)).encode("ascii") + b"\r\n"
            payload += b"".join(
                b"$"
                + str(len(part)).encode("ascii")
                + b"\r\n"
                + part
                + b"\r\n"
                for part in encoded
            )
            try:
                self._stream.write(payload)
                self._stream.flush()
                return self._read_response()
            except MegaCacheProtocolError:
                self.close()
                raise
            except (OSError, EOFError) as exc:
                self.close()
                raise MegaCacheConnectionError(
                    "connection to {}:{} failed: {}".format(
                        self.host, self.port, exc
                    )
                ) from exc

    def ping(self) -> str:
        value = self._expect_text(self.command("PING"), "PING")
        if value != "PONG":
            raise self._protocol_error("PING response must be PONG")
        return value

    def get(self, key: str) -> Optional[bytes]:
        return self._expect_bulk(self.command("GET", key), "GET", nullable=True)

    def set(
        self, key: str, value: Union[str, bytes], expire_seconds: Optional[int] = None
    ) -> str:
        parts: List[CommandPart] = ["SET", key, value]
        if expire_seconds is not None:
            parts.extend(["EX", expire_seconds])
        result, _ = self._mutation_command(parts)
        return result

    def mget(self, keys: Iterable[str]) -> List[Optional[bytes]]:
        selected = tuple(keys)
        response = self.command("MGET", *selected)
        if not isinstance(response, list) or len(response) != len(selected):
            raise self._protocol_error(
                "MGET response must contain one value per key"
            )
        return [
            self._expect_bulk(item, "MGET", nullable=True)
            for item in response
        ]

    def mset(self, values: Iterable[Tuple[str, Union[str, bytes]]]) -> str:
        parts: List[CommandPart] = ["MSET"]
        for key, value in values:
            parts.extend((key, value))
        result, _ = self._mutation_command(parts)
        return result

    def delete(self, *keys: str) -> int:
        with self._mutation_lock:
            result = self._expect_integer(
                self.command("DEL", *keys), "DEL", minimum=0,
                maximum=len(keys),
            )
            if result:
                self._mutation_succeeded()
        return result

    def exists(self, *keys: str) -> int:
        return self._expect_integer(
            self.command("EXISTS", *keys), "EXISTS", minimum=0,
            maximum=len(keys),
        )

    def expire(self, key: str, seconds: int) -> bool:
        with self._mutation_lock:
            result = self._expect_integer(
                self.command("EXPIRE", key, seconds),
                "EXPIRE",
                minimum=0,
                maximum=1,
            )
            if result:
                self._mutation_succeeded()
        return bool(result)

    def ttl(self, key: str) -> int:
        return self._expect_integer(
            self.command("TTL", key), "TTL", minimum=-2
        )

    def put(
        self,
        key: str,
        value: Union[str, bytes],
        *,
        ttl_seconds: Optional[int] = None,
        stale_seconds: Optional[int] = None,
        tags: Iterable[str] = (),
        lease_token: Optional[str] = None,
    ) -> str:
        parts: List[CommandPart] = ["MC.SET", key, value]
        if ttl_seconds is not None:
            parts.extend(("TTL", ttl_seconds))
        if stale_seconds is not None:
            parts.extend(("STALE", stale_seconds))
        normalized_tags = tuple(tags)
        if normalized_tags:
            parts.extend(("TAGS", len(normalized_tags)))
            parts.extend(normalized_tags)
        if lease_token is not None:
            parts.extend(("LEASE", lease_token))
        result, _ = self._put_with_generation(parts)
        return result

    def lease(self, key: str) -> LeaseResult:
        response = self.command("MC.LEASE", key, "WINDOWS")
        if not isinstance(response, list) or not response:
            raise self._protocol_error("invalid MC.LEASE response")
        state = self._expect_text(response[0], "MC.LEASE state")
        if state == "fresh":
            self._expect_arity(response, "MC.LEASE fresh", (2, 4))
            return LeaseResult(
                state,
                value=self._expect_bulk(response[1], "MC.LEASE fresh"),
                expires_in_seconds=(
                    self._lease_milliseconds(response[2], "expires")
                    if len(response) > 2 else None
                ),
                stale_for_seconds=(
                    self._lease_milliseconds(response[3], "stale")
                    if len(response) > 3 else None
                ),
            )
        if state == "stale":
            self._expect_arity(response, "MC.LEASE stale", (2, 3))
            return LeaseResult(
                state,
                value=self._expect_bulk(response[1], "MC.LEASE stale"),
                stale_for_seconds=(
                    self._lease_milliseconds(response[2], "stale")
                    if len(response) > 2 else None
                ),
            )
        if state == "stale_lease":
            self._expect_arity(response, "MC.LEASE stale_lease", (3, 4))
            return LeaseResult(
                state,
                value=self._expect_bulk(
                    response[1], "MC.LEASE stale_lease"
                ),
                lease_token=self._expect_text(
                    response[2], "MC.LEASE lease token"
                ),
                stale_for_seconds=(
                    self._lease_milliseconds(response[3], "stale")
                    if len(response) > 3 else None
                ),
            )
        if state == "lease":
            self._expect_arity(response, "MC.LEASE lease", (2,))
            return LeaseResult(
                state,
                lease_token=self._expect_text(
                    response[1], "MC.LEASE lease token"
                ),
            )
        if state == "loading":
            self._expect_arity(response, "MC.LEASE loading", (2,))
            retry = self._expect_integer(
                response[1], "MC.LEASE retry", minimum=0
            )
            return LeaseResult(state, retry_after_seconds=retry / 1000)
        raise self._protocol_error("unknown MC.LEASE state")

    def fetch(
        self,
        key: str,
        origin: str,
        path: str,
        *,
        refresh: bool = False,
        traceparent: Optional[str] = None,
    ) -> FetchResult:
        parts: List[CommandPart] = ["MC.FETCH", key, origin, path]
        if refresh:
            parts.append("REFRESH")
        selected_trace = traceparent or self._current_traceparent()
        if selected_trace is not None:
            parts.extend(("TRACEPARENT", selected_trace))
        document = self._json(self.command(*parts), "MC.FETCH")
        if not isinstance(document.get("state"), str):
            raise self._protocol_error(
                "MC.FETCH JSON state must be a string"
            )
        if not isinstance(document.get("origin"), str):
            raise self._protocol_error(
                "MC.FETCH JSON origin must be a string"
            )
        self._optional_json_integer(document, "status_code", minimum=100)
        self._optional_json_integer(document, "attempts", minimum=0)
        if document.get("error") is not None and not isinstance(
            document["error"], str
        ):
            raise self._protocol_error(
                "MC.FETCH JSON error must be a string or null"
            )
        return FetchResult(
            state=document["state"],
            origin=document["origin"],
            value=document.get("value"),
            status_code=document.get("status_code"),
            attempts=document.get("attempts", 0),
            error=document.get("error"),
        )

    def invalidate(self, *tags: str) -> int:
        with self._mutation_lock:
            result = self._expect_integer(
                self.command("MC.INVALIDATE", *tags),
                "MC.INVALIDATE",
                minimum=0,
            )
            if result:
                self._mutation_succeeded()
        return result

    def status(self) -> Dict[str, Any]:
        document = self._json(self.command("MC.STATUS"), "MC.STATUS")
        if not isinstance(document.get("degraded"), bool):
            raise self._protocol_error(
                "MC.STATUS JSON degraded must be a boolean"
            )
        return document

    def invalidations(self) -> Dict[str, Any]:
        document = self._json(
            self.command(
                "MC.INVALIDATIONS",
                "" if self._cursor is None else self._cursor,
            ),
            "MC.INVALIDATIONS",
        )
        if not isinstance(document.get("cursor"), (str, int)) or isinstance(
            document.get("cursor"), bool
        ):
            raise self._protocol_error(
                "MC.INVALIDATIONS JSON cursor must be a string or integer"
            )
        has_epoch = "epoch" in document
        has_generation = "generation" in document
        if has_epoch != has_generation:
            raise self._protocol_error(
                "MC.INVALIDATIONS JSON epoch and generation must appear together"
            )
        if has_epoch and not isinstance(document["epoch"], str):
            raise self._protocol_error(
                "MC.INVALIDATIONS JSON epoch must be a string"
            )
        if has_generation:
            self._optional_json_integer(
                document, "generation", minimum=0, required=True
            )
        if "changed" in document and not isinstance(document["changed"], bool):
            raise self._protocol_error(
                "MC.INVALIDATIONS JSON changed must be a boolean"
            )
        return document

    def set_traceparent(self, traceparent: str) -> None:
        self._expect_ok(
            self.command("MC.TRACEPARENT", traceparent), "MC.TRACEPARENT"
        )
        self._active_traceparent = traceparent.lower()

    @contextmanager
    def traceparent_scope(self, traceparent: Optional[str]) -> Any:
        token = _request_traceparent.set(
            None if traceparent is None else traceparent.lower()
        )
        try:
            yield
        finally:
            _request_traceparent.reset(token)

    def get_or_load(
        self,
        key: str,
        loader: Callable[[], Union[str, bytes]],
        policy: CachePolicy = CachePolicy(),
    ) -> CachedValue:
        self._poll_invalidations()
        cached = self.local.get(key)
        if cached is not None and cached.state == "fresh":
            return cached

        with self._flight_lock:
            flight = self._flights.get(key)
            leader = flight is None
            if leader:
                flight = _Flight(threading.Event())
                self._flights[key] = flight
        assert flight is not None
        if not leader:
            flight.event.wait()
            if flight.result is not None:
                if (
                    flight.result.state != "stale_if_error"
                    or (
                        flight.result.stale_deadline is not None
                        and time.monotonic()
                        < flight.result.stale_deadline
                    )
                ):
                    return flight.result
            if flight.error is not None:
                raise flight.error
            raise MegaCacheClientError("coalesced load produced no valid value")

        stale = cached
        try:
            while True:
                expected_generation = self._current_generation()
                lease_started = time.monotonic()
                lease = self.lease(key)
                lease_elapsed = _elapsed_since(lease_started)
                if lease.state == "fresh" and lease.value is not None:
                    fresh_budget = (
                        policy.ttl_seconds
                        if (
                            lease.expires_in_seconds is None
                            or lease.expires_in_seconds < 0
                        )
                        else min(
                            policy.ttl_seconds,
                            lease.expires_in_seconds,
                        )
                    )
                    stale_budget = (
                        policy.stale_seconds
                        if (
                            lease.stale_for_seconds is None
                            or lease.stale_for_seconds < 0
                        )
                        else min(
                            policy.stale_seconds,
                            lease.stale_for_seconds,
                        )
                    )
                    fresh_seconds, stale_seconds = _remaining_windows(
                        fresh_budget, stale_budget, lease_elapsed
                    )
                    if fresh_seconds + stale_seconds > 0:
                        self._admit_if_generation(
                            expected_generation,
                            key,
                            lease.value,
                            fresh_seconds,
                            stale_seconds,
                        )
                    result = CachedValue(lease.value, "fresh")
                    flight.result = result
                    return result
                if lease.state == "stale" and lease.value is not None:
                    stale_budget = min(
                        policy.stale_seconds,
                        policy.stale_seconds
                        if (
                            lease.stale_for_seconds is None
                            or lease.stale_for_seconds < 0
                        )
                        else lease.stale_for_seconds,
                    )
                    remaining = max(0.0, stale_budget - lease_elapsed)
                    if remaining > 0:
                        self._admit_if_generation(
                            expected_generation, key, lease.value, 0, remaining
                        )
                    result = CachedValue(
                        lease.value,
                        "stale",
                        _deadline_after(lease_started, stale_budget),
                    )
                    flight.result = result
                    return result
                if lease.state == "loading":
                    time.sleep(max(0.001, lease.retry_after_seconds))
                    continue
                if lease.state in ("lease", "stale_lease"):
                    if lease.state == "stale_lease" and lease.value is not None:
                        stale_budget = min(
                            policy.stale_seconds,
                            policy.stale_seconds
                            if (
                                lease.stale_for_seconds is None
                                or lease.stale_for_seconds < 0
                            )
                            else lease.stale_for_seconds,
                        )
                        stale = CachedValue(
                            lease.value,
                            "stale",
                            _deadline_after(lease_started, stale_budget),
                        )
                    value = loader()
                    encoded = value.encode("utf-8") if isinstance(value, str) else value
                    parts: List[CommandPart] = ["MC.SET", key, encoded]
                    parts.extend(("TTL", policy.ttl_seconds))
                    parts.extend(("STALE", policy.stale_seconds))
                    if policy.tags:
                        parts.extend(("TAGS", len(policy.tags)))
                        parts.extend(policy.tags)
                    if lease.lease_token is not None:
                        parts.extend(("LEASE", lease.lease_token))
                    put_started = time.monotonic()
                    _, own_generation = self._put_with_generation(parts)
                    fresh_seconds, stale_seconds = _remaining_windows(
                        policy.ttl_seconds,
                        policy.stale_seconds,
                        _elapsed_since(put_started),
                    )
                    if fresh_seconds + stale_seconds > 0:
                        self._admit_if_generation(
                            own_generation,
                            key,
                            encoded,
                            fresh_seconds,
                            stale_seconds,
                        )
                    result = CachedValue(encoded, "loaded")
                    flight.result = result
                    return result
        except BaseException as exc:
            if (
                stale is not None
                and policy.stale_if_error
                and stale.stale_deadline is not None
                and time.monotonic() < stale.stale_deadline
            ):
                result = CachedValue(
                    stale.value, "stale_if_error", stale.stale_deadline
                )
                flight.result = result
                flight.error = exc
                return result
            flight.error = exc
            raise
        finally:
            with self._flight_lock:
                if self._flights.get(key) is flight:
                    self._flights.pop(key, None)
                flight.event.set()

    def _poll_invalidations(self) -> None:
        now = time.monotonic()
        if now - self._last_poll < self.invalidation_poll_seconds:
            return
        document = self.invalidations()
        cursor = str(document["cursor"])
        epoch_value = document.get("epoch")
        generation_value = document.get("generation")
        epoch = None if epoch_value is None else str(epoch_value)
        generation = (
            None if generation_value is None else int(generation_value)
        )
        changed = (
            self._cursor is not None
            and (
                cursor != self._cursor
                if epoch is None or generation is None
                else (
                    epoch != self._cursor_epoch
                    or generation != self._cursor_generation
                )
            )
        )
        if changed:
            self._mutation_succeeded()
        self._cursor = cursor
        self._cursor_epoch = epoch
        self._cursor_generation = generation
        self._last_poll = now

    def _current_traceparent(self) -> Optional[str]:
        scoped = _request_traceparent.get()
        if scoped is not None:
            return scoped
        if self.traceparent_provider is not None:
            value = self.traceparent_provider()
            if value is not None:
                return value
        return self._active_traceparent

    def _put_with_generation(
        self, parts: Sequence[CommandPart]
    ) -> Tuple[str, int]:
        return self._mutation_command(parts)

    def _mutation_command(
        self, parts: Sequence[CommandPart]
    ) -> Tuple[str, int]:
        with self._mutation_lock:
            result = self._expect_ok(
                self.command(*parts), str(parts[0]).upper()
            )
            return result, self._mutation_succeeded()

    def _mutation_succeeded(self) -> int:
        with self._flight_lock:
            self._mutation_generation += 1
            generation = self._mutation_generation
            self.local.clear()
            return generation

    def _current_generation(self) -> int:
        with self._flight_lock:
            return self._mutation_generation

    def _admit_if_generation(
        self,
        generation: int,
        key: str,
        value: bytes,
        ttl_seconds: float,
        stale_seconds: float,
    ) -> bool:
        with self._flight_lock:
            if generation != self._mutation_generation:
                return False
            return self.local.put(key, value, ttl_seconds, stale_seconds)

    def _protocol_error(self, message: str) -> MegaCacheProtocolError:
        try:
            self.close()
        except Exception:
            pass
        return MegaCacheProtocolError(message)

    def _expect_text(self, value: Any, command: str) -> str:
        if isinstance(value, str):
            return value
        if isinstance(value, bytes):
            try:
                return value.decode("utf-8")
            except UnicodeDecodeError as exc:
                raise self._protocol_error(
                    "{} response is not valid UTF-8".format(command)
                ) from exc
        raise self._protocol_error(
            "{} response must be a string".format(command)
        )

    def _expect_ok(self, value: Any, command: str) -> str:
        if not isinstance(value, str) or value != "OK":
            raise self._protocol_error(
                "{} response must be OK".format(command)
            )
        return value

    def _expect_bulk(
        self, value: Any, command: str, nullable: bool = False
    ) -> Optional[bytes]:
        if value is None and nullable:
            return None
        if not isinstance(value, bytes):
            raise self._protocol_error(
                "{} response must be a bulk string{}".format(
                    command, " or null" if nullable else ""
                )
            )
        return value

    def _expect_integer(
        self,
        value: Any,
        command: str,
        *,
        minimum: Optional[int] = None,
        maximum: Optional[int] = None,
    ) -> int:
        if not isinstance(value, int) or isinstance(value, bool):
            raise self._protocol_error(
                "{} response must be an integer".format(command)
            )
        if minimum is not None and value < minimum:
            raise self._protocol_error(
                "{} response is below its valid range".format(command)
            )
        if maximum is not None and value > maximum:
            raise self._protocol_error(
                "{} response is above its valid range".format(command)
            )
        return value

    def _expect_arity(
        self, values: Sequence[Any], command: str, allowed: Tuple[int, ...]
    ) -> None:
        if len(values) not in allowed:
            raise self._protocol_error(
                "{} response has invalid arity".format(command)
            )

    def _lease_milliseconds(self, value: Any, field: str) -> float:
        milliseconds = self._expect_integer(
            value, "MC.LEASE {}".format(field), minimum=-1
        )
        return milliseconds / 1000

    def _json(self, value: Any, command: str) -> Dict[str, Any]:
        encoded = self._expect_bulk(value, command)
        assert encoded is not None
        try:
            raw = encoded.decode("utf-8")
            document = json.loads(
                raw,
                parse_constant=lambda constant: (_ for _ in ()).throw(
                    ValueError("invalid JSON constant {}".format(constant))
                ),
            )
        except (UnicodeDecodeError, ValueError, TypeError) as exc:
            raise self._protocol_error(
                "{} returned invalid JSON".format(command)
            ) from exc
        if not isinstance(document, dict):
            raise self._protocol_error(
                "{} must return a JSON object".format(command)
            )
        return document

    def _optional_json_integer(
        self,
        document: Dict[str, Any],
        field: str,
        *,
        minimum: Optional[int] = None,
        required: bool = False,
    ) -> None:
        if field not in document:
            if required:
                raise self._protocol_error(
                    "missing JSON field {}".format(field)
                )
            return
        value = document[field]
        if value is None and not required:
            return
        self._expect_integer(
            value, "JSON field {}".format(field), minimum=minimum
        )

    def _read_response(
        self, depth: int = 0, budget: Optional[List[int]] = None
    ) -> Any:
        assert self._stream is not None
        if depth > _MAX_RESPONSE_DEPTH:
            raise MegaCacheProtocolError("RESP nesting exceeds limit")
        if budget is None:
            budget = [0]
        marker = self._stream.read(1)
        if not marker:
            raise EOFError("server closed the connection")
        self._consume_response_bytes(budget, 1)
        line = self._readline(budget)
        if marker == b"+":
            return line.decode("utf-8")
        if marker == b"-":
            raise MegaCacheCommandError(line.decode("utf-8", "replace"))
        if marker == b":":
            try:
                return int(line)
            except ValueError as exc:
                raise MegaCacheProtocolError("invalid integer response") from exc
        if marker == b"$":
            return self._read_bulk(line, budget)
        if marker == b"*":
            try:
                count = int(line)
            except ValueError as exc:
                raise MegaCacheProtocolError("invalid array response") from exc
            if count == -1:
                return None
            if count < 0 or count > _MAX_ARRAY_ITEMS:
                raise MegaCacheProtocolError("invalid array length")
            values = []
            for _ in range(count):
                values.append(self._read_response(depth + 1, budget))
            return values
        raise MegaCacheProtocolError(
            "unknown RESP marker {!r}".format(marker)
        )

    def _read_bulk(
        self, length_line: bytes, budget: List[int]
    ) -> Optional[bytes]:
        assert self._stream is not None
        try:
            length = int(length_line)
        except ValueError as exc:
            raise MegaCacheProtocolError("invalid bulk string length") from exc
        if length == -1:
            return None
        if length < 0:
            raise MegaCacheProtocolError("invalid bulk string length")
        if length > _MAX_BULK_BYTES:
            raise MegaCacheProtocolError("bulk string exceeds limit")
        self._consume_response_bytes(budget, length + 2)
        value = self._stream.read(length)
        if len(value) != length or self._stream.read(2) != b"\r\n":
            raise MegaCacheProtocolError("incomplete bulk string response")
        return value

    def _readline(self, budget: List[int]) -> bytes:
        assert self._stream is not None
        line = self._stream.readline(65_537)
        self._consume_response_bytes(budget, len(line))
        if len(line) > 65_536 or not line.endswith(b"\r\n"):
            raise MegaCacheProtocolError("unterminated or oversized response")
        return line[:-2]

    @staticmethod
    def _consume_response_bytes(budget: List[int], count: int) -> None:
        budget[0] += count
        if budget[0] > _MAX_RESPONSE_BYTES:
            raise MegaCacheProtocolError("response exceeds byte limit")

    @staticmethod
    def _encode_part(value: CommandPart) -> bytes:
        if isinstance(value, bytes):
            return value
        if isinstance(value, int):
            return str(value).encode("ascii")
        if isinstance(value, str):
            return value.encode("utf-8")
        raise TypeError("command parts must be strings, bytes, or integers")
