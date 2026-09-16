"""Environment-backed configuration."""

import os
from dataclasses import dataclass
from typing import Optional


def _positive_int(name: str, default: int) -> int:
    raw = os.getenv(name, str(default))
    try:
        value = int(raw)
    except ValueError as exc:
        raise ValueError("{} must be an integer".format(name)) from exc
    if value <= 0:
        raise ValueError("{} must be greater than zero".format(name))
    return value


@dataclass(frozen=True)
class Config:
    host: str
    port: int
    resp_host: str
    resp_port: int
    max_entries: int
    max_body_bytes: int
    default_ttl_seconds: int
    default_stale_seconds: int
    lease_seconds: int
    api_key: Optional[str]

    @classmethod
    def from_env(cls) -> "Config":
        return cls(
            host=os.getenv("MEGACACHE_HOST", "0.0.0.0"),
            port=_positive_int("MEGACACHE_PORT", 8080),
            resp_host=os.getenv("MEGACACHE_RESP_HOST", "0.0.0.0"),
            resp_port=_positive_int("MEGACACHE_RESP_PORT", 6380),
            max_entries=_positive_int("MEGACACHE_MAX_ENTRIES", 10_000),
            max_body_bytes=_positive_int("MEGACACHE_MAX_BODY_BYTES", 1_048_576),
            default_ttl_seconds=_positive_int(
                "MEGACACHE_DEFAULT_TTL_SECONDS", 300
            ),
            default_stale_seconds=_positive_int(
                "MEGACACHE_DEFAULT_STALE_SECONDS", 900
            ),
            lease_seconds=_positive_int("MEGACACHE_LEASE_SECONDS", 30),
            api_key=os.getenv("MEGACACHE_API_KEY") or None,
        )
