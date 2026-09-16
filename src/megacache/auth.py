"""Authentication and authorization for HTTP and RESP clients."""

import base64
import hashlib
import hmac
import json
import os
import secrets
import threading
import time
from collections import OrderedDict
from dataclasses import dataclass
from typing import FrozenSet, Iterable, Optional

_PBKDF2_ITERATIONS = 600_000
_PERMISSIONS = frozenset(("read", "write", "invalidate", "admin"))
_PASSWORD_VERIFY_SLOTS = threading.BoundedSemaphore(4)


@dataclass(frozen=True)
class Principal:
    username: str
    permissions: FrozenSet[str]
    key_prefixes: tuple

    def allows(self, permission: str, keys: Iterable[str] = ()) -> bool:
        if "admin" not in self.permissions and permission not in self.permissions:
            return False
        if not self.key_prefixes:
            return True
        return all(
            any(key.startswith(prefix) for prefix in self.key_prefixes)
            for key in keys
        )


@dataclass(frozen=True)
class _User:
    username: str
    password_hash: str
    permissions: FrozenSet[str]
    key_prefixes: tuple


class AuthManager:
    def __init__(
        self, api_key: Optional[str] = None, users_file: Optional[str] = None
    ) -> None:
        self._api_key = api_key
        self._users_configured = users_file is not None
        self._users = self._load_users(users_file)
        self._success_cache = OrderedDict()
        self._cache_lock = threading.Lock()

    @property
    def required(self) -> bool:
        return self._api_key is not None or self._users_configured

    def anonymous(self) -> Optional[Principal]:
        if self.required:
            return None
        return Principal("anonymous", frozenset(("admin",)), ())

    def authenticate(
        self, username: Optional[str], password: str
    ) -> Optional[Principal]:
        if (
            self._api_key is not None
            and username in (None, "default")
            and hmac.compare_digest(password, self._api_key)
        ):
            return Principal("legacy-api-key", frozenset(("admin",)), ())
        if username is None:
            return None
        user = self._users.get(username)
        if user is None:
            return None
        cache_key = hashlib.sha256(
            username.encode("utf-8")
            + b"\x00"
            + password.encode("utf-8")
        ).digest()
        now = time.monotonic()
        with self._cache_lock:
            cached = self._success_cache.get(cache_key)
            if cached is not None and cached[0] > now:
                self._success_cache.move_to_end(cache_key)
                return cached[1]
            self._success_cache.pop(cache_key, None)
        if not _PASSWORD_VERIFY_SLOTS.acquire(blocking=False):
            return None
        try:
            if not verify_password(password, user.password_hash):
                return None
        finally:
            _PASSWORD_VERIFY_SLOTS.release()
        principal = Principal(user.username, user.permissions, user.key_prefixes)
        with self._cache_lock:
            self._success_cache[cache_key] = (now + 60, principal)
            self._success_cache.move_to_end(cache_key)
            while len(self._success_cache) > 1024:
                self._success_cache.popitem(last=False)
        return principal

    @staticmethod
    def _load_users(path: Optional[str]) -> dict:
        if path is None:
            return {}
        try:
            with open(path, "r", encoding="utf-8") as source:
                document = json.load(source)
        except (OSError, json.JSONDecodeError) as exc:
            raise ValueError(
                "unable to load MEGACACHE_USERS_FILE: {}".format(exc)
            ) from exc
        if not isinstance(document, dict) or not isinstance(
            document.get("users"), list
        ):
            raise ValueError("users file must contain a users array")
        if not document["users"]:
            raise ValueError("users file must contain at least one user")
        users = {}
        for value in document["users"]:
            if not isinstance(value, dict):
                raise ValueError("each user must be an object")
            username = value.get("username")
            password_hash = value.get("password_hash")
            permissions = value.get("permissions", [])
            prefixes = value.get("key_prefixes", [])
            if (
                not isinstance(username, str)
                or not username
                or not isinstance(password_hash, str)
                or not isinstance(permissions, list)
                or not isinstance(prefixes, list)
                or any(not isinstance(item, str) or not item for item in prefixes)
            ):
                raise ValueError("invalid user definition")
            permission_set = frozenset(permissions)
            if not permission_set or not permission_set.issubset(_PERMISSIONS):
                raise ValueError("user has invalid or empty permissions")
            if username in users:
                raise ValueError("duplicate user '{}'".format(username))
            validate_password_hash(password_hash)
            users[username] = _User(
                username,
                password_hash,
                permission_set,
                tuple(prefixes),
            )
        return users


def hash_password(password: str, iterations: int = _PBKDF2_ITERATIONS) -> str:
    if not password:
        raise ValueError("password must not be empty")
    salt = secrets.token_bytes(16)
    digest = hashlib.pbkdf2_hmac(
        "sha256", password.encode("utf-8"), salt, iterations
    )
    return "pbkdf2_sha256${}${}${}".format(
        iterations,
        base64.b64encode(salt).decode("ascii"),
        base64.b64encode(digest).decode("ascii"),
    )


def verify_password(password: str, encoded: str) -> bool:
    iterations, salt, expected = validate_password_hash(encoded)
    actual = hashlib.pbkdf2_hmac(
        "sha256", password.encode("utf-8"), salt, iterations
    )
    return hmac.compare_digest(actual, expected)


def validate_password_hash(encoded: str) -> tuple:
    try:
        algorithm, raw_iterations, raw_salt, raw_digest = encoded.split("$", 3)
        iterations = int(raw_iterations)
        salt = base64.b64decode(raw_salt, validate=True)
        digest = base64.b64decode(raw_digest, validate=True)
    except (ValueError, TypeError) as exc:
        raise ValueError("invalid password hash") from exc
    if (
        algorithm != "pbkdf2_sha256"
        or iterations < 100_000
        or len(salt) < 16
        or len(digest) != 32
    ):
        raise ValueError("invalid password hash parameters")
    return iterations, salt, digest


def write_example_users_file(path: str, username: str, password: str) -> None:
    document = {
        "users": [
            {
                "username": username,
                "password_hash": hash_password(password),
                "permissions": ["admin"],
                "key_prefixes": [],
            }
        ]
    }
    flags = os.O_WRONLY | os.O_CREAT | os.O_EXCL
    descriptor = os.open(path, flags, 0o600)
    with os.fdopen(descriptor, "w", encoding="utf-8") as destination:
        json.dump(document, destination, indent=2)
        destination.write("\n")
