"""Dependency-free integration hooks for Python HTTP and database stacks."""

from typing import Any, Callable

from .client import CachePolicy, CachedValue, MegaCacheClient


def wsgi_traceparent(
    client: MegaCacheClient, application: Callable[..., Any]
) -> Callable[..., Any]:
    """Wrap Flask, Django, or another WSGI app without importing it."""

    def wrapped(environ: dict, start_response: Callable[..., Any]) -> Any:
        traceparent = environ.get("HTTP_TRACEPARENT")
        with client.traceparent_scope(traceparent):
            iterable = application(environ, start_response)

        def scoped_iterable() -> Any:
            try:
                iterator = iter(iterable)
                while True:
                    with client.traceparent_scope(traceparent):
                        try:
                            yield next(iterator)
                        except StopIteration:
                            return
            finally:
                close = getattr(iterable, "close", None)
                if close is not None:
                    close()

        return scoped_iterable()

    return wrapped


def cached_loader(
    client: MegaCacheClient, policy: CachePolicy
) -> Callable[[str, Callable[[], bytes]], CachedValue]:
    """Adapt any DB-driver callable to the SDK's coalesced cache loader."""

    def load(key: str, query: Callable[[], bytes]) -> CachedValue:
        return client.get_or_load(key, query, policy)

    return load
