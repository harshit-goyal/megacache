from megacache.client import CachePolicy, MegaCacheClient
from megacache.integrations import cached_loader, wsgi_traceparent

client = MegaCacheClient()
load_user = cached_loader(
    client,
    CachePolicy(ttl_seconds=60, stale_seconds=300, tags=("users",)),
)


def application(environ, start_response):
    value = load_user("user:42", lambda: b'{"id":42}')
    start_response("200 OK", [("Content-Type", "application/json")])
    return [value.value]


application = wsgi_traceparent(client, application)
