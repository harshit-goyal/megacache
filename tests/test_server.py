import json
import threading
import unittest
from http.client import HTTPConnection

from megacache.config import Config
from megacache.engine import CacheEngine
from megacache.server import MegaCacheServer


class ServerTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        config = Config(
            host="127.0.0.1",
            port=0,
            resp_host="127.0.0.1",
            resp_port=0,
            max_entries=100,
            max_body_bytes=10_000,
            default_ttl_seconds=60,
            default_stale_seconds=60,
            lease_seconds=10,
            api_key="secret",
        )
        cls.server = MegaCacheServer(
            ("127.0.0.1", 0), config, CacheEngine(max_entries=100)
        )
        cls.thread = threading.Thread(target=cls.server.serve_forever, daemon=True)
        cls.thread.start()
        cls.port = cls.server.server_address[1]

    @classmethod
    def tearDownClass(cls):
        cls.server.shutdown()
        cls.server.server_close()
        cls.thread.join(timeout=1)

    def request(self, method, path, body=None, authenticated=True):
        connection = HTTPConnection("127.0.0.1", self.port, timeout=2)
        headers = {}
        if authenticated:
            headers["Authorization"] = "Bearer secret"
        if body is not None:
            body = json.dumps(body)
            headers["Content-Type"] = "application/json"
        connection.request(method, path, body=body, headers=headers)
        response = connection.getresponse()
        payload = json.loads(response.read())
        connection.close()
        return response.status, payload

    def test_cache_crud_and_invalidation(self):
        status, _ = self.request(
            "PUT",
            "/v1/cache/product%3A1",
            {"value": {"name": "Desk"}, "tags": ["products"]},
        )
        self.assertEqual(201, status)
        status, payload = self.request("GET", "/v1/cache/product%3A1")
        self.assertEqual(200, status)
        self.assertEqual({"name": "Desk"}, payload["value"])
        status, payload = self.request(
            "POST", "/v1/invalidate", {"tags": ["products"]}
        )
        self.assertEqual(1, payload["invalidated"])
        status, _ = self.request("GET", "/v1/cache/product%3A1")
        self.assertEqual(404, status)

    def test_lease_protocol(self):
        status, lease = self.request("POST", "/v1/lease/report%3A1")
        self.assertEqual(201, status)
        status, loading = self.request("POST", "/v1/lease/report%3A1")
        self.assertEqual(202, status)
        self.assertEqual("loading", loading["state"])
        status, _ = self.request(
            "PUT",
            "/v1/cache/report%3A1",
            {"value": "done", "lease_token": lease["lease_token"]},
        )
        self.assertEqual(201, status)

    def test_authentication_is_required(self):
        status, payload = self.request(
            "GET", "/v1/stats", authenticated=False
        )
        self.assertEqual(401, status)
        self.assertEqual("unauthorized", payload["error"])

    def test_health_is_public(self):
        status, payload = self.request(
            "GET", "/healthz", authenticated=False
        )
        self.assertEqual(200, status)
        self.assertEqual("ok", payload["status"])

    def test_empty_key_returns_validation_error(self):
        status, payload = self.request("GET", "/v1/cache/")
        self.assertEqual(400, status)
        self.assertEqual("invalid_request", payload["error"])


if __name__ == "__main__":
    unittest.main()
