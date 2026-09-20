import json
import threading
import unittest
from http.client import HTTPConnection

from megacache.cluster import QuorumError
from megacache.config import Config
from megacache.engine import CacheEngine
from megacache.intelligence import CacheIntelligence
from megacache.origin import HTTPOrigin, OriginCache, OriginResponse
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
            max_memory_bytes=1_000_000,
            max_entry_bytes=10_000,
            max_body_bytes=10_000,
            default_ttl_seconds=60,
            default_stale_seconds=60,
            lease_seconds=10,
            shutdown_grace_seconds=1,
            api_key="secret",
            tls_cert_file=None,
            tls_key_file=None,
            users_file=None,
            log_format="text",
        )
        cls.engine = CacheIntelligence(
            OriginCache(
                CacheEngine(max_entries=100),
                [
                    HTTPOrigin.from_dict(
                        {
                            "name": "catalog",
                            "base_url": "https://origin.example",
                            "allowed_hosts": ["origin.example"],
                            "allowed_ports": [443],
                            "allowed_path_prefixes": ["/v1/"],
                            "retry_attempts": 0,
                        }
                    )
                ],
                resolver=lambda host, port: ["93.184.216.34"],
                transport=lambda origin, path, addresses: OriginResponse(
                    200, path.encode("utf-8")
                ),
            ),
            enabled=True,
        )
        cls.server = MegaCacheServer(
            ("127.0.0.1", 0), config, cls.engine
        )
        cls.thread = threading.Thread(target=cls.server.serve_forever, daemon=True)
        cls.thread.start()
        cls.port = cls.server.server_address[1]

    @classmethod
    def tearDownClass(cls):
        cls.server.shutdown()
        cls.server.server_close()
        cls.thread.join(timeout=1)
        cls.engine.close(1)

    def request(
        self, method, path, body=None, authenticated=True, extra_headers=None
    ):
        connection = HTTPConnection("127.0.0.1", self.port, timeout=2)
        headers = dict(extra_headers or {})
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

    def test_fetch_rejects_invalid_traceparent(self):
        status, payload = self.request(
            "POST",
            "/v1/fetch/trace",
            {"origin": "catalog", "path": "/v1/trace"},
            extra_headers={"traceparent": "not-a-traceparent"},
        )
        self.assertEqual(400, status)
        self.assertEqual("invalid_request", payload["error"])

    def test_empty_key_returns_validation_error(self):
        status, payload = self.request("GET", "/v1/cache/")
        self.assertEqual(400, status)
        self.assertEqual("invalid_request", payload["error"])

    def test_get_with_body_is_rejected_and_connection_closed(self):
        connection = HTTPConnection("127.0.0.1", self.port, timeout=2)
        connection.request(
            "GET",
            "/healthz",
            body="unexpected",
            headers={"Content-Type": "text/plain"},
        )
        response = connection.getresponse()
        response.read()
        self.assertEqual(400, response.status)
        self.assertEqual("close", response.getheader("Connection"))
        connection.close()

    def test_http_fetch_and_origin_health(self):
        status, payload = self.request(
            "POST",
            "/v1/fetch/http%3Aproduct%3A1",
            {"origin": "catalog", "path": "/v1/product/1"},
        )
        self.assertEqual(200, status)
        self.assertEqual("refreshed", payload["state"])
        self.assertEqual("/v1/product/1", payload["value"])

        status, payload = self.request("GET", "/v1/origins")
        self.assertEqual(200, status)
        self.assertEqual("closed", payload["catalog"]["breaker_state"])

        status, payload = self.request(
            "GET", "/v1/explain/http%3Aproduct%3A1"
        )
        self.assertEqual(200, status)
        self.assertEqual("fresh", payload["current"]["state"])
        self.assertEqual("catalog", payload["current"]["lineage"]["origin"])

    def test_policy_simulation_and_experiment_status(self):
        status, payload = self.request(
            "POST",
            "/v1/policies/simulate",
            {
                "records": [
                    {
                        "key": "product:1",
                        "base_ttl_seconds": 60,
                        "accesses": 20,
                        "loads": 2,
                        "changes": 1,
                    }
                ]
            },
        )
        self.assertEqual(200, status)
        self.assertTrue(payload["dry_run"])
        status, payload = self.request("GET", "/v1/experiments")
        self.assertEqual(200, status)
        self.assertEqual("disabled", payload["status"])

    def test_http_fetch_reports_quorum_failure_as_service_unavailable(self):
        original = self.engine.fetch

        def unavailable(*args, **kwargs):
            raise QuorumError("read quorum unavailable")

        self.engine.fetch = unavailable
        try:
            status, payload = self.request(
                "POST",
                "/v1/fetch/http%3Aquorum",
                {"origin": "catalog", "path": "/v1/quorum"},
            )
        finally:
            self.engine.fetch = original
        self.assertEqual(503, status)
        self.assertEqual("quorum_unavailable", payload["error"])


if __name__ == "__main__":
    unittest.main()
