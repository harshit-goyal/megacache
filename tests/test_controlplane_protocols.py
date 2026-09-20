import base64
import contextlib
import hashlib
import hmac
import io
import json
import os
import tempfile
import threading
import time
import unittest
from http.client import HTTPConnection

from megacache.auth import hash_password
from megacache.cli import run
from megacache.config import Config
from megacache.controlplane import (
    BackupPolicy,
    ControlPlaneDefinition,
    DisasterRecoveryPlan,
    ManagedControlPlane,
    StaticKeyProvider,
    TenantDefinition,
    TenantQuotas,
    TenantRetention,
)
from megacache.engine import CacheEngine
from megacache.events import load_event_automation, webhook_signature_payload
from megacache.intelligence import CacheIntelligence
from megacache.origin import HTTPOrigin, OriginCache, OriginResponse
from megacache.resp import MegaCacheRespServer
from megacache.server import MegaCacheServer

from tests.test_resp import RespClient


def _tenant(tenant_id, origin, webhook):
    return TenantDefinition(
        tenant_id=tenant_id,
        display_name=tenant_id,
        enabled=True,
        quotas=TenantQuotas(50, 500_000, 50_000, 100, 100, 20, 2),
        origins=(origin,),
        webhooks=(webhook,),
        regions=("local",),
        primary_region="local",
        desired_version="1.0.0",
        desired_replicas=1,
        max_unavailable=1,
        drain=False,
        backup=BackupPolicy(0, 2, 3600),
        disaster_recovery=DisasterRecoveryPlan(3600, 60, ("local",)),
        retention=TenantRetention(24, 50, 2),
    )


class ControlPlaneProtocolTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.directory = tempfile.TemporaryDirectory()
        password_hash = hash_password("correct horse")
        cls.users_file = os.path.join(cls.directory.name, "users.json")
        with open(cls.users_file, "w", encoding="utf-8") as destination:
            json.dump(
                {
                    "version": 2,
                    "users": [
                        {
                            "username": "alice",
                            "password_hash": password_hash,
                            "permissions": ["admin"],
                            "key_prefixes": [],
                            "tenant_id": "alpha",
                            "roles": ["tenant_admin"],
                        },
                        {
                            "username": "bob",
                            "password_hash": password_hash,
                            "permissions": ["admin"],
                            "key_prefixes": [],
                            "tenant_id": "beta",
                            "roles": ["tenant_admin"],
                        },
                        {
                            "username": "platform",
                            "password_hash": password_hash,
                            "permissions": [],
                            "key_prefixes": [],
                            "tenant_id": "alpha",
                            "roles": [
                                "platform_admin",
                                "operator",
                                "auditor",
                                "billing_admin",
                            ],
                        },
                    ],
                },
                destination,
            )
        cls.events_file = os.path.join(cls.directory.name, "events.json")
        with open(cls.events_file, "w", encoding="utf-8") as destination:
            json.dump(
                {
                    "rules": [
                        {
                            "name": "item-change",
                            "sources": [
                                "database",
                                "alpha-hook",
                                "beta-hook",
                            ],
                            "operations": ["update"],
                            "keys": ["item:{payload.id}"],
                        }
                    ],
                    "webhooks": [
                        {"name": "alpha-hook", "secret": "alpha-secret-1234"},
                        {"name": "beta-hook", "secret": "beta-secret-12345"},
                    ],
                },
                destination,
            )
        tenants = (
            _tenant("alpha", "alpha-origin", "alpha-hook"),
            _tenant("beta", "beta-origin", "beta-hook"),
        )
        engines = {}
        tenant_paths = {}
        for item in tenants:
            origin = HTTPOrigin.from_dict(
                {
                    "name": item.origins[0],
                    "base_url": "https://origin.example",
                    "allowed_hosts": ["origin.example"],
                    "allowed_ports": [443],
                    "allowed_path_prefixes": ["/v1/"],
                    "retry_attempts": 0,
                }
            )
            engine = OriginCache(
                CacheIntelligence(CacheEngine(max_entries=50), enabled=True),
                [origin],
                resolver=lambda host, port: ["93.184.216.34"],
                transport=lambda origin, path, addresses: OriginResponse(
                    200, path.encode("utf-8")
                ),
            )
            event_path = os.path.join(
                cls.directory.name, item.tenant_id + "-events.json"
            )
            engine = load_event_automation(
                engine,
                cls.events_file,
                event_path,
                allowed_webhooks=item.webhooks,
            )
            engines[item.tenant_id] = engine
            tenant_paths[item.tenant_id] = (event_path,)
        cls.control = ManagedControlPlane(
            ControlPlaneDefinition("alpha", tenants),
            engines,
            state_directory=os.path.join(cls.directory.name, "control"),
            key_provider=StaticKeyProvider(b"m" * 32),
            tenant_state_paths=tenant_paths,
        )
        cls.config = Config(
            host="127.0.0.1",
            port=0,
            resp_host="127.0.0.1",
            resp_port=0,
            max_entries=100,
            max_memory_bytes=1_000_000,
            max_entry_bytes=50_000,
            max_body_bytes=100_000,
            default_ttl_seconds=60,
            default_stale_seconds=60,
            lease_seconds=10,
            shutdown_grace_seconds=1,
            api_key=None,
            tls_cert_file=None,
            tls_key_file=None,
            users_file=cls.users_file,
            log_format="text",
        )
        cls.http = MegaCacheServer(
            ("127.0.0.1", 0), cls.config, cls.control
        )
        cls.resp = MegaCacheRespServer(
            ("127.0.0.1", 0), cls.config, cls.control
        )
        cls.http_thread = threading.Thread(
            target=cls.http.serve_forever, daemon=True
        )
        cls.resp_thread = threading.Thread(
            target=cls.resp.serve_forever, daemon=True
        )
        cls.http_thread.start()
        cls.resp_thread.start()

    @classmethod
    def tearDownClass(cls):
        cls.http.shutdown()
        cls.resp.shutdown()
        cls.http.server_close()
        cls.resp.server_close()
        cls.http_thread.join(timeout=1)
        cls.resp_thread.join(timeout=1)
        cls.control.close(1)
        cls.directory.cleanup()

    def http_request(self, username, method, path, body=None):
        connection = HTTPConnection(
            "127.0.0.1", self.http.server_address[1], timeout=3
        )
        credential = base64.b64encode(
            "{}:correct horse".format(username).encode("utf-8")
        ).decode("ascii")
        headers = {"Authorization": "Basic " + credential}
        encoded = None
        if body is not None:
            encoded = json.dumps(body)
            headers["Content-Type"] = "application/json"
        connection.request(method, path, body=encoded, headers=headers)
        response = connection.getresponse()
        payload = json.loads(response.read())
        status = response.status
        connection.close()
        return status, payload

    def resp_client(self, username):
        client = RespClient(self.resp.server_address[1])
        self.assertEqual(
            "OK", client.command("AUTH", username, "correct horse")
        )
        return client

    def test_http_and_resp_keys_tags_and_stats_are_tenant_scoped(self):
        self.http_request(
            "alice",
            "PUT",
            "/v1/cache/shared",
            {"value": "alpha", "tags": ["common"]},
        )
        status, _ = self.http_request("bob", "GET", "/v1/cache/shared")
        self.assertEqual(404, status)
        self.http_request(
            "bob",
            "PUT",
            "/v1/cache/shared",
            {"value": "beta", "tags": ["common"]},
        )

        status, result = self.http_request(
            "alice", "POST", "/v1/invalidate", {"tags": ["common"]}
        )
        self.assertEqual(200, status)
        self.assertEqual(1, result["invalidated"])

        alice = self.resp_client("alice")
        bob = self.resp_client("bob")
        try:
            self.assertIsNone(alice.command("GET", "shared"))
            self.assertEqual(b"beta", bob.command("GET", "shared"))
            self.assertEqual(0, alice.command("DBSIZE"))
            self.assertEqual(1, bob.command("DBSIZE"))
        finally:
            alice.close()
            bob.close()

    def test_origins_events_and_intelligence_do_not_enumerate_other_tenants(self):
        status, origins = self.http_request("alice", "GET", "/v1/origins")
        self.assertEqual(200, status)
        self.assertEqual(["alpha-origin"], sorted(origins))
        status, origins = self.http_request("bob", "GET", "/v1/origins")
        self.assertEqual(200, status)
        self.assertEqual(["beta-origin"], sorted(origins))

        for username in ("alice", "bob"):
            self.http_request(
                username,
                "PUT",
                "/v1/cache/item%3A1",
                {"value": username},
            )
        event = {
            "event_id": "change-1",
            "source": "database",
            "stream": "items",
            "position": 1,
            "operation": "update",
            "payload": {"id": 1},
        }
        status, result = self.http_request(
            "alice", "POST", "/v1/events", event
        )
        self.assertEqual(200, status)
        self.assertEqual(1, result["invalidated_keys"])
        self.assertEqual(
            404, self.http_request("alice", "GET", "/v1/cache/item%3A1")[0]
        )
        self.assertEqual(
            "bob",
            self.http_request("bob", "GET", "/v1/cache/item%3A1")[1]["value"],
        )
        alice_status = self.http_request(
            "alice", "GET", "/v1/events/status"
        )[1]
        beta_status = self.http_request(
            "bob", "GET", "/v1/events/status"
        )[1]
        self.assertEqual(1, alice_status["metrics"]["processed_total"])
        self.assertEqual(0, beta_status["metrics"].get("processed_total", 0))
        self.assertEqual(("alpha-hook",), tuple(alice_status["webhooks"]))
        self.assertEqual(("beta-hook",), tuple(beta_status["webhooks"]))

    def test_control_rbac_and_native_cli_status(self):
        status, dashboard = self.http_request(
            "alice", "GET", "/v1/control/status"
        )
        self.assertEqual(200, status)
        self.assertEqual("alpha", dashboard["tenant_id"])
        status, payload = self.http_request(
            "alice", "GET", "/v1/control/status?tenant=beta"
        )
        self.assertEqual(404, status)
        self.assertEqual("not_found", payload["error"])
        status, tenants = self.http_request(
            "platform", "GET", "/v1/control/tenants"
        )
        self.assertEqual(200, status)
        self.assertEqual(
            ["alpha", "beta"],
            sorted(item["tenant_id"] for item in tenants["tenants"]),
        )
        self.assertEqual(
            403,
            self.http_request(
                "platform", "GET", "/v1/cache/shared"
            )[0],
        )

        output = io.StringIO()
        with contextlib.redirect_stdout(output):
            result = run(
                [
                    "--host",
                    "127.0.0.1",
                    "--port",
                    str(self.resp.server_address[1]),
                    "--username",
                    "alice",
                    "--password",
                    "correct horse",
                    "--json",
                    "control-status",
                ]
            )
        self.assertEqual(0, result)
        self.assertEqual("alpha", json.loads(output.getvalue())["tenant_id"])

    def test_webhook_source_is_cryptographically_routed_to_one_tenant(self):
        for username in ("alice", "bob"):
            self.http_request(
                username,
                "PUT",
                "/v1/cache/item%3A2",
                {"value": username},
            )
        document = {
            "event_id": "webhook-change-2",
            "stream": "items",
            "position": 2,
            "operation": "update",
            "payload": {"id": 2},
        }
        body = json.dumps(document, separators=(",", ":")).encode("utf-8")
        timestamp = str(int(time.time()))
        delivery = "delivery-alpha-2"
        signature = "sha256=" + hmac.new(
            b"alpha-secret-1234",
            webhook_signature_payload(
                "alpha-hook", timestamp, delivery, body
            ),
            hashlib.sha256,
        ).hexdigest()
        connection = HTTPConnection(
            "127.0.0.1", self.http.server_address[1], timeout=3
        )
        connection.request(
            "POST",
            "/v1/events/webhook/alpha-hook",
            body=body,
            headers={
                "Content-Type": "application/json",
                "X-MegaCache-Timestamp": timestamp,
                "X-MegaCache-Delivery": delivery,
                "X-MegaCache-Signature": signature,
            },
        )
        response = connection.getresponse()
        response.read()
        connection.close()
        self.assertEqual(200, response.status)
        self.assertEqual(
            404, self.http_request("alice", "GET", "/v1/cache/item%3A2")[0]
        )
        self.assertEqual(
            "bob",
            self.http_request("bob", "GET", "/v1/cache/item%3A2")[1]["value"],
        )

    def test_platform_deployment_metadata_and_async_backup_over_resp(self):
        client = self.resp_client("platform")
        try:
            desired = json.loads(
                client.command(
                    "MC.CONTROL.DEPLOYMENT",
                    "beta",
                    json.dumps(
                        {
                            "version": "1.1.0",
                            "regions": ["local"],
                            "replicas": 1,
                            "max_unavailable": 0,
                            "drain": True,
                        }
                    ),
                )
            )
            self.assertEqual("1.1.0", desired["version"])
            tenants = json.loads(client.command("MC.CONTROL.TENANTS"))
            self.assertEqual(2, len(tenants["tenants"]))
        finally:
            client.close()

        alice = self.resp_client("alice")
        try:
            operation = json.loads(alice.command("MC.BACKUP"))
            completed = self.control.wait_operation(
                operation["operation_id"], timeout=3
            )
            self.assertEqual("succeeded", completed["status"])
            cross = alice.command("MC.CONTROL.STATUS", "beta")
            self.assertIsInstance(cross, RuntimeError)
            self.assertIn("tenant is not configured", str(cross))
        finally:
            alice.close()


if __name__ == "__main__":
    unittest.main()
