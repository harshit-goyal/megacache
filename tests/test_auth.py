import base64
import json
import os
import tempfile
import threading
import unittest
from http.client import HTTPConnection

from megacache.auth import AuthManager, hash_password, verify_password
from megacache.client import MegaCacheClient, MegaCacheCommandError
from megacache.config import Config
from megacache.engine import CacheEngine
from megacache.resp import MegaCacheRespServer
from megacache.server import MegaCacheServer


class AuthenticationTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.directory = tempfile.TemporaryDirectory()
        cls.users_file = os.path.join(cls.directory.name, "users.json")
        password_hash = hash_password("correct horse")
        with open(cls.users_file, "w", encoding="utf-8") as destination:
            json.dump(
                {
                    "users": [
                        {
                            "username": "reader",
                            "password_hash": password_hash,
                            "permissions": ["read"],
                            "key_prefixes": ["tenant:1:"],
                        },
                        {
                            "username": "admin",
                            "password_hash": password_hash,
                            "permissions": ["admin"],
                            "key_prefixes": [],
                        },
                        {
                            "username": "writer",
                            "password_hash": password_hash,
                            "permissions": ["write"],
                            "key_prefixes": ["tenant:1:"],
                        },
                    ]
                },
                destination,
            )

    @classmethod
    def tearDownClass(cls):
        cls.directory.cleanup()

    def test_password_hash_is_salted_and_verifiable(self):
        first = hash_password("password")
        second = hash_password("password")
        self.assertNotEqual(first, second)
        self.assertTrue(verify_password("password", first))
        self.assertFalse(verify_password("wrong", first))

    def test_principal_enforces_permissions_and_prefixes(self):
        auth = AuthManager(users_file=self.users_file)
        reader = auth.authenticate("reader", "correct horse")
        self.assertIsNotNone(reader)
        self.assertTrue(reader.allows("read", ("tenant:1:key",)))
        self.assertFalse(reader.allows("write", ("tenant:1:key",)))
        self.assertFalse(reader.allows("read", ("tenant:2:key",)))
        self.assertIsNone(auth.authenticate("reader", "wrong"))

    def test_empty_configured_users_file_is_rejected(self):
        path = os.path.join(self.directory.name, "empty-users.json")
        with open(path, "w", encoding="utf-8") as destination:
            json.dump({"users": []}, destination)
        with self.assertRaisesRegex(ValueError, "at least one user"):
            AuthManager(users_file=path)

    def test_resp_named_user_permissions(self):
        config = self._config()
        engine = CacheEngine(max_entries=100)
        engine.put("tenant:1:key", b"value")
        server = MegaCacheRespServer(("127.0.0.1", 0), config, engine)
        thread = threading.Thread(target=server.serve_forever, daemon=True)
        thread.start()
        try:
            with MegaCacheClient(
                port=server.server_address[1],
                username="reader",
                password="correct horse",
            ) as client:
                self.assertEqual(b"value", client.command("GET", "tenant:1:key"))
                with self.assertRaisesRegex(MegaCacheCommandError, "NOPERM"):
                    client.command("SET", "tenant:1:key", "changed")
                with self.assertRaisesRegex(MegaCacheCommandError, "NOPERM"):
                    client.command("GET", "tenant:2:key")
            with MegaCacheClient(
                port=server.server_address[1],
                username="writer",
                password="correct horse",
            ) as client:
                with self.assertRaisesRegex(MegaCacheCommandError, "NOPERM"):
                    client.command("MC.LEASE", "tenant:1:key")
        finally:
            server.shutdown()
            server.server_close()
            thread.join(timeout=1)

    def test_http_basic_auth_enforces_permissions_and_prefixes(self):
        engine = CacheEngine(max_entries=100)
        engine.put("tenant:1:key", "value")
        server = MegaCacheServer(("127.0.0.1", 0), self._config(), engine)
        thread = threading.Thread(target=server.serve_forever, daemon=True)
        thread.start()
        credential = base64.b64encode(
            b"reader:correct horse"
        ).decode("ascii")
        try:
            connection = HTTPConnection(
                "127.0.0.1", server.server_address[1], timeout=2
            )
            connection.request(
                "GET",
                "/v1/cache/tenant%3A1%3Akey",
                headers={"Authorization": "Basic " + credential},
            )
            response = connection.getresponse()
            response.read()
            self.assertEqual(200, response.status)
            connection.close()

            connection = HTTPConnection(
                "127.0.0.1", server.server_address[1], timeout=2
            )
            connection.request(
                "DELETE",
                "/v1/cache/tenant%3A1%3Akey",
                headers={"Authorization": "Basic " + credential},
            )
            response = connection.getresponse()
            response.read()
            self.assertEqual(403, response.status)
            connection.close()
        finally:
            server.shutdown()
            server.server_close()
            thread.join(timeout=1)

    def _config(self):
        return Config(
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
            api_key=None,
            tls_cert_file=None,
            tls_key_file=None,
            users_file=self.users_file,
            log_format="text",
        )


if __name__ == "__main__":
    unittest.main()
