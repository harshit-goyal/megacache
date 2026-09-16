import io
import threading
import unittest
from contextlib import redirect_stderr, redirect_stdout

from megacache.cli import run
from megacache.client import MegaCacheClient, MegaCacheCommandError
from megacache.config import Config
from megacache.engine import CacheEngine
from megacache.resp import MegaCacheRespServer


class NativeClientTests(unittest.TestCase):
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
        cls.engine = CacheEngine(max_entries=100)
        cls.server = MegaCacheRespServer(("127.0.0.1", 0), config, cls.engine)
        cls.thread = threading.Thread(target=cls.server.serve_forever, daemon=True)
        cls.thread.start()
        cls.port = cls.server.server_address[1]

    @classmethod
    def tearDownClass(cls):
        cls.server.shutdown()
        cls.server.server_close()
        cls.thread.join(timeout=1)

    def setUp(self):
        self.engine.flush()

    def test_client_authenticates_and_preserves_binary_values(self):
        with MegaCacheClient(
            port=self.port, password="secret"
        ) as client:
            self.assertEqual("OK", client.command("SET", "key", b"\x00\xff"))
            self.assertEqual(b"\x00\xff", client.command("GET", "key"))

    def test_original_positional_client_authentication_is_preserved(self):
        with MegaCacheClient(
            "127.0.0.1", self.port, "secret", 2
        ) as client:
            self.assertEqual("PONG", client.command("PING"))

    def test_client_surfaces_server_errors(self):
        with MegaCacheClient(port=self.port) as client:
            with self.assertRaisesRegex(MegaCacheCommandError, "NOAUTH"):
                client.command("GET", "key")

    def test_native_cli_put_get_and_invalidate(self):
        connection = [
            "--port",
            str(self.port),
            "--password",
            "secret",
            "--json",
        ]
        output = io.StringIO()
        with redirect_stdout(output):
            code = run(
                connection
                + [
                    "put",
                    "product:1",
                    '{"id":1}',
                    "--ttl",
                    "60",
                    "--stale",
                    "120",
                    "--tag",
                    "products",
                ]
            )
        self.assertEqual(0, code)
        self.assertEqual('"OK"', output.getvalue().strip())

        output = io.StringIO()
        with redirect_stdout(output):
            code = run(connection + ["get", "product:1"])
        self.assertEqual(0, code)
        self.assertEqual('"{\\"id\\":1}"', output.getvalue().strip())

        output = io.StringIO()
        with redirect_stdout(output):
            code = run(connection + ["invalidate", "products"])
        self.assertEqual(0, code)
        self.assertEqual("1", output.getvalue().strip())

    def test_native_cli_returns_failure_for_missing_get(self):
        output = io.StringIO()
        with redirect_stdout(output), redirect_stderr(io.StringIO()):
            code = run(
                [
                    "--port",
                    str(self.port),
                    "--password",
                    "secret",
                    "get",
                    "missing",
                ]
            )
        self.assertEqual(1, code)
        self.assertEqual("(nil)", output.getvalue().strip())

    def test_native_cli_reports_version(self):
        output = io.StringIO()
        with redirect_stdout(output), self.assertRaises(SystemExit) as exit_status:
            run(["--version"])
        self.assertEqual(0, exit_status.exception.code)
        self.assertEqual("MegaCache 0.5.0", output.getvalue().strip())

    def test_native_cli_topology_and_status(self):
        connection = [
            "--port",
            str(self.port),
            "--password",
            "secret",
            "--json",
        ]
        for command in ("topology", "status"):
            output = io.StringIO()
            with redirect_stdout(output):
                code = run(connection + [command])
            self.assertEqual(0, code)
            document = __import__("json").loads(output.getvalue())
            self.assertFalse(document["degraded"])


if __name__ == "__main__":
    unittest.main()
