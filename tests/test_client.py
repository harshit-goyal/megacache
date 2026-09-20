import io
import ssl
import threading
import time
import unittest
from contextlib import redirect_stderr, redirect_stdout
from unittest.mock import Mock, patch

from megacache.cli import run
from megacache.client import (
    CachePolicy,
    LeaseResult,
    MegaCacheClient,
    MegaCacheCommandError,
    MegaCacheConnectionError,
    MegaCacheProtocolError,
)
from megacache.config import Config
from megacache.engine import CacheEngine
from megacache.integrations import wsgi_traceparent
from megacache.intelligence import CacheIntelligence
from megacache.origin import HTTPOrigin, OriginCache, OriginResponse
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
        cls.traces = []

        def transport(origin, path, addresses, headers):
            cls.traces.append(headers.get("traceparent"))
            return OriginResponse(200, path.encode("utf-8"))

        cls.engine = OriginCache(
            CacheIntelligence(
                CacheEngine(max_entries=100), enabled=True
            ),
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
            transport=transport,
        )
        cls.server = MegaCacheRespServer(("127.0.0.1", 0), config, cls.engine)
        cls.thread = threading.Thread(target=cls.server.serve_forever, daemon=True)
        cls.thread.start()
        cls.port = cls.server.server_address[1]

    @classmethod
    def tearDownClass(cls):
        cls.server.shutdown()
        cls.server.server_close()
        cls.thread.join(timeout=1)
        cls.engine.close(1)

    def setUp(self):
        self.engine.flush()
        self.traces.clear()

    def test_client_authenticates_and_preserves_binary_values(self):
        with MegaCacheClient("127.0.0.1", self.port, "secret") as client:
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

    def test_native_cli_explain(self):
        self.engine.put("product:explain", "value", ttl_seconds=60)
        output = io.StringIO()
        with redirect_stdout(output):
            code = run(
                [
                    "--port",
                    str(self.port),
                    "--password",
                    "secret",
                    "--json",
                    "explain",
                    "product:explain",
                ]
            )
        self.assertEqual(0, code)
        document = __import__("json").loads(output.getvalue())
        self.assertEqual("fresh", document["current"]["state"])
        self.assertIn("recommended_policy", document)

    def test_native_cli_reports_version(self):
        output = io.StringIO()
        with redirect_stdout(output), self.assertRaises(SystemExit) as exit_status:
            run(["--version"])
        self.assertEqual(0, exit_status.exception.code)
        self.assertEqual("MegaCache 1.0.0", output.getvalue().strip())

    def test_native_cli_fetches_only_from_a_named_origin(self):
        output = io.StringIO()
        with redirect_stdout(output):
            code = run(
                [
                    "--port",
                    str(self.port),
                    "--password",
                    "secret",
                    "--json",
                    "fetch",
                    "product:1",
                    "catalog",
                    "/v1/product/1",
                ]
            )
        self.assertEqual(0, code)
        document = __import__("json").loads(output.getvalue())
        self.assertEqual("refreshed", document["state"])
        self.assertEqual("/v1/product/1", document["value"])
        with MegaCacheClient("127.0.0.1", self.port, "secret") as client:
            self.assertEqual(
                b"/v1/product/1", client.command("GET", "product:1")
            )
            self.assertEqual(
                [b"/v1/product/1"],
                client.command("MGET", "product:1"),
            )

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

    def test_inline_fetch_traceparent_is_command_scoped(self):
        traceparent = (
            "00-4bf92f3577b34da6a3ce929d0e0e4736-"
            "00f067aa0ba902b7-01"
        )
        with MegaCacheClient("127.0.0.1", self.port, "secret") as client:
            client.fetch(
                "first", "catalog", "/v1/first",
                refresh=True, traceparent=traceparent,
            )
            client.fetch("second", "catalog", "/v1/second", refresh=True)
        self.assertEqual([traceparent, None], self.traces)


class SDKRegressionTests(unittest.TestCase):
    def client(self):
        client = MegaCacheClient(invalidation_poll_seconds=60)
        client._last_poll = time.monotonic()
        return client

    def test_zero_freshness_is_not_admitted_but_absent_windows_fallback(self):
        client = self.client()
        client.lease = lambda key: LeaseResult(
            "fresh", value=b"zero", expires_in_seconds=0,
            stale_for_seconds=0,
        )
        self.assertEqual(
            b"zero",
            client.get_or_load("zero", lambda: b"unused").value,
        )
        self.assertIsNone(client.local.get("zero"))

        client.lease = lambda key: LeaseResult("fresh", value=b"legacy")
        client.get_or_load("legacy", lambda: b"unused")
        self.assertEqual("fresh", client.local.get("legacy").state)

    def test_lease_elapsed_time_is_removed_from_l1_deadline(self):
        clock = [100.0]
        client = self.client()

        def lease(key):
            clock[0] += 3
            return LeaseResult(
                "fresh", value=b"value", expires_in_seconds=5,
                stale_for_seconds=5,
            )

        client.lease = lease
        with patch("megacache.client.time.monotonic", side_effect=lambda: clock[0]):
            client._last_poll = clock[0]
            client.get_or_load("key", lambda: b"unused")
            clock[0] = 109.9
            self.assertIsNotNone(client.local.get("key"))
            clock[0] = 110.0
            self.assertIsNone(client.local.get("key"))

    def test_stale_if_error_rechecks_monotonic_hard_deadline(self):
        clock = [100.0]
        client = self.client()

        def lease(key):
            return LeaseResult(
                "stale_lease", value=b"stale", lease_token="token",
                stale_for_seconds=1,
            )

        def loader():
            clock[0] += 2
            raise RuntimeError("load failed")

        client.lease = lease
        with patch("megacache.client.time.monotonic", side_effect=lambda: clock[0]):
            client._last_poll = clock[0]
            with self.assertRaisesRegex(RuntimeError, "load failed"):
                client.get_or_load("key", loader)

    def test_initial_connect_failures_are_connection_errors(self):
        client = self.client()
        failure = OSError("refused")
        with patch(
            "megacache.client.socket.create_connection",
            side_effect=failure,
        ):
            with self.assertRaises(MegaCacheConnectionError) as captured:
                client.connect()
        self.assertIs(failure, captured.exception.__cause__)

    def test_concurrent_connects_share_one_socket(self):
        client = self.client()
        raw_socket = Mock()
        raw_socket.makefile.return_value = Mock()
        entered = threading.Event()
        release = threading.Event()
        failures = []

        def create_connection(*args, **kwargs):
            entered.set()
            release.wait(1)
            return raw_socket

        with patch(
            "megacache.client.socket.create_connection",
            side_effect=create_connection,
        ) as mocked_connect:
            threads = [
                threading.Thread(
                    target=lambda: self._connect_and_capture(client, failures)
                )
                for _ in range(2)
            ]
            threads[0].start()
            self.assertTrue(entered.wait(1))
            threads[1].start()
            time.sleep(0.02)
            self.assertEqual(1, mocked_connect.call_count)
            release.set()
            for thread in threads:
                thread.join(1)
                self.assertFalse(thread.is_alive())
        self.assertEqual([], failures)
        self.assertEqual(1, mocked_connect.call_count)
        client.close()

    @staticmethod
    def _connect_and_capture(client, failures):
        try:
            client.connect()
        except Exception as error:
            failures.append(error)

    def test_tls_handshake_failures_are_connection_errors(self):
        raw_socket = Mock()
        context = Mock()
        failure = ssl.SSLError("handshake failed")
        context.wrap_socket.side_effect = failure
        client = MegaCacheClient(tls=True, invalidation_poll_seconds=60)
        with patch(
            "megacache.client.socket.create_connection",
            return_value=raw_socket,
        ), patch(
            "megacache.client.ssl.create_default_context",
            return_value=context,
        ):
            with self.assertRaises(MegaCacheConnectionError) as captured:
                client.connect()
        self.assertIs(failure, captured.exception.__cause__)
        raw_socket.close.assert_called_once()

    def test_auth_command_errors_keep_their_type(self):
        raw_socket = Mock()
        raw_socket.makefile.return_value = Mock()
        client = MegaCacheClient(
            password="secret", invalidation_poll_seconds=60
        )
        failure = MegaCacheCommandError("WRONGPASS invalid credentials")
        with patch(
            "megacache.client.socket.create_connection",
            return_value=raw_socket,
        ), patch.object(client, "command", side_effect=failure):
            with self.assertRaises(MegaCacheCommandError) as captured:
                client.connect()
        self.assertIs(failure, captured.exception)

    def test_auth_protocol_errors_keep_their_type(self):
        raw_socket = Mock()
        raw_socket.makefile.return_value = Mock()
        client = MegaCacheClient(
            password="secret", invalidation_poll_seconds=60
        )
        failure = MegaCacheProtocolError("invalid AUTH response")
        with patch(
            "megacache.client.socket.create_connection",
            return_value=raw_socket,
        ), patch.object(client, "command", side_effect=failure):
            with self.assertRaises(MegaCacheProtocolError) as captured:
                client.connect()
        self.assertIs(failure, captured.exception)

    def test_all_command_decoders_raise_protocol_errors(self):
        cases = [
            ("ping type", 1, lambda client: client.ping()),
            ("get type", "value", lambda client: client.get("key")),
            ("set shape", b"OK", lambda client: client.set("key", b"value")),
            (
                "mget arity",
                [b"value"],
                lambda client: client.mget(("first", "second")),
            ),
            ("mget element", [1], lambda client: client.mget(("key",))),
            (
                "mset shape",
                b"OK",
                lambda client: client.mset((("key", b"value"),)),
            ),
            ("delete range", -1, lambda client: client.delete("key")),
            ("exists range", 2, lambda client: client.exists("key")),
            ("expire range", 2, lambda client: client.expire("key", 30)),
            ("ttl range", -3, lambda client: client.ttl("key")),
            (
                "put shape",
                1,
                lambda client: client.put("key", b"value"),
            ),
            (
                "lease arity",
                [b"fresh"],
                lambda client: client.lease("key"),
            ),
            (
                "lease element",
                [b"fresh", b"value", b"bad", 1],
                lambda client: client.lease("key"),
            ),
            (
                "lease retry",
                [b"loading", -1],
                lambda client: client.lease("key"),
            ),
            (
                "fetch JSON",
                b"{",
                lambda client: client.fetch("key", "origin", "/path"),
            ),
            (
                "fetch semantics",
                b'{"state":1,"origin":"origin"}',
                lambda client: client.fetch("key", "origin", "/path"),
            ),
            (
                "invalidate range",
                -1,
                lambda client: client.invalidate("tag"),
            ),
            (
                "status shape",
                b'{"degraded":"false"}',
                lambda client: client.status(),
            ),
            (
                "invalidations semantics",
                b'{"cursor":"x","epoch":"e","generation":"bad"}',
                lambda client: client.invalidations(),
            ),
            (
                "traceparent shape",
                b"OK",
                lambda client: client.set_traceparent("trace"),
            ),
        ]
        for name, response, operation in cases:
            with self.subTest(name=name):
                client = self.client()
                client.command = Mock(return_value=response)
                with self.assertRaises(MegaCacheProtocolError):
                    operation(client)

    def test_inflight_admission_cannot_overwrite_successful_mutation(self):
        client = self.client()
        entered = threading.Event()
        release = threading.Event()

        def lease(key):
            entered.set()
            release.wait(1)
            return LeaseResult(
                "fresh", value=b"old", expires_in_seconds=30,
                stale_for_seconds=30,
            )

        client.lease = lease
        result = []
        thread = threading.Thread(
            target=lambda: result.append(
                client.get_or_load("key", lambda: b"unused").value
            )
        )
        thread.start()
        self.assertTrue(entered.wait(1))
        client._mutation_succeeded()
        release.set()
        thread.join(1)
        self.assertEqual([b"old"], result)
        self.assertIsNone(client.local.get("key"))

    def test_failed_mutation_does_not_clear_or_advance_generation(self):
        client = self.client()
        client.local.put("key", b"value", 30, 30)
        generation = client._mutation_generation

        def fail(*parts):
            raise MegaCacheCommandError("ERR rejected")

        client.command = fail
        with self.assertRaises(MegaCacheCommandError):
            client.set("other", b"value")
        self.assertEqual(generation, client._mutation_generation)
        self.assertEqual(b"value", client.local.get("key").value)

    def test_flight_failure_is_shared_only_with_current_waiters(self):
        client = self.client()
        client.lease = lambda key: LeaseResult("lease", lease_token="token")
        entered = threading.Event()
        release = threading.Event()
        errors = []

        def loader():
            entered.set()
            release.wait(1)
            raise RuntimeError("load failed")

        def run():
            try:
                client.get_or_load("key", loader)
            except RuntimeError as error:
                errors.append(error)

        threads = [threading.Thread(target=run) for _ in range(3)]
        for thread in threads:
            thread.start()
        self.assertTrue(entered.wait(1))
        time.sleep(0.02)
        release.set()
        for thread in threads:
            thread.join(1)
        self.assertEqual(3, len(errors))
        self.assertTrue(all(str(error) == "load failed" for error in errors))
        self.assertEqual({}, client._flights)

        client.lease = lambda key: LeaseResult("fresh", value=b"recovered")
        self.assertEqual(
            b"recovered",
            client.get_or_load("key", lambda: b"unused").value,
        )

    def test_protocol_limit_closes_poisoned_connection(self):
        class Socket:
            closed = False

            def close(self):
                self.closed = True

        class Stream:
            def __init__(self, response):
                self.reader = io.BytesIO(response)
                self.closed = False

            def write(self, value):
                return len(value)

            def flush(self):
                pass

            def read(self, size=-1):
                return self.reader.read(size)

            def readline(self, size=-1):
                return self.reader.readline(size)

            def close(self):
                self.closed = True
                self.reader.close()

        responses = (
            b"$16777217\r\n",
            b"*100001\r\n",
            b"*1\r\n" * 130 + b"$1\r\nx\r\n",
        )
        for response in responses:
            client = self.client()
            stream = Stream(response)
            raw_socket = Socket()
            client._stream = stream
            client._socket = raw_socket
            with self.assertRaises(MegaCacheProtocolError):
                client.command("PING")
            self.assertTrue(stream.closed)
            self.assertTrue(raw_socket.closed)
            self.assertIsNone(client._socket)

    def test_traceparent_scope_restores_previous_request(self):
        client = self.client()
        client._active_traceparent = "connection"
        with client.traceparent_scope("request"):
            self.assertEqual("request", client._current_traceparent())
        self.assertEqual("connection", client._current_traceparent())

    def test_wsgi_traceparent_covers_lazy_iteration_without_leaking(self):
        client = self.client()
        seen = []

        def application(environ, start_response):
            def body():
                seen.append(client._current_traceparent())
                yield b"ok"

            return body()

        wrapped = wsgi_traceparent(client, application)
        self.assertEqual(
            [b"ok"],
            list(wrapped({"HTTP_TRACEPARENT": "request"}, lambda *args: None)),
        )
        self.assertEqual(["request"], seen)
        self.assertIsNone(client._current_traceparent())


if __name__ == "__main__":
    unittest.main()
