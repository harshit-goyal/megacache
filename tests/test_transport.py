import socket
import threading
import unittest
from dataclasses import replace

from megacache.config import Config
from megacache.resp import MegaCacheRespServer
from megacache.server import MegaCacheServer
from megacache.transport import validate_tls_config


class TransportTests(unittest.TestCase):
    def setUp(self):
        self.config = Config(
            host="127.0.0.1",
            port=8080,
            resp_host="127.0.0.1",
            resp_port=6380,
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
            users_file=None,
            log_format="text",
        )

    def test_tls_certificate_and_key_must_be_configured_together(self):
        with self.assertRaisesRegex(ValueError, "configured together"):
            validate_tls_config(
                replace(self.config, tls_cert_file="/tmp/cert.pem")
            )

    def test_tls_can_be_disabled(self):
        validate_tls_config(self.config)

    def test_protocol_servers_drain_non_daemon_handlers(self):
        self.assertFalse(MegaCacheServer.daemon_threads)
        self.assertFalse(MegaCacheRespServer.daemon_threads)

    def test_draining_closes_idle_connections_after_grace_period(self):
        from megacache.engine import CacheEngine

        server = MegaCacheRespServer(
            ("127.0.0.1", 0), self.config, CacheEngine()
        )
        thread = threading.Thread(target=server.serve_forever, daemon=True)
        thread.start()
        connection = socket.create_connection(server.server_address, timeout=1)
        try:
            connection.sendall(b"*1\r\n$4\r\nPING\r\n")
            self.assertEqual(b"+PONG\r\n", connection.recv(64))
            server.start_draining()
            server.shutdown()
            server.drain_connections(0.01)
            server.server_close()
            thread.join(timeout=1)
            self.assertEqual(b"", connection.recv(1))
        finally:
            connection.close()


if __name__ == "__main__":
    unittest.main()
