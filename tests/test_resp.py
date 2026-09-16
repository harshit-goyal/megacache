import socket
import threading
import unittest

from megacache.config import Config
from megacache.engine import CacheEngine
from megacache.resp import MegaCacheRespServer


class RespClient:
    def __init__(self, port):
        self.socket = socket.create_connection(("127.0.0.1", port), timeout=2)
        self.stream = self.socket.makefile("rwb")

    def close(self):
        self.stream.close()
        self.socket.close()

    def command(self, *parts):
        encoded = [
            part if isinstance(part, bytes) else str(part).encode("utf-8")
            for part in parts
        ]
        payload = b"*" + str(len(encoded)).encode("ascii") + b"\r\n"
        for part in encoded:
            payload += (
                b"$"
                + str(len(part)).encode("ascii")
                + b"\r\n"
                + part
                + b"\r\n"
            )
        self.stream.write(payload)
        self.stream.flush()
        return self._read()

    def _read(self):
        marker = self.stream.read(1)
        line = self.stream.readline()
        if marker == b"+":
            return line[:-2].decode("utf-8")
        if marker == b"-":
            return RuntimeError(line[:-2].decode("utf-8"))
        if marker == b":":
            return int(line)
        if marker == b"$":
            length = int(line)
            if length == -1:
                return None
            value = self.stream.read(length)
            self.assert_crlf()
            return value
        if marker == b"*":
            return [self._read() for _ in range(int(line))]
        raise AssertionError("unknown RESP marker {!r}".format(marker))

    def assert_crlf(self):
        if self.stream.read(2) != b"\r\n":
            raise AssertionError("missing CRLF")


class RespServerTests(unittest.TestCase):
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
        self.client = RespClient(self.port)

    def tearDown(self):
        self.client.close()

    def authenticate(self):
        self.assertEqual("OK", self.client.command("AUTH", "secret"))

    def test_requires_authentication(self):
        error = self.client.command("GET", "key")
        self.assertIsInstance(error, RuntimeError)
        self.assertIn("NOAUTH", str(error))

    def test_ping_set_get_expire_ttl_and_delete(self):
        self.authenticate()
        self.assertEqual("PONG", self.client.command("PING"))
        self.assertEqual("OK", self.client.command("SET", "key", b"\x00value"))
        self.assertEqual(b"\x00value", self.client.command("GET", "key"))
        self.assertEqual(-1, self.client.command("TTL", "key"))
        self.assertEqual(1, self.client.command("EXPIRE", "key", 30))
        self.assertGreaterEqual(self.client.command("TTL", "key"), 29)
        self.assertEqual(1, self.client.command("DEL", "key"))
        self.assertIsNone(self.client.command("GET", "key"))

    def test_mset_mget_exists_and_dbsize(self):
        self.authenticate()
        self.assertEqual("OK", self.client.command("MSET", "a", "1", "b", "2"))
        self.assertEqual([b"1", b"2", None], self.client.command("MGET", "a", "b", "c"))
        self.assertEqual(2, self.client.command("EXISTS", "a", "b", "c"))
        self.assertEqual(2, self.client.command("DBSIZE"))

    def test_megacache_tags_and_refresh_lease(self):
        self.authenticate()
        self.assertEqual(
            "OK",
            self.client.command(
                "MC.SET", "product:1", "value", "TTL", 60, "STALE", 120,
                "TAGS", 2, "products", "tenant:1"
            ),
        )
        self.assertEqual(1, self.client.command("MC.INVALIDATE", "products"))
        lease = self.client.command("MC.LEASE", "product:1")
        self.assertEqual(b"lease", lease[0])
        self.assertEqual(2, len(lease))
        self.assertEqual(
            "OK",
            self.client.command(
                "MC.SET", "product:1", "refreshed", "LEASE", lease[1]
            ),
        )
        self.assertEqual(b"refreshed", self.client.command("GET", "product:1"))

    def test_select_and_hello_resp2(self):
        self.authenticate()
        self.assertEqual("OK", self.client.command("SELECT", 0))
        hello = self.client.command("HELLO", 2)
        self.assertEqual(b"server", hello[0])
        error = self.client.command("HELLO", 3)
        self.assertIsInstance(error, RuntimeError)


if __name__ == "__main__":
    unittest.main()
