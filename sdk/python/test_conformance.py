import os
import threading
import unittest

from megacache.client import CachePolicy, MegaCacheClient


def fixtures():
    values = {}
    with open(os.environ["MEGACACHE_FIXTURES"], encoding="utf-8") as stream:
        for line in stream:
            if line.startswith("#") or not line.strip():
                continue
            key, value = line.rstrip("\n").split("\t", 1)
            values[key] = value
    return values


class PythonSDKConformance(unittest.TestCase):
    def setUp(self):
        self.values = fixtures()
        self.client = MegaCacheClient(
            host=os.environ["MEGACACHE_CONFORMANCE_HOST"],
            port=int(os.environ["MEGACACHE_CONFORMANCE_PORT"]),
            invalidation_poll_seconds=0,
        )

    def tearDown(self):
        self.client.close()

    def test_protocol_and_megacache_operations(self):
        v = self.values
        self.assertEqual("PONG", self.client.ping())
        self.assertEqual("OK", self.client.set(v["key"], v["value"]))
        self.assertEqual(v["value"].encode(), self.client.get(v["key"]))
        self.assertEqual(
            "OK", self.client.mset(((v["second_key"], v["second_value"]),))
        )
        self.assertEqual(
            [v["value"].encode(), v["second_value"].encode()],
            self.client.mget((v["key"], v["second_key"])),
        )
        self.assertEqual(2, self.client.exists(v["key"], v["second_key"]))
        self.assertTrue(self.client.expire(v["key"], 30))
        self.assertGreaterEqual(self.client.ttl(v["key"]), 0)
        self.assertEqual(1, self.client.delete(v["second_key"]))
        self.client.put(
            v["key"], v["value"], ttl_seconds=30,
            stale_seconds=30, tags=(v["tag"],),
        )
        lease = self.client.lease(v["key"])
        self.assertEqual("fresh", lease.state)
        self.assertGreater(lease.expires_in_seconds, 0)
        self.assertGreater(lease.stale_for_seconds, 0)
        self.assertEqual(1, self.client.invalidate(v["tag"]))
        self.assertIsNone(self.client.get(v["key"]))
        self.client.set_traceparent(v["traceparent"])
        fetched = self.client.fetch(v["fetch_key"], v["origin"], v["path"])
        self.assertEqual(v["path"], fetched.value)
        self.assertFalse(self.client.status()["degraded"])

    def test_l1_request_coalescing(self):
        self.client.delete(self.values["l1_key"])
        calls = []
        barrier = threading.Barrier(2)

        def loader():
            calls.append(1)
            return b"loaded"

        results = []

        def run():
            barrier.wait()
            results.append(
                self.client.get_or_load(
                    self.values["l1_key"],
                    loader,
                    CachePolicy(ttl_seconds=30, stale_seconds=30),
                ).value
            )

        threads = [threading.Thread(target=run) for _ in range(2)]
        for thread in threads:
            thread.start()
        for thread in threads:
            thread.join()
        self.assertEqual([b"loaded", b"loaded"], sorted(results))
        self.assertEqual(1, len(calls))
        with MegaCacheClient(
            host=os.environ["MEGACACHE_CONFORMANCE_HOST"],
            port=int(os.environ["MEGACACHE_CONFORMANCE_PORT"]),
        ) as writer:
            writer.set(self.values["l1_key"], b"external")
        refreshed = self.client.get_or_load(
            self.values["l1_key"],
            lambda: self.fail("fresh L2 value should avoid the loader"),
            CachePolicy(ttl_seconds=30, stale_seconds=30),
        )
        self.assertEqual(b"external", refreshed.value)
