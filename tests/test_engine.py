import threading
import time
import unittest

from megacache import CacheEngine


class FakeClock:
    def __init__(self):
        self.value = 100.0

    def __call__(self):
        return self.value

    def advance(self, seconds):
        self.value += seconds


class CacheEngineTests(unittest.TestCase):
    def setUp(self):
        self.clock = FakeClock()
        self.cache = CacheEngine(
            max_entries=2,
            default_ttl_seconds=10,
            default_stale_seconds=20,
            lease_seconds=5,
            clock=self.clock,
        )

    def test_fresh_stale_and_expired_lifecycle(self):
        self.cache.put("key", {"answer": 42})
        self.assertEqual("fresh", self.cache.get("key").state)
        self.clock.advance(11)
        self.assertEqual("stale", self.cache.get("key").state)
        self.clock.advance(20)
        self.assertEqual("miss", self.cache.get("key").state)

    def test_tag_invalidation_removes_related_entries(self):
        self.cache.put("p:1", 1, tags=["product", "tenant:1"])
        self.cache.put("p:2", 2, tags=["product", "tenant:1"])
        self.assertEqual(2, self.cache.invalidate_tags(["product"]))
        self.assertEqual("miss", self.cache.get("p:1").state)

    def test_lru_evicts_least_recently_used(self):
        self.cache.put("a", 1)
        self.cache.put("b", 2)
        self.cache.get("a")
        self.cache.put("c", 3)
        self.assertEqual("fresh", self.cache.get("a").state)
        self.assertEqual("miss", self.cache.get("b").state)

    def test_lease_coalesces_misses_and_guards_write(self):
        leader = self.cache.acquire_lease("key")
        follower = self.cache.acquire_lease("key")
        self.assertEqual("lease", leader.state)
        self.assertEqual("loading", follower.state)
        with self.assertRaisesRegex(ValueError, "does not match"):
            self.cache.put("key", "bad", lease_token="wrong")
        self.cache.put("key", "value", lease_token=leader.lease_token)
        self.assertEqual("value", self.cache.get("key").value)
        self.assertEqual(0, self.cache.stats()["lease_memory_bytes"])

    def test_stale_value_can_be_refreshed_under_lease(self):
        self.cache.put("key", "old")
        self.clock.advance(11)
        leader = self.cache.acquire_lease("key")
        follower = self.cache.acquire_lease("key")
        self.assertEqual("stale_lease", leader.state)
        self.assertEqual("old", leader.value)
        self.assertIsNotNone(leader.lease_token)
        self.assertEqual("stale", follower.state)
        self.cache.put("key", "new", lease_token=leader.lease_token)
        self.assertEqual("new", self.cache.get("key").value)

    def test_singleflight_calls_loader_once(self):
        started = threading.Event()
        release = threading.Event()
        calls = []
        results = []

        def loader():
            calls.append(1)
            started.set()
            release.wait(timeout=1)
            return "loaded"

        def run():
            results.append(self.cache.get_or_load("key", loader).value)

        first = threading.Thread(target=run)
        second = threading.Thread(target=run)
        first.start()
        started.wait(timeout=1)
        second.start()
        time.sleep(0.02)
        release.set()
        first.join(timeout=1)
        second.join(timeout=1)
        self.assertEqual(1, len(calls))
        self.assertEqual(["loaded", "loaded"], sorted(results))

    def test_invalid_inputs_are_rejected(self):
        with self.assertRaises(ValueError):
            self.cache.put("", "value")
        with self.assertRaises(ValueError):
            self.cache.put("key", "value", ttl_seconds=0)
        with self.assertRaises(ValueError):
            self.cache.put("key", "value", tags="not-an-array")

    def test_byte_limit_evicts_lru_entry(self):
        cache = CacheEngine(
            max_entries=10,
            max_memory_bytes=300,
            max_entry_bytes=250,
        )
        cache.put("a", "a" * 100)
        cache.put("b", "b" * 100)
        self.assertEqual("miss", cache.get("a").state)
        self.assertEqual("fresh", cache.get("b").state)
        self.assertLessEqual(cache.stats()["memory_bytes"], 300)

    def test_oversized_entry_is_rejected_without_eviction(self):
        cache = CacheEngine(
            max_entries=10,
            max_memory_bytes=1_000,
            max_entry_bytes=200,
        )
        cache.put("existing", "ok")
        with self.assertRaisesRegex(ValueError, "maximum"):
            cache.put("large", "x" * 200)
        self.assertEqual("fresh", cache.get("existing").state)
        self.assertEqual(1, cache.stats()["rejected_entries_total"])

    def test_mutable_values_are_snapshotted_and_defensively_copied(self):
        original = {"items": []}
        self.cache.put("key", original)
        original["items"].append("outside")
        cached = self.cache.get("key").value
        self.assertEqual({"items": []}, cached)
        cached["items"].append("returned")
        self.assertEqual({"items": []}, self.cache.get("key").value)

    def test_leases_are_bounded_and_accounted(self):
        cache = CacheEngine(
            max_entries=1,
            max_memory_bytes=1_000,
            max_entry_bytes=500,
        )
        cache.acquire_lease("first")
        self.assertGreater(cache.stats()["lease_memory_bytes"], 0)
        with self.assertRaisesRegex(ValueError, "lease capacity"):
            cache.acquire_lease("second")
        self.assertEqual(1, cache.stats()["rejected_leases_total"])

    def test_expired_leases_release_memory_capacity(self):
        clock = FakeClock()
        cache = CacheEngine(
            max_entries=1,
            default_ttl_seconds=10,
            default_stale_seconds=10,
            lease_seconds=5,
            clock=clock,
            max_memory_bytes=1_000,
            max_entry_bytes=500,
        )
        cache.acquire_lease("first")
        clock.advance(6)
        cache.acquire_lease("second")
        self.assertEqual(1, cache.stats()["active_leases"])

    def test_original_positional_constructor_order_is_preserved(self):
        clock = FakeClock()
        cache = CacheEngine(100, 60, 120, 5, clock)
        cache.put("key", "value")
        clock.advance(61)
        self.assertEqual("stale", cache.get("key").state)


if __name__ == "__main__":
    unittest.main()
