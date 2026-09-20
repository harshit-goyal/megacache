import os
import unittest
from unittest import mock

from megacache import (
    CacheEngine,
    CacheIntelligence,
    ClusterNode,
    ClusterStorage,
    QuorumError,
)
from megacache import HTTPOrigin, OriginCache, OriginResponse


class FakeClock:
    def __init__(self):
        self.value = 100.0

    def __call__(self):
        return self.value

    def advance(self, seconds):
        self.value += seconds


class IntelligenceTests(unittest.TestCase):
    def test_default_lru_is_preserved_and_cost_policy_is_opt_in(self):
        lru = CacheEngine(max_entries=2)
        lru.put("a", "a")
        lru.put("b", "b")
        lru.get("a")
        lru.put("c", "c")
        self.assertEqual("miss", lru.get("b").state)

        cost = CacheEngine(max_entries=2, eviction_policy="cost")
        cost.put("expensive", "x")
        cost.put("cheap", "x")
        cost.record_load_cost("expensive", 2_000)
        cost.get("expensive")
        cost.put("new", "x")
        self.assertEqual("fresh", cost.get("expensive").state)
        self.assertEqual("miss", cost.get("cheap").state)
        self.assertEqual(1, cost.stats()["cost_aware_eviction_enabled"])

    def test_telemetry_is_bounded_and_adaptive_ttl_never_exceeds_base(self):
        cache = CacheIntelligence(
            CacheEngine(),
            enabled=True,
            adaptive_ttl=True,
            telemetry_max_keys=2,
            telemetry_max_classes=2,
            min_ttl_seconds=5,
            max_ttl_seconds=1_000,
        )
        cache.record_origin_load("a:1", "catalog", 0.1, b"one")
        cache.record_origin_load("a:1", "catalog", 0.2, b"two")
        self.assertEqual(25, cache.adaptive_ttl("a:1", "catalog", 100))
        cache.get("b:1")
        cache.get("c:1")
        self.assertEqual(2, cache.stats()["intelligence_tracked_keys"])
        self.assertGreaterEqual(
            cache.stats()["intelligence_telemetry_evictions_total"], 1
        )

    def test_origin_refresh_uses_adaptive_ttl_and_records_load_cost(self):
        clock = FakeClock()
        engine = CacheEngine(clock=clock)
        intelligence = CacheIntelligence(
            engine,
            enabled=True,
            adaptive_ttl=True,
            min_ttl_seconds=5,
            max_ttl_seconds=1_000,
            clock=clock,
        )
        bodies = iter((b"one", b"two", b"three"))
        service = OriginCache(
            intelligence,
            [
                HTTPOrigin.from_dict(
                    {
                        "name": "catalog",
                        "base_url": "https://origin.example",
                        "allowed_hosts": ["origin.example"],
                        "allowed_ports": [443],
                        "allowed_path_prefixes": ["/v1/"],
                        "ttl_seconds": 100,
                        "retry_attempts": 0,
                    }
                )
            ],
            clock=clock,
            resolver=lambda host, port: ["93.184.216.34"],
            transport=lambda origin, path, addresses: OriginResponse(
                200, next(bodies)
            ),
        )
        try:
            for _ in range(3):
                service.fetch(
                    "catalog:1", "catalog", "/v1/items/1", force_refresh=True
                )
            current = engine.explain_entry("catalog:1")
            self.assertEqual(25, current["configured_ttl_seconds"])
            self.assertGreaterEqual(current["load_cost_ms"], 0)
            evidence = intelligence.explain("catalog:1")["evidence"]
            self.assertEqual(3, evidence["loads"])
            self.assertEqual(2, evidence["changes"])
            self.assertEqual(0, evidence["mutations"])
        finally:
            service.close(1)

    def test_live_and_simulated_adaptive_ttl_use_identical_reuse_formula(self):
        cache = CacheIntelligence(
            CacheEngine(),
            enabled=True,
            adaptive_ttl=True,
            min_ttl_seconds=5,
            max_ttl_seconds=80,
        )
        for _ in range(4):
            cache.record_origin_load("catalog:1", "catalog", 0.01, b"same")
        cache.get("catalog:1")
        cache.get("catalog:1")
        for _ in range(4):
            cache.record_origin_load("catalog:2", "catalog", 0.01, b"same")
        for _ in range(20):
            cache.get("catalog:2")
        live = cache.adaptive_ttl("catalog:1", "catalog", 100)
        clamped_live = cache.adaptive_ttl("catalog:2", "catalog", 100)
        simulated = cache.simulate(
            {
                "policy": {
                    "min_ttl_seconds": 5,
                    "max_ttl_seconds": 80,
                },
                "records": [
                    {
                        "key": "catalog:1",
                        "base_ttl_seconds": 100,
                        "loads": 4,
                        "changes": 0,
                        "accesses": 2,
                    },
                    {
                        "key": "catalog:2",
                        "base_ttl_seconds": 100,
                        "loads": 4,
                        "changes": 0,
                        "accesses": 20,
                    },
                ],
            }
        )["records"]
        self.assertEqual(50, live)
        self.assertEqual(live, simulated[0]["simulated_ttl_seconds"])
        self.assertEqual(80, clamped_live)
        self.assertEqual(clamped_live, simulated[1]["simulated_ttl_seconds"])

    def test_explain_has_evidence_policy_and_origin_lineage_without_value(self):
        engine = CacheEngine()
        cache = CacheIntelligence(engine, enabled=True)
        engine.put(
            "catalog:1",
            {
                "$megacache_origin_value_v1": True,
                "origin": "catalog",
                "path": "/v1/items/1",
                "status": 200,
                "body": "c2VjcmV0",
            },
            ttl_seconds=60,
        )
        cache.get("catalog:1")
        explanation = cache.explain("catalog:1")
        self.assertEqual("fresh", explanation["current"]["state"])
        self.assertEqual(
            "catalog", explanation["current"]["lineage"]["origin"]
        )
        self.assertNotIn("value", explanation["current"])
        self.assertIn("recommended_policy", explanation)
        self.assertIn("reasons", explanation)

    def test_hot_keys_receive_bounded_in_process_extra_replica(self):
        clock = FakeClock()
        nodes = [
            ClusterNode(name, CacheEngine(clock=clock))
            for name in ("a", "b", "c")
        ]
        cluster = ClusterStorage(
            nodes,
            replica_count=1,
            consistency="one",
            clock=clock,
        )
        cache = CacheIntelligence(
            cluster,
            enabled=True,
            hot_key_threshold=2,
            hot_key_window_seconds=60,
            hot_key_extra_replicas=1,
            clock=clock,
        )
        cache.put("hot:key", "value")
        cache.get("hot:key")
        cache.get("hot:key")
        ownership = cluster.ownership("hot:key")
        self.assertEqual(1, len(ownership["hot_replicas"]))
        self.assertEqual("in-process", cluster.explain_entry("hot:key")["distribution_scope"])

    def test_hot_copy_never_substitutes_for_configured_quorum(self):
        clock = FakeClock()
        cluster = ClusterStorage(
            [
                ClusterNode(name, CacheEngine(clock=clock))
                for name in ("a", "b", "c")
            ],
            replica_count=2,
            consistency="all",
            clock=clock,
        )
        cluster.put("hot:key", "value")
        self.assertEqual(1, cluster.replicate_hot_key("hot:key", 1))
        cluster.mark_node_down(cluster.ownership("hot:key")["replicas"][0])
        with self.assertRaises(QuorumError):
            cluster.get("hot:key")

    def test_simulation_is_dry_run_and_respects_freshness_ceiling(self):
        cache = CacheIntelligence(CacheEngine(), enabled=True)
        result = cache.simulate(
            {
                "policy": {
                    "min_ttl_seconds": 5,
                    "max_ttl_seconds": 600,
                    "eviction_policy": "cost",
                    "capacity_entries": 1,
                },
                "records": [
                    {
                        "key": "a",
                        "base_ttl_seconds": 100,
                        "accesses": 100,
                        "loads": 3,
                        "changes": 2,
                        "size_bytes": 100,
                        "load_latency_ms": 20,
                    },
                    {
                        "key": "b",
                        "base_ttl_seconds": 100,
                        "accesses": 1,
                        "loads": 1,
                        "changes": 0,
                        "size_bytes": 1000,
                    }
                ],
            }
        )
        self.assertTrue(result["dry_run"])
        self.assertFalse(result["activated"])
        self.assertEqual(
            0,
            result["summary"]["configured_freshness_bound_violations"],
        )
        self.assertLessEqual(
            result["records"][0]["simulated_ttl_seconds"], 100
        )
        self.assertEqual("cost", result["eviction"]["policy"])
        self.assertEqual(1, len(result["eviction"]["proposed_evicted"]))

    def test_experiment_rolls_back_and_persists_auditable_decision(self):
        path = os.path.join(
            os.getcwd(), ".test-intelligence-state-{}.json".format(os.getpid())
        )
        try:
            cache = CacheIntelligence(
                CacheEngine(),
                enabled=True,
                adaptive_ttl=True,
                experiment_enabled=True,
                experiment_id="guardrail-test",
                experiment_allocation_percent=50,
                experiment_min_samples=2,
                experiment_max_miss_regression=0,
                state_file=path,
            )
            candidate = []
            control = []
            for index in range(1000):
                key = "key:{}".format(index)
                (candidate if cache._arm(key) == "candidate" else control).append(key)
                if len(candidate) >= 2 and len(control) >= 2:
                    break
            for key in control[:2]:
                cache.put(key, "value")
                cache.get(key)
            for key in candidate[:2]:
                cache.get(key)
            status = cache.experiment_status()
            self.assertEqual("rolled_back", status["status"])
            self.assertEqual("automatic_rollback", status["audit"][-1]["decision"])
            restored = CacheIntelligence(
                CacheEngine(),
                enabled=True,
                adaptive_ttl=True,
                experiment_enabled=True,
                experiment_id="guardrail-test",
                experiment_allocation_percent=50,
                experiment_min_samples=2,
                experiment_max_miss_regression=0,
                state_file=path,
            )
            self.assertEqual(
                "rolled_back", restored.experiment_status()["status"]
            )
        finally:
            try:
                os.unlink(path)
            except OSError:
                pass
            try:
                os.unlink(path + ".next")
            except OSError:
                pass

    def test_rollback_persistence_failure_is_fail_safe_and_non_fatal(self):
        path = os.path.join(
            os.getcwd(), ".test-intelligence-failure-{}.json".format(os.getpid())
        )
        cache = CacheIntelligence(
            CacheEngine(),
            enabled=True,
            adaptive_ttl=True,
            experiment_enabled=True,
            experiment_id="guardrail-write-failure",
            experiment_allocation_percent=50,
            experiment_min_samples=1,
            experiment_max_miss_regression=0,
            state_file=path,
        )
        candidate = next(
            "candidate:{}".format(index)
            for index in range(1000)
            if cache._arm("candidate:{}".format(index)) == "candidate"
        )
        control = next(
            "control:{}".format(index)
            for index in range(1000)
            if cache._arm("control:{}".format(index)) == "control"
        )
        cache.put(control, "value")
        cache.get(control)
        with mock.patch(
            "megacache.intelligence.os.replace",
            side_effect=OSError("disk unavailable"),
        ):
            cache.get(candidate)

        status = cache.experiment_status()
        self.assertEqual("rolled_back", status["status"])
        self.assertIsNotNone(status["persistence_error"])
        self.assertEqual(
            1, cache.stats()["intelligence_state_write_errors_total"]
        )
        self.assertEqual(100, cache.adaptive_ttl(candidate, "default", 100))
        self.assertFalse(os.path.exists(path))


if __name__ == "__main__":
    unittest.main()
