import unittest

from megacache import (
    CacheEngine,
    ClusterNode,
    ClusterStorage,
    ConsistentHashRing,
    FenceError,
    QuorumError,
    RebalanceError,
    SnapshotError,
    StorageEntry,
)
from megacache.cluster import (
    SnapshotBackpressure,
    SnapshotChunk,
    SnapshotError,
    SnapshotReceiver,
)


class FakeClock:
    def __init__(self):
        self.value = 100.0

    def __call__(self):
        return self.value

    def advance(self, seconds):
        self.value += seconds


class FailingPutEngine(CacheEngine):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.fail_put = False

    def put(self, *args, **kwargs):
        result = super().put(*args, **kwargs)
        if self.fail_put:
            raise RuntimeError("simulated write acknowledgement failure")
        return result


class FailingRestoreEngine(CacheEngine):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.fail_restore = False

    def restore_entries(self, entries):
        result = super().restore_entries(entries)
        if self.fail_restore:
            raise RuntimeError("simulated restore acknowledgement failure")
        return result


class FailingDeleteEngine(CacheEngine):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.fail_delete_key = None

    def delete(self, key):
        result = super().delete(key)
        if key == self.fail_delete_key:
            raise RuntimeError("simulated delete acknowledgement failure")
        return result


class NoFullCheckpointFailingPutEngine(FailingPutEngine):
    def checkpoint(self):
        raise AssertionError("single-key writes must not copy the full cache")


class DroppingRestoreEngine(CacheEngine):
    def restore_entries(self, entries):
        return len(tuple(entries))


class FailingGetEngine(CacheEngine):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.fail_get = False

    def get(self, key):
        if self.fail_get:
            raise ValueError("simulated replica read failure")
        return super().get(key)


class ClusterTests(unittest.TestCase):
    def make_cluster(self, consistency="majority", **kwargs):
        self.clock = FakeClock()
        self.stores = {
            node_id: CacheEngine(
                max_entries=100,
                default_ttl_seconds=60,
                default_stale_seconds=60,
                clock=self.clock,
            )
            for node_id in ("node-a", "node-b", "node-c")
        }
        return ClusterStorage(
            [
                ClusterNode(node_id, storage)
                for node_id, storage in self.stores.items()
            ],
            replica_count=3,
            virtual_nodes=32,
            consistency=consistency,
            heartbeat_timeout_seconds=10,
            clock=self.clock,
            **kwargs
        )

    def test_ring_is_deterministic_across_node_order(self):
        first = ConsistentHashRing(
            ["node-c", "node-a", "node-b"], virtual_nodes=64, version=7
        )
        second = ConsistentHashRing(
            ["node-b", "node-c", "node-a"], virtual_nodes=64, version=7
        )
        self.assertEqual(first.fingerprint, second.fingerprint)
        self.assertEqual(
            first.owners("tenant:42:item:9", 3),
            second.owners("tenant:42:item:9", 3),
        )
        self.assertEqual(7, first.describe()["version"])

    def test_majority_survives_one_node_loss_and_repairs_on_return(self):
        cluster = self.make_cluster()
        cluster.put("key", "v1")
        cluster.mark_node_down("node-c")
        cluster.put("key", "v2")
        self.assertEqual("v2", cluster.get("key").value)

        cluster.heartbeat("node-c", incarnation=2)
        self.assertEqual("v2", cluster.get("key").value)
        self.assertEqual("v2", self.stores["node-c"].get("key").value)
        self.assertFalse(cluster.status()["degraded"])

    def test_quorum_failure_rejects_write_without_partial_mutation(self):
        cluster = self.make_cluster()
        cluster.put("key", "stable")
        cluster.mark_node_down("node-b")
        cluster.mark_node_down("node-c")
        with self.assertRaisesRegex(QuorumError, "write quorum unavailable"):
            cluster.put("key", "unacknowledged")
        with self.assertRaises(QuorumError):
            cluster.get("key")
        self.assertEqual(
            "stable", cluster.get("key", consistency="one").value
        )

    def test_explicit_all_and_one_profiles(self):
        cluster = self.make_cluster(consistency="all")
        cluster.put("key", "value")
        cluster.mark_node_down("node-c")
        with self.assertRaises(QuorumError):
            cluster.get("key")
        self.assertEqual(
            "value", cluster.get("key", consistency="one").value
        )

    def test_leader_failover_fences_old_term(self):
        cluster = self.make_cluster()
        old_fence = cluster.leadership()
        self.assertEqual("node-a", old_fence.leader_id)
        cluster.mark_node_down("node-a")
        new_fence = cluster.leadership()
        self.assertEqual("node-b", new_fence.leader_id)
        self.assertGreater(new_fence.term, old_fence.term)
        with self.assertRaises(FenceError):
            cluster.put("key", "value", fence=old_fence)
        cluster.put("key", "value", fence=new_fence)

    def test_cluster_lease_and_tag_invalidation_are_distributed(self):
        cluster = self.make_cluster()
        lease = cluster.acquire_lease("product:1")
        cluster.put(
            "product:1",
            {"name": "Desk"},
            tags=["catalog"],
            lease_token=lease.lease_token,
        )
        self.assertEqual(1, cluster.invalidate_tags(["catalog"]))
        for storage in self.stores.values():
            self.assertEqual("miss", storage.get("product:1").state)

    def test_cluster_leases_are_bounded_and_consumed_by_writes(self):
        cluster = ClusterStorage(
            [ClusterNode("node-a", CacheEngine())],
            max_leases=1,
            max_lease_memory_bytes=1_000,
            clock=FakeClock(),
        )
        lease = cluster.acquire_lease("first")
        self.assertGreater(cluster.stats()["lease_memory_bytes"], 0)
        with self.assertRaisesRegex(ValueError, "lease capacity"):
            cluster.acquire_lease("second")
        cluster.put("first", "value")
        self.assertEqual(0, cluster.stats()["lease_memory_bytes"])
        with self.assertRaisesRegex(ValueError, "missing or expired"):
            cluster.put("first", "new", lease_token=lease.lease_token)

    def test_cluster_preserves_tag_input_validation(self):
        cluster = self.make_cluster()
        with self.assertRaisesRegex(ValueError, "array"):
            cluster.put("key", "value", tags="not-an-array")

    def test_online_add_and_drain_rebalance_preserve_values(self):
        cluster = self.make_cluster()
        for index in range(20):
            cluster.put("key:{}".format(index), "value:{}".format(index))

        new_store = CacheEngine(max_entries=100, clock=self.clock)
        add_plan = cluster.add_node("node-d", new_store)
        self.assertTrue(add_plan.moves)
        cluster.apply_rebalance(add_plan)
        self.assertIn("node-d", cluster.ring.node_ids)
        self.assertEqual(2, cluster.ring.version)
        for index in range(20):
            self.assertEqual(
                "value:{}".format(index),
                cluster.get("key:{}".format(index)).value,
            )

        drain_plan = cluster.drain_node("node-a")
        cluster.apply_rebalance(drain_plan)
        self.assertNotIn("node-a", cluster.ring.node_ids)
        topology = cluster.topology()
        node_a = next(
            node for node in topology["nodes"] if node["node_id"] == "node-a"
        )
        self.assertEqual("drained", node_a["status"])

    def test_snapshot_checksums_limits_and_backpressure(self):
        cluster = self.make_cluster(
            snapshot_chunk_bytes=512,
            snapshot_max_in_flight=1,
        )
        cluster.put("key", b"\x00binary", tags=["tag"])
        session = cluster.create_snapshot("node-a")
        chunk = session.next_chunk()
        self.assertIsNotNone(chunk)
        with self.assertRaises(SnapshotBackpressure):
            session.next_chunk()

        receiver = SnapshotReceiver(
            session.manifest, max_payload_bytes=10_000
        )
        corrupt = SnapshotChunk(
            snapshot_id=chunk.snapshot_id,
            index=chunk.index,
            payload=chunk.payload + b"x",
            checksum=chunk.checksum,
        )
        with self.assertRaisesRegex(SnapshotError, "checksum"):
            receiver.receive(corrupt)

        receiver.receive(chunk)
        session.acknowledge(chunk.index)
        self.assertEqual(1, len(receiver.records()))

        target = CacheEngine(max_entries=100, clock=self.clock)
        cluster.add_node("node-d", target)
        transfer = cluster.create_snapshot("node-a", keys=("key",))
        cluster.bootstrap_node("node-d", transfer)
        self.assertEqual(b"\x00binary", target.get("key").value)
        self.assertEqual(("tag",), target.export_entries(("key",))[0].tags)

    def test_snapshot_payload_limit_is_enforced(self):
        cluster = self.make_cluster(
            snapshot_payload_limit_bytes=256,
            snapshot_chunk_bytes=128,
        )
        cluster.put("key", "x" * 200)
        with self.assertRaisesRegex(SnapshotError, "payload limit"):
            cluster.create_snapshot("node-a")

    def test_cluster_mset_rolls_back_every_key_on_admission_failure(self):
        cluster = self.make_cluster()
        with self.assertRaises(ValueError):
            cluster.mset(
                (("first", "stored"), ("too-large", "x" * 1_100_000))
            )
        self.assertEqual("miss", cluster.get("first").state)

    def test_rebalance_refuses_stale_healthy_source(self):
        cluster = self.make_cluster()
        cluster.put("key", "v1")
        cluster.mark_node_down("node-c")
        cluster.put("key", "v2")
        cluster.mark_node_down("node-a")
        cluster.mark_node_down("node-b")
        cluster.heartbeat("node-c", incarnation=2)

        with self.assertRaisesRegex(
            RebalanceError, "latest-version source"
        ):
            cluster.add_node(
                "node-d", CacheEngine(max_entries=100, clock=self.clock)
            )
        self.assertNotIn(
            "node-d",
            {node["node_id"] for node in cluster.topology()["nodes"]},
        )
        self.assertEqual("v2", self.stores["node-a"].get("key").value)
        self.assertEqual("v2", self.stores["node-b"].get("key").value)

    def test_rebalance_verifies_targets_before_old_owner_cleanup(self):
        clock = FakeClock()
        source = CacheEngine(max_entries=100, clock=clock)
        cluster = ClusterStorage(
            [ClusterNode("node-a", source)],
            replica_count=1,
            virtual_nodes=32,
            clock=clock,
        )
        for index in range(20):
            cluster.put("key:{}".format(index), "value:{}".format(index))
        target = DroppingRestoreEngine(max_entries=100, clock=clock)
        plan = cluster.add_node("node-b", target)
        self.assertTrue(plan.moves)

        with self.assertRaisesRegex(RebalanceError, "not verified"):
            cluster.apply_rebalance(plan)

        self.assertEqual(("node-a",), cluster.ring.node_ids)
        for move in plan.moves:
            self.assertEqual(
                "value:{}".format(move.key.split(":")[1]),
                source.get(move.key).value,
            )

    def test_rebalance_cleanup_delete_failure_restores_plan_state(self):
        clock = FakeClock()
        source = FailingDeleteEngine(max_entries=100, clock=clock)
        cluster = ClusterStorage(
            [ClusterNode("node-a", source)],
            replica_count=1,
            virtual_nodes=32,
            clock=clock,
        )
        for index in range(20):
            cluster.put("key:{}".format(index), "value:{}".format(index))
        target = CacheEngine(max_entries=100, clock=clock)
        plan = cluster.add_node("node-b", target)
        move = next(
            move
            for move in plan.moves
            if move.source_node_id == "node-a"
        )
        source.fail_delete_key = move.key

        with self.assertRaisesRegex(RuntimeError, "delete acknowledgement"):
            cluster.apply_rebalance(plan)

        self.assertEqual(("node-a",), cluster.ring.node_ids)
        for index in range(20):
            self.assertEqual(
                "value:{}".format(index),
                source.get("key:{}".format(index)).value,
            )
        self.assertEqual(0, target.size())

    def test_failed_quorum_write_restores_lru_and_accounting(self):
        clock = FakeClock()
        stores = {
            "node-a": CacheEngine(max_entries=1, clock=clock),
            "node-b": CacheEngine(max_entries=1, clock=clock),
            "node-c": FailingPutEngine(max_entries=1, clock=clock),
        }
        cluster = ClusterStorage(
            [
                ClusterNode(node_id, storage)
                for node_id, storage in stores.items()
            ],
            replica_count=3,
            consistency="all",
            clock=clock,
        )
        for node_id, storage in stores.items():
            storage.put("unrelated:{}".format(node_id), "stable")
        before = {node_id: storage.stats() for node_id, storage in stores.items()}
        stores["node-c"].fail_put = True

        with self.assertRaises(QuorumError):
            cluster.put("new", "value")

        for node_id, storage in stores.items():
            self.assertEqual(
                "stable",
                storage.get("unrelated:{}".format(node_id)).value,
            )
            self.assertEqual("miss", storage.get("new").state)
            self.assertEqual(
                before[node_id].get("evictions_total", 0),
                storage.stats().get("evictions_total", 0),
            )
            self.assertEqual(
                before[node_id]["memory_bytes"],
                storage.stats()["memory_bytes"],
            )

    def test_single_key_replication_uses_targeted_mutation_journals(self):
        clock = FakeClock()
        stores = {
            "node-a": NoFullCheckpointFailingPutEngine(
                max_entries=1, clock=clock
            ),
            "node-b": NoFullCheckpointFailingPutEngine(
                max_entries=1, clock=clock
            ),
            "node-c": NoFullCheckpointFailingPutEngine(
                max_entries=1, clock=clock
            ),
        }
        cluster = ClusterStorage(
            [
                ClusterNode(node_id, storage)
                for node_id, storage in stores.items()
            ],
            replica_count=3,
            consistency="all",
            clock=clock,
        )
        for node_id, storage in stores.items():
            storage.put("unrelated:{}".format(node_id), "stable")
        stores["node-c"].fail_put = True

        with self.assertRaises(QuorumError):
            cluster.put("new", "value")

        for node_id, storage in stores.items():
            self.assertEqual(
                "stable",
                storage.get("unrelated:{}".format(node_id)).value,
            )
            self.assertEqual("miss", storage.get("new").state)

    def test_mutating_runtime_delete_failure_restores_every_replica(self):
        clock = FakeClock()
        stores = {
            "node-a": CacheEngine(max_entries=10, clock=clock),
            "node-b": CacheEngine(max_entries=10, clock=clock),
            "node-c": FailingDeleteEngine(max_entries=10, clock=clock),
        }
        cluster = ClusterStorage(
            [
                ClusterNode(node_id, storage)
                for node_id, storage in stores.items()
            ],
            replica_count=3,
            consistency="all",
            clock=clock,
        )
        cluster.put("key", "stable", tags=["tag"])
        before = {
            node_id: storage.stats() for node_id, storage in stores.items()
        }
        stores["node-c"].fail_delete_key = "key"

        with self.assertRaises(QuorumError):
            cluster.delete("key")

        self.assertEqual("stable", cluster.get("key").value)
        for node_id, storage in stores.items():
            self.assertEqual("stable", storage.get("key").value)
            self.assertEqual(
                before[node_id]["memory_bytes"],
                storage.stats()["memory_bytes"],
            )

    def test_delete_many_is_transaction_wide_atomic(self):
        clock = FakeClock()
        stores = {
            "node-a": CacheEngine(max_entries=10, clock=clock),
            "node-b": CacheEngine(max_entries=10, clock=clock),
            "node-c": FailingDeleteEngine(max_entries=10, clock=clock),
        }
        cluster = ClusterStorage(
            [
                ClusterNode(node_id, storage)
                for node_id, storage in stores.items()
            ],
            replica_count=3,
            consistency="all",
            clock=clock,
        )
        cluster.put("first", "one")
        cluster.put("second", "two")
        stores["node-c"].fail_delete_key = "second"

        with self.assertRaises(QuorumError):
            cluster.delete_many(("first", "second"))

        self.assertEqual("one", cluster.get("first").value)
        self.assertEqual("two", cluster.get("second").value)
        for storage in stores.values():
            self.assertEqual("one", storage.get("first").value)
            self.assertEqual("two", storage.get("second").value)

    def test_tombstone_gc_is_immediate_or_bounded_by_backpressure(self):
        cluster = self.make_cluster(max_retained_tombstones=1)
        cluster.put("first", "one")
        cluster.delete("first")
        self.assertEqual(0, cluster.stats()["retained_tombstones"])

        cluster.put("first", "one")
        cluster.put("second", "two")
        cluster.mark_node_down("node-c")
        cluster.delete("first")
        self.assertEqual(1, cluster.stats()["retained_tombstones"])
        with self.assertRaisesRegex(ValueError, "tombstone limit"):
            cluster.delete("second")
        self.assertEqual("two", cluster.get("second").value)

        cluster.heartbeat("node-c", incarnation=2)
        self.assertEqual("miss", cluster.get("first").state)
        self.assertEqual(0, cluster.stats()["retained_tombstones"])
        self.assertTrue(cluster.delete("second"))

    def test_rebalance_compacts_quorum_confirmed_expired_catalog_keys(self):
        cluster = self.make_cluster()
        cluster.put("expired", "value", ttl_seconds=1, stale_seconds=1)
        self.clock.advance(3)

        plan = cluster.add_node(
            "node-d", CacheEngine(max_entries=100, clock=self.clock)
        )
        cluster.apply_rebalance(plan)

        self.assertEqual("miss", cluster.get("expired").state)
        self.assertEqual(0, cluster.stats()["retained_tombstones"])

    def test_rebalance_compacts_fully_evicted_catalog_keys(self):
        clock = FakeClock()
        stores = {
            node_id: CacheEngine(max_entries=1, clock=clock)
            for node_id in ("node-a", "node-b", "node-c")
        }
        cluster = ClusterStorage(
            [
                ClusterNode(node_id, storage)
                for node_id, storage in stores.items()
            ],
            replica_count=3,
            consistency="majority",
            virtual_nodes=32,
            clock=clock,
        )
        cluster.put("evicted", "old")
        cluster.put("current", "new")

        plan = cluster.add_node(
            "node-d", CacheEngine(max_entries=10, clock=clock)
        )
        cluster.apply_rebalance(plan)

        self.assertEqual("miss", cluster.get("evicted").state)
        self.assertEqual("new", cluster.get("current").value)

    def test_equal_version_physical_miss_prefers_live_and_repairs(self):
        cluster = self.make_cluster()
        cluster.put("key", "value")
        first_owner = cluster.ring.owners("key", 3)[0]
        self.stores[first_owner].delete("key")

        self.assertEqual("value", cluster.get("key").value)
        self.assertEqual("value", self.stores[first_owner].get("key").value)
        self.assertGreaterEqual(cluster.stats()["read_repairs_total"], 1)

    def test_explain_skips_first_owner_physical_miss_for_live_replica(self):
        cluster = self.make_cluster()
        cluster.put("key", "value")
        first_owner = cluster.ring.owners("key", 3)[0]
        self.stores[first_owner].delete("key")
        before = {
            node_id: storage.stats()
            for node_id, storage in self.stores.items()
        }

        explanation = cluster.explain_entry("key")

        self.assertEqual("fresh", explanation["state"])
        self.assertTrue(explanation["present"])
        self.assertEqual(
            "miss",
            self.stores[first_owner].explain_entry("key")["state"],
        )
        for node_id, storage in self.stores.items():
            after = storage.stats()
            self.assertEqual(
                before[node_id].get("hits_total", 0),
                after.get("hits_total", 0),
            )
            self.assertEqual(
                before[node_id].get("misses_total", 0),
                after.get("misses_total", 0),
            )

    def test_stale_and_evicted_hot_replicas_are_pruned_without_masking_owner(self):
        clock = FakeClock()
        stores = {
            name: CacheEngine(max_entries=10, clock=clock)
            for name in ("a", "b", "c")
        }
        cluster = ClusterStorage(
            [ClusterNode(name, storage) for name, storage in stores.items()],
            replica_count=1,
            consistency="one",
            clock=clock,
        )
        cluster.put("key", "v1")
        self.assertEqual(1, cluster.replicate_hot_key("key"))
        stale_hot = cluster.ownership("key")["hot_replicas"][0]
        cluster.put("key", "v2")

        self.assertEqual("v2", cluster.get("key").value)
        self.assertNotIn(stale_hot, cluster.ownership("key")["hot_replicas"])
        self.assertEqual("miss", stores[stale_hot].get("key").state)

        self.assertEqual(1, cluster.replicate_hot_key("key"))
        evicted_hot = cluster.ownership("key")["hot_replicas"][0]
        stores[evicted_hot].delete("key")

        self.assertEqual("v2", cluster.get("key").value)
        self.assertNotIn(evicted_hot, cluster.ownership("key")["hot_replicas"])
        self.assertNotIn("key", cluster._metadata[evicted_hot])

    def test_hot_replica_miss_never_counts_toward_owner_read_quorum(self):
        clock = FakeClock()
        stores = {
            name: FailingGetEngine(max_entries=10, clock=clock)
            for name in ("a", "b", "c")
        }
        cluster = ClusterStorage(
            [ClusterNode(name, storage) for name, storage in stores.items()],
            replica_count=2,
            consistency="all",
            clock=clock,
        )
        cluster.put("key", "value")
        self.assertEqual(1, cluster.replicate_hot_key("key"))
        ownership = cluster.ownership("key")
        hot = ownership["hot_replicas"][0]
        stores[hot].delete("key")
        stores[ownership["replicas"][1]].fail_get = True

        with self.assertRaisesRegex(QuorumError, "required 2, received 1"):
            cluster.get("key")
        self.assertNotIn(hot, cluster.ownership("key")["hot_replicas"])

    def test_snapshot_cannot_overwrite_newer_target_version(self):
        cluster = self.make_cluster()
        cluster.put("key", "old")
        snapshot = cluster.create_snapshot("node-a", keys=("key",))
        cluster.put("key", "new")

        cluster.bootstrap_node("node-b", snapshot)
        self.assertEqual("new", self.stores["node-b"].get("key").value)

    def test_compacted_tombstone_rejects_older_snapshot_record(self):
        cluster = self.make_cluster()
        cluster.put("key", "old")
        snapshot = cluster.create_snapshot("node-a", keys=("key",))
        cluster.delete("key")
        self.assertEqual(0, cluster.stats()["retained_tombstones"])

        cluster.bootstrap_node("node-b", snapshot)

        self.assertEqual("miss", cluster.get("key").state)
        self.assertEqual("miss", self.stores["node-b"].get("key").state)

    def test_snapshot_is_bound_to_leadership_fence(self):
        cluster = self.make_cluster()
        cluster.put("key", "value")
        snapshot = cluster.create_snapshot("node-a", keys=("key",))
        cluster.mark_node_down("node-a")

        with self.assertRaisesRegex(SnapshotError, "leadership fence"):
            cluster.bootstrap_node("node-b", snapshot)

    def test_tag_invalidation_preflights_entire_key_set(self):
        clock = FakeClock()
        stores = {
            node_id: CacheEngine(max_entries=100, clock=clock)
            for node_id in ("node-a", "node-b", "node-c")
        }
        cluster = ClusterStorage(
            [
                ClusterNode(node_id, storage)
                for node_id, storage in stores.items()
            ],
            replica_count=2,
            consistency="all",
            virtual_nodes=32,
            clock=clock,
        )
        by_owners = {}
        for index in range(1_000):
            key = "tagged:{:04d}".format(index)
            by_owners.setdefault(cluster.ring.owners(key, 2), key)
        first_key = None
        second_key = None
        failed_node = None
        owner_sets = list(by_owners.items())
        for first_owners, candidate_first in owner_sets:
            for second_owners, candidate_second in owner_sets:
                difference = set(second_owners) - set(first_owners)
                if difference and candidate_first < candidate_second:
                    first_key = candidate_first
                    second_key = candidate_second
                    failed_node = sorted(difference)[0]
                    break
            if first_key is not None:
                break
        self.assertIsNotNone(first_key)
        cluster.put(first_key, "first", tags=["group"])
        cluster.put(second_key, "second", tags=["group"])
        cluster.mark_node_down(failed_node)

        with self.assertRaises(QuorumError):
            cluster.invalidate_tags(["group"])

        cluster.heartbeat(failed_node, incarnation=2)
        self.assertEqual("first", cluster.get(first_key).value)
        self.assertEqual("second", cluster.get(second_key).value)

    def test_cluster_restore_is_transaction_wide_atomic(self):
        clock = FakeClock()
        stores = {
            "node-a": CacheEngine(max_entries=10, clock=clock),
            "node-b": CacheEngine(max_entries=10, clock=clock),
            "node-c": FailingRestoreEngine(max_entries=10, clock=clock),
        }
        cluster = ClusterStorage(
            [
                ClusterNode(node_id, storage)
                for node_id, storage in stores.items()
            ],
            replica_count=3,
            consistency="all",
            clock=clock,
        )
        for node_id, storage in stores.items():
            storage.put("unrelated:{}".format(node_id), "stable")
        before = {node_id: storage.stats() for node_id, storage in stores.items()}
        stores["node-c"].fail_restore = True
        entries = tuple(
            StorageEntry(
                key=key,
                value=value,
                fresh_for_seconds=None,
                stale_for_seconds=None,
                tags=("restored",),
                persistent=True,
            )
            for key, value in (("first", "one"), ("second", "two"))
        )

        with self.assertRaises(QuorumError):
            cluster.restore_entries(entries)

        for node_id, storage in stores.items():
            self.assertEqual(
                "stable",
                storage.get("unrelated:{}".format(node_id)).value,
            )
            self.assertEqual("miss", storage.get("first").state)
            self.assertEqual("miss", storage.get("second").state)
            self.assertEqual(
                before[node_id]["memory_bytes"],
                storage.stats()["memory_bytes"],
            )
        self.assertEqual(0, cluster.stats()["entries"])
        self.assertEqual(0, cluster.stats()["tags"])

    def test_deterministic_write_validation_is_not_a_quorum_error(self):
        cluster = self.make_cluster()
        for kwargs in (
            {"ttl_seconds": 0},
            {"value": "x" * 1_100_000},
            {"value": object()},
        ):
            value = kwargs.pop("value", "value")
            with self.assertRaises(ValueError) as raised:
                cluster.put("key", value, **kwargs)
            self.assertNotIsInstance(raised.exception, QuorumError)

    def test_failure_detection_uses_heartbeats(self):
        cluster = self.make_cluster()
        self.clock.advance(11)
        cluster.heartbeat("node-b")
        cluster.heartbeat("node-c")
        self.assertEqual(("node-a",), cluster.detect_failures())
        self.assertEqual("node-b", cluster.leadership().leader_id)

    def test_cluster_heartbeat_timeout_must_be_finite(self):
        for timeout in (float("nan"), float("inf"), float("-inf")):
            with self.subTest(timeout=timeout), self.assertRaisesRegex(
                ValueError, "timeouts"
            ):
                ClusterStorage(
                    [ClusterNode("node-a", CacheEngine())],
                    heartbeat_timeout_seconds=timeout,
                )


if __name__ == "__main__":
    unittest.main()
