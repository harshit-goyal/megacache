import base64
import json
import os
import tempfile
import unittest
from unittest.mock import patch

from megacache.controlplane import (
    AtomicControlState,
    BackupPolicy,
    ControlPlaneDefinition,
    ControlPlaneError,
    DisasterRecoveryPlan,
    EncryptedArtifactStore,
    JSONFileKeyProvider,
    ManagedControlPlane,
    StaticKeyProvider,
    TenantDefinition,
    TenantQuotaExceeded,
    TenantQuotas,
    TenantRetention,
    TenantUnavailable,
    load_control_plane_definition,
    tenant_namespace_id,
)
from megacache.engine import CacheEngine


class _Clock:
    def __init__(self):
        self.wall = 1_700_000_000.0
        self.monotonic = 100.0

    def time(self):
        return self.wall

    def monotonic_time(self):
        return self.monotonic

    def advance(self, seconds):
        self.wall += seconds
        self.monotonic += seconds


class _BillingProvider:
    def __init__(self):
        self.calls = []

    def export_usage(self, batch_id, payload):
        self.calls.append((batch_id, payload))
        return {"provider_reference": "receipt-" + batch_id}


def tenant(
    tenant_id,
    *,
    entries=10,
    memory=100_000,
    ops=10,
    burst=10,
    connections=2,
    backup_interval=0,
):
    return TenantDefinition(
        tenant_id=tenant_id,
        display_name=tenant_id.upper(),
        enabled=True,
        quotas=TenantQuotas(
            max_entries=entries,
            max_bytes=memory,
            max_entry_bytes=min(10_000, memory),
            ops_per_second=ops,
            burst_ops=burst,
            max_connections=connections,
            origin_concurrency=2,
        ),
        origins=(),
        webhooks=(),
        regions=("local", "recovery"),
        primary_region="local",
        desired_version="1.0.0",
        desired_replicas=1,
        max_unavailable=1,
        drain=False,
        backup=BackupPolicy(
            interval_seconds=backup_interval,
            retention_count=3,
            retention_seconds=86_400,
        ),
        disaster_recovery=DisasterRecoveryPlan(
            rpo_seconds=3600,
            rto_seconds=60,
            recovery_regions=("recovery",),
        ),
        retention=TenantRetention(
            usage_periods=24,
            completed_operations=50,
            export_count=3,
        ),
    )


class ControlPlaneTests(unittest.TestCase):
    def setUp(self):
        self.directory = tempfile.TemporaryDirectory()
        self.clock = _Clock()
        self.provider = StaticKeyProvider(b"k" * 32)
        self.tenants = (tenant("alpha"), tenant("beta"))
        self.engines = {
            item.tenant_id: CacheEngine(
                max_entries=item.quotas.max_entries,
                max_memory_bytes=item.quotas.max_bytes,
                max_entry_bytes=item.quotas.max_entry_bytes,
            )
            for item in self.tenants
        }
        self.control = ManagedControlPlane(
            ControlPlaneDefinition("alpha", self.tenants),
            self.engines,
            state_directory=self.directory.name,
            key_provider=self.provider,
            clock=self.clock.time,
            monotonic=self.clock.monotonic_time,
            start_workers=False,
            audit_segment_bytes=1024,
            audit_max_segments=16,
        )

    def tearDown(self):
        self.control.close()
        self.directory.cleanup()

    def run_operation(self, operation):
        self.control.run_pending_operations()
        return self.control.operation_status(operation["operation_id"])

    def test_repository_control_plane_example_is_valid(self):
        definition = load_control_plane_definition(
            "control-plane.example.json",
            max_tenants=100,
            max_entries=10_000,
            max_bytes=67_108_864,
            max_entry_bytes=1_048_576,
            max_usage_periods=744,
            max_operations=1000,
        )
        self.assertEqual("default", definition.default_tenant)
        self.assertEqual(2, len(definition.tenants))

    def test_tenant_data_planes_are_independent_and_namespaces_are_opaque(self):
        alpha = self.control.engine_for("alpha")
        beta = self.control.engine_for("beta")
        alpha.put("same-key", "alpha", tags=("shared-tag",))
        beta.put("same-key", "beta", tags=("shared-tag",))

        self.assertEqual("alpha", alpha.get("same-key").value)
        self.assertEqual("beta", beta.get("same-key").value)
        self.assertEqual(1, alpha.invalidate_tags(("shared-tag",)))
        self.assertEqual("miss", alpha.get("same-key").state)
        self.assertEqual("fresh", beta.get("same-key").state)
        self.assertNotEqual(
            self.control.namespace_for("alpha"),
            self.control.namespace_for("beta"),
        )
        self.assertNotIn(
            "alpha", self.control.namespace_for("alpha")
        )
        metrics = self.control.prometheus_metrics()
        self.assertNotIn('tenant="alpha"', metrics)
        self.assertNotIn('tenant="beta"', metrics)

    def test_connection_and_operation_quotas_are_hard_per_tenant(self):
        self.control.acquire_connection("alpha")
        self.control.acquire_connection("alpha")
        with self.assertRaises(TenantQuotaExceeded):
            self.control.acquire_connection("alpha")
        self.control.acquire_connection("beta")
        self.control.release_connection("alpha")
        self.control.release_connection("alpha")
        self.control.release_connection("beta")

        limited_directory = tempfile.TemporaryDirectory()
        try:
            definition = ControlPlaneDefinition(
                "limited",
                (tenant("limited", ops=1, burst=1),),
            )
            limited = ManagedControlPlane(
                definition,
                {"limited": CacheEngine(max_entries=10)},
                state_directory=limited_directory.name,
                key_provider=self.provider,
                clock=self.clock.time,
                monotonic=self.clock.monotonic_time,
                start_workers=False,
            )
            try:
                limited.begin_operation("limited")
                limited.end_operation("limited")
                with self.assertRaises(TenantQuotaExceeded):
                    limited.begin_operation("limited")
                self.clock.advance(1)
                limited.begin_operation("limited")
                limited.end_operation("limited")
            finally:
                limited.close()
        finally:
            limited_directory.cleanup()

    def test_metering_is_durable_bounded_and_billing_is_idempotent(self):
        self.control.observe_tenant_request(
            "alpha", "resp", "GET", 10, 20, True
        )
        provider = _BillingProvider()
        first = self.control.billing_export(
            tenant_id="alpha", provider=provider, actor="billing"
        )
        second = self.control.billing_export(
            tenant_id="alpha", provider=provider, actor="billing"
        )
        self.assertEqual(first["batch_id"], second["batch_id"])
        self.assertFalse(first["idempotent_replay"])
        self.assertTrue(second["idempotent_replay"])
        self.assertEqual(1, len(provider.calls))
        self.assertEqual(
            1,
            first["payload"]["records"][0]["usage"]["operations"],
        )

        self.control.close()
        reopened = ManagedControlPlane(
            ControlPlaneDefinition("alpha", self.tenants),
            {
                item.tenant_id: CacheEngine(max_entries=item.quotas.max_entries)
                for item in self.tenants
            },
            state_directory=self.directory.name,
            key_provider=self.provider,
            clock=self.clock.time,
            monotonic=self.clock.monotonic_time,
            start_workers=False,
            audit_segment_bytes=1024,
            audit_max_segments=16,
        )
        self.control = reopened
        dashboard = reopened.tenant_status("alpha")
        self.assertEqual(1, dashboard["usage"]["cumulative"]["operations"])

    def test_metering_failure_does_not_interrupt_healthy_data_plane(self):
        engine = self.control.engine_for("alpha")
        with patch.object(
            self.control._state_store,
            "persist",
            side_effect=OSError("state volume unavailable"),
        ):
            self.control.observe_tenant_request(
                "alpha", "http", "GET /v1/cache/{key}", 1, 1, True
            )
            engine.put("still-available", "value")
            self.assertEqual(
                "value", engine.get("still-available").value
            )
        status = self.control.status()
        self.assertTrue(status["control_plane_degraded"])

    def test_encrypted_backup_validation_restore_and_drill(self):
        engine = self.control.engine_for("alpha")
        engine.put("secret-key", "secret-value", tags=("private",))
        backup = self.run_operation(
            self.control.request_backup("alpha", actor="admin")
        )
        self.assertEqual("succeeded", backup["status"])
        metadata = backup["result"]["backup"]
        artifact_path = os.path.join(
            self.directory.name, "artifacts", metadata["filename"]
        )
        with open(artifact_path, "rb") as source:
            ciphertext = source.read()
        self.assertNotIn(b"secret-key", ciphertext)
        self.assertNotIn(b"secret-value", ciphertext)

        engine.flush()
        validation = self.run_operation(
            self.control.request_restore_validation(
                "alpha", metadata["artifact_id"], actor="admin"
            )
        )
        self.assertTrue(validation["result"]["valid"])
        restored = self.run_operation(
            self.control.request_restore(
                "alpha",
                metadata["artifact_id"],
                validation["result"]["validation_token"],
                "alpha",
                actor="admin",
            )
        )
        self.assertEqual("succeeded", restored["status"])
        self.assertEqual("secret-value", engine.get("secret-key").value)

        drill = self.run_operation(
            self.control.request_drill(
                "alpha", metadata["artifact_id"], actor="operator"
            )
        )
        self.assertEqual("succeeded", drill["status"])
        self.assertEqual("local_validation_only", drill["result"]["scope"])

    def test_restore_does_not_extend_expired_freshness_windows(self):
        engine = self.control.engine_for("alpha")
        engine.put("short-lived", "value", ttl_seconds=1, stale_seconds=1)
        backup = self.run_operation(
            self.control.request_backup("alpha", actor="admin")
        )
        backup_id = backup["result"]["backup"]["artifact_id"]
        self.clock.advance(3)
        validation = self.run_operation(
            self.control.request_restore_validation(
                "alpha", backup_id, actor="admin"
            )
        )
        restored = self.run_operation(
            self.control.request_restore(
                "alpha",
                backup_id,
                validation["result"]["validation_token"],
                "alpha",
                actor="admin",
            )
        )
        self.assertEqual(0, restored["result"]["restored_entries"])
        self.assertEqual("miss", engine.get("short-lived").state)

    def test_tampered_backup_fails_validation_without_mutating_cache(self):
        engine = self.control.engine_for("alpha")
        engine.put("protected", "original")
        backup = self.run_operation(
            self.control.request_backup("alpha", actor="admin")
        )
        metadata = backup["result"]["backup"]
        artifact_path = os.path.join(
            self.directory.name, "artifacts", metadata["filename"]
        )
        with open(artifact_path, "r", encoding="utf-8") as source:
            envelope = json.load(source)
        replacement = "A" if envelope["ciphertext"][0] != "A" else "B"
        envelope["ciphertext"] = replacement + envelope["ciphertext"][1:]
        with open(artifact_path, "w", encoding="utf-8") as destination:
            json.dump(envelope, destination, separators=(",", ":"))
        validation = self.run_operation(
            self.control.request_restore_validation(
                "alpha", metadata["artifact_id"], actor="admin"
            )
        )
        self.assertEqual("failed", validation["status"])
        self.assertEqual("original", engine.get("protected").value)

    def test_export_and_irreversible_deletion_safeguards(self):
        self.control.engine_for("alpha").put("customer:1", {"name": "Ada"})
        exported = self.run_operation(
            self.control.request_data_export("alpha", actor="admin")
        )
        self.assertEqual("succeeded", exported["status"])
        challenge = self.control.create_deletion_challenge(
            "alpha", actor="admin"
        )
        with self.assertRaisesRegex(ControlPlaneError, "exactly match"):
            self.control.request_tenant_deletion(
                "alpha",
                actor="admin",
                challenge=challenge["challenge"],
                confirmation="beta",
            )
        deleted = self.run_operation(
            self.control.request_tenant_deletion(
                "alpha",
                actor="admin",
                challenge=challenge["challenge"],
                confirmation="alpha",
            )
        )
        self.assertEqual("succeeded", deleted["status"])
        with self.assertRaises(TenantUnavailable):
            self.control.engine_for("alpha")
        self.assertEqual(
            "deleted", self.control.tenant_status("alpha")["status"]
        )
        self.control.acquire_connection("alpha")
        self.control.begin_operation("alpha", control=True)
        self.control.end_operation("alpha")
        with self.assertRaises(TenantUnavailable):
            self.control.begin_operation("alpha")
        self.control.release_connection("alpha")

    def test_desired_and_observed_deployment_metadata_is_not_orchestration(self):
        desired = self.control.set_desired_deployment(
            "alpha",
            {
                "version": "1.1.0",
                "regions": ["local", "recovery"],
                "replicas": 2,
                "max_unavailable": 1,
                "drain": True,
            },
            actor="operator",
        )
        observed = self.control.report_observed_deployment(
            "alpha",
            {
                "instance_id": "instance-a",
                "region": "local",
                "version": "1.0.0",
                "health": "healthy",
                "draining": False,
                "observed_generation": 1,
            },
            actor="operator",
        )
        self.assertEqual(2, desired["generation"])
        self.assertEqual("instance-a", observed["instance_id"])
        view = self.control.orchestrator_status()
        self.assertEqual(
            "metadata_only_external_orchestrator", view["boundary"]
        )
        self.assertIn(
            "deployment_drift",
            {
                item["code"]
                for item in self.control.tenant_status("alpha")["alerts"]
            },
        )

    def test_audit_chain_rotates_exports_and_detects_tampering(self):
        for index in range(8):
            self.control.set_desired_deployment(
                "alpha",
                {"version": "1.0.{}".format(index)},
                actor="operator",
            )
        exported = self.control.export_audit(tenant_id="alpha")
        self.assertTrue(exported["chain_valid"])
        self.assertGreaterEqual(len(exported["records"]), 8)
        self.assertGreaterEqual(
            len(os.listdir(os.path.join(self.directory.name, "audit"))), 2
        )

        self.control.close()
        audit_path = os.path.join(
            self.directory.name, "audit", "audit-00000001.jsonl"
        )
        with open(audit_path, "r+b") as handle:
            data = handle.read()
            handle.seek(0)
            handle.write(data.replace(b"operator", b"attacker", 1))
            handle.truncate()
        with self.assertRaisesRegex(ControlPlaneError, "verification failed"):
            ManagedControlPlane(
                ControlPlaneDefinition("alpha", self.tenants),
                {
                    item.tenant_id: CacheEngine(max_entries=item.quotas.max_entries)
                    for item in self.tenants
                },
                state_directory=self.directory.name,
                key_provider=self.provider,
                start_workers=False,
                audit_segment_bytes=1024,
                audit_max_segments=16,
            )

    def test_exported_audit_segments_can_be_pruned_with_signed_anchor(self):
        for index in range(8):
            self.control.set_desired_deployment(
                "alpha",
                {"version": "2.0.{}".format(index)},
                actor="operator",
            )
        exported = self.control.export_audit()
        boundary = exported["records"][-1]
        result = self.control.prune_audit(
            through_sequence=boundary["sequence"],
            expected_hash=boundary["hash"],
            actor="auditor",
        )
        self.assertGreater(result["removed_segments"], 0)
        self.control.close()
        reopened = ManagedControlPlane(
            ControlPlaneDefinition("alpha", self.tenants),
            {
                item.tenant_id: CacheEngine(max_entries=item.quotas.max_entries)
                for item in self.tenants
            },
            state_directory=self.directory.name,
            key_provider=self.provider,
            start_workers=False,
            audit_segment_bytes=1024,
            audit_max_segments=16,
        )
        self.control = reopened
        self.assertTrue(reopened.export_audit()["chain_valid"])

    def test_single_owner_and_state_migration(self):
        with self.assertRaisesRegex(ControlPlaneError, "already owned"):
            AtomicControlState(self.directory.name, 1_000_000)

        self.control.close()
        state_path = os.path.join(self.directory.name, "control-state.json")
        with open(state_path, "r", encoding="utf-8") as source:
            state = json.load(source)
        state["version"] = 1
        state.pop("billing_exports")
        state.pop("meter_sequence")
        with open(state_path, "w", encoding="utf-8") as destination:
            json.dump(state, destination)
        migrated = ManagedControlPlane(
            ControlPlaneDefinition("alpha", self.tenants),
            {
                item.tenant_id: CacheEngine(max_entries=item.quotas.max_entries)
                for item in self.tenants
            },
            state_directory=self.directory.name,
            key_provider=self.provider,
            start_workers=False,
            audit_segment_bytes=1024,
            audit_max_segments=16,
        )
        self.control = migrated
        with open(state_path, "r", encoding="utf-8") as source:
            self.assertEqual(2, json.load(source)["version"])

    def test_configuration_is_strict_and_respects_global_capacity(self):
        path = os.path.join(self.directory.name, "tenants.json")
        with open(path, "w", encoding="utf-8") as destination:
            json.dump(
                {
                    "version": 1,
                    "default_tenant": "alpha",
                    "tenants": [
                        {
                            "id": "alpha",
                            "quotas": {
                                "max_entries": 10,
                                "max_bytes": 1000,
                                "max_entry_bytes": 500,
                                "ops_per_second": 20,
                                "burst_ops": 20,
                                "max_connections": 5,
                                "origin_concurrency": 2,
                            },
                        }
                    ],
                },
                destination,
            )
        loaded = load_control_plane_definition(
            path,
            max_tenants=2,
            max_entries=10,
            max_bytes=1000,
            max_entry_bytes=500,
            max_usage_periods=24,
            max_operations=100,
        )
        self.assertEqual("alpha", loaded.default_tenant)

        with open(path, encoding="utf-8") as source:
            document = json.load(source)
        document["tenants"][0]["quotas"]["max_entries"] = 11
        with open(path, "w", encoding="utf-8") as destination:
            json.dump(document, destination)
        with self.assertRaisesRegex(ControlPlaneError, "entry quotas"):
            load_control_plane_definition(
                path,
                max_tenants=2,
                max_entries=10,
                max_bytes=1000,
                max_entry_bytes=500,
                max_usage_periods=24,
                max_operations=100,
            )

    def test_file_key_provider_rotates_artifacts_without_changing_namespace(self):
        key_path = os.path.join(self.directory.name, "keys.json")
        first_key = base64.urlsafe_b64encode(b"a" * 32).decode("ascii")
        second_key = base64.urlsafe_b64encode(b"b" * 32).decode("ascii")
        with open(key_path, "w", encoding="utf-8") as destination:
            json.dump(
                {
                    "version": 1,
                    "active_key_id": "first",
                    "namespace_key_id": "first",
                    "keys": {"first": first_key},
                },
                destination,
            )
        os.chmod(key_path, 0o600)
        first = JSONFileKeyProvider(key_path)
        namespace = tenant_namespace_id(first, "alpha")
        artifacts = EncryptedArtifactStore(
            self.directory.name, first, 100_000
        )
        metadata = artifacts.write(
            "export", namespace, {"version": 1, "value": "protected"}
        )

        with open(key_path, "w", encoding="utf-8") as destination:
            json.dump(
                {
                    "version": 1,
                    "active_key_id": "second",
                    "namespace_key_id": "first",
                    "keys": {
                        "first": first_key,
                        "second": second_key,
                    },
                },
                destination,
            )
        os.chmod(key_path, 0o600)
        rotated = JSONFileKeyProvider(key_path)
        self.assertEqual(namespace, tenant_namespace_id(rotated, "alpha"))
        self.assertEqual(
            "protected",
            EncryptedArtifactStore(
                self.directory.name, rotated, 100_000
            ).read("export", metadata["artifact_id"], namespace)["value"],
        )


if __name__ == "__main__":
    unittest.main()
