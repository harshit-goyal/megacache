import hashlib
import hmac
import io
import json
import os
import shutil
import socket
import threading
import time
import unittest
import uuid
from contextlib import redirect_stdout
from http.client import HTTPConnection
from unittest.mock import patch

from megacache.cli import run
from megacache.config import Config
from megacache.engine import CacheEngine
from megacache.events import (
    AtomicCheckpointStore,
    ChangeEvent,
    DependencyGraph,
    DependencyGraphError,
    EventAutomation,
    EventBackpressure,
    EventError,
    EventIngestionDisabled,
    EventIngestor,
    EventRule,
    KafkaRecord,
    KafkaRecordAdapter,
    MongoChangeRecord,
    MongoChangeStreamAdapter,
    MySQLBinlogAdapter,
    MySQLBinlogRecord,
    PostgresLogicalAdapter,
    PostgresLogicalRecord,
    ReaderRange,
    SchemaRegistry,
    VersionedNamespace,
    WebhookAuthError,
    WebhookAuthenticator,
    WebhookSource,
    load_event_automation,
    parse_postgres_lsn,
    webhook_signature_payload,
)
from megacache.resp import MegaCacheRespServer
from megacache.server import MegaCacheServer


class _Artifacts:
    def setUp(self):
        self.directory = os.path.join(
            os.getcwd(), ".test-events-{}".format(uuid.uuid4().hex)
        )
        os.mkdir(self.directory)
        self.state_path = os.path.join(self.directory, "state.json")

    def tearDown(self):
        shutil.rmtree(self.directory, ignore_errors=True)

    def ingestor(self, engine=None, **kwargs):
        return EventIngestor(
            engine or CacheEngine(max_entries=100),
            AtomicCheckpointStore(self.state_path),
            **kwargs
        )


def _event(event_id="event-1", position=1, **changes):
    values = {
        "event_id": event_id,
        "source": "postgres",
        "stream": "catalog-slot",
        "position": position,
        "operation": "update",
        "payload": {"id": 42, "tenant": "acme"},
        "timestamp": 1_700_000_000,
    }
    values.update(changes)
    return ChangeEvent(**values)


class EventCoreTests(_Artifacts, unittest.TestCase):
    def test_restart_resumes_from_durable_checkpoint_without_skipping(self):
        engine = CacheEngine(max_entries=100)
        engine.put("product:42", "old")
        rule = EventRule("products", key_templates=("product:{payload.id}",))
        first = self.ingestor(engine, rules=(rule,))
        result = first.ingest(_event())
        self.assertEqual("processed", result.outcome)
        self.assertEqual("miss", engine.get("product:42").state)

        engine.put("product:42", "new")
        first.close()
        restarted = self.ingestor(engine, rules=(rule,))
        replay = restarted.ingest(_event())
        self.assertEqual("duplicate", replay.outcome)
        self.assertEqual("fresh", engine.get("product:42").state)
        self.assertEqual(
            1,
            restarted.status()["checkpoints"][_event().checkpoint_key][
                "position"
            ],
        )

    def test_event_metrics_are_exported(self):
        engine = CacheEngine(max_entries=100)
        automation = EventAutomation(
            engine,
            self.ingestor(
                engine,
                rules=(EventRule("products", key_templates=("fixed",)),),
            ),
        )
        automation.ingest_event(_event().as_dict())
        metrics = automation.prometheus_metrics()
        self.assertIn(
            'megacache_events_total{outcome="processed"} 1', metrics
        )
        self.assertIn("megacache_event_dead_letter_depth 0", metrics)

    def test_duplicate_and_out_of_order_events_do_not_reinvalidate(self):
        engine = CacheEngine(max_entries=100)
        engine.put("product:42", "one")
        ingestor = self.ingestor(
            engine,
            rules=(
                EventRule(
                    "products", key_templates=("product:{payload.id}",)
                ),
            ),
        )
        self.assertEqual("processed", ingestor.ingest(_event()).outcome)
        engine.put("product:42", "two")
        self.assertEqual("duplicate", ingestor.ingest(_event()).outcome)
        self.assertEqual(
            "replayed",
            ingestor.ingest(_event("other-id", position=1)).outcome,
        )
        self.assertEqual("fresh", engine.get("product:42").state)

    def test_rules_invalidate_keys_tags_namespaces_and_dependencies(self):
        engine = CacheEngine(max_entries=100)
        for key in (
            "catalog:v1:product:42",
            "catalog:v2:product:42",
            "page:42",
            "other",
        ):
            engine.put(key, key, tags=("tenant:acme",) if key == "other" else ())
        graph = DependencyGraph()
        graph.add_dependency("catalog:v1:product:42", "page:42")
        namespace = VersionedNamespace("catalog", 2, 1, 2, "rolling")
        ingestor = self.ingestor(
            engine,
            graph=graph,
            namespaces=(namespace,),
            rules=(
                EventRule(
                    "products",
                    key_templates=("product:{payload.id}",),
                    tag_templates=("tenant:{payload.tenant}",),
                    namespace="catalog",
                ),
            ),
        )
        result = ingestor.ingest(_event())
        self.assertEqual(3, result.invalidated_keys)
        self.assertEqual(1, result.invalidated_tags)
        self.assertEqual(1, result.graph_expanded)
        self.assertEqual(0, engine.size())

    def test_dependency_cycles_fanout_and_traversal_are_bounded(self):
        graph = DependencyGraph(
            max_nodes=10,
            max_edges=10,
            max_fanout=1,
            max_depth=1,
            max_invalidation_nodes=2,
        )
        graph.add_dependency("a", "b")
        with self.assertRaisesRegex(DependencyGraphError, "fanout"):
            graph.add_dependency("a", "c")
        with self.assertRaisesRegex(DependencyGraphError, "cycle"):
            graph.add_dependency("b", "a")
        graph.add_dependency("b", "c")
        with self.assertRaisesRegex(DependencyGraphError, "depth"):
            graph.expand(("a",))

    def test_schema_ranges_and_explicit_migrations(self):
        engine = CacheEngine(max_entries=100)
        engine.put("product:42", "old")
        schemas = SchemaRegistry((ReaderRange("product", 2, 3),))
        schemas.register_migration(
            "product",
            1,
            2,
            lambda payload: {"product_id": payload["id"]},
        )
        ingestor = self.ingestor(
            engine,
            schemas=schemas,
            rules=(
                EventRule(
                    "products",
                    key_templates=("product:{payload.product_id}",),
                    schema_names=("product",),
                ),
            ),
        )
        result = ingestor.ingest(_event(schema_id="product@1"))
        self.assertEqual("processed", result.outcome)
        self.assertEqual("miss", engine.get("product:42").state)

        incompatible = ingestor.ingest(
            _event("event-2", 2, schema_id="unknown@1")
        )
        self.assertEqual("dead_letter", incompatible.outcome)
        self.assertEqual(1, ingestor.status()["dead_letter_depth"])

    def test_dead_letter_queue_is_bounded_and_tracks_retries(self):
        ingestor = EventIngestor(
            CacheEngine(max_entries=100),
            AtomicCheckpointStore(self.state_path, max_dead_letters=1),
            rules=(
                EventRule(
                    "broken", key_templates=("key:{payload.missing}",)
                ),
            ),
        )
        ingestor.ingest(_event("event-1", 1))
        with self.assertRaises(EventBackpressure):
            ingestor.ingest(_event("event-2", 2))
        letters = ingestor.checkpoints.dead_letters()
        self.assertEqual(1, len(letters))
        self.assertEqual("event-1", letters[0]["event"]["event_id"])
        self.assertEqual(1, letters[0]["attempts"])
        self.assertEqual(0, ingestor.checkpoints.snapshot()["dead_letter_dropped"])
        self.assertEqual(
            1,
            ingestor.checkpoints.checkpoint(_event().checkpoint_key)["position"],
        )
        retried = ingestor.retry_dead_letters(now=time.time() + 2)
        self.assertEqual({"retried": 1, "succeeded": 0, "failed": 1}, retried)
        self.assertEqual(2, ingestor.checkpoints.dead_letters()[0]["attempts"])

    def test_webhook_signature_timestamp_and_replay_are_enforced(self):
        store = AtomicCheckpointStore(self.state_path)
        now = int(time.time())
        auth = WebhookAuthenticator(
            store,
            (WebhookSource("catalog", b"x" * 32, 300),),
            clock=lambda: now,
        )
        body = b'{"event_id":"delivery-1"}'
        timestamp = str(now)
        signature = "sha256=" + hmac.new(
            b"x" * 32,
            webhook_signature_payload(
                "catalog", timestamp, "delivery-1", body
            ),
            hashlib.sha256,
        ).hexdigest()
        headers = {
            "X-MegaCache-Timestamp": timestamp,
            "X-MegaCache-Signature": signature,
            "X-MegaCache-Delivery": "delivery-1",
        }
        auth.authenticate("catalog", headers, body)
        store.close()
        restarted = WebhookAuthenticator(
            AtomicCheckpointStore(self.state_path),
            (WebhookSource("catalog", b"x" * 32, 300),),
            clock=lambda: now,
        )
        with self.assertRaisesRegex(WebhookAuthError, "already"):
            restarted.authenticate("catalog", headers, body)
        with self.assertRaisesRegex(WebhookAuthError, "signature"):
            auth.authenticate(
                "catalog",
                dict(headers, **{"X-MegaCache-Delivery": "delivery-2",
                                 "X-MegaCache-Signature": "sha256=bad"}),
                body,
            )

    def test_webhook_hmac_is_framed_and_live_claims_apply_backpressure(self):
        clock = [1_700_000_000]
        store = AtomicCheckpointStore(
            self.state_path, max_replay_tokens=1
        )
        auth = WebhookAuthenticator(
            store,
            (
                WebhookSource("a", b"x" * 32, 300),
                WebhookSource("a:b", b"y" * 32, 300),
            ),
            clock=lambda: clock[0],
        )

        def headers(source, delivery, secret):
            timestamp = str(clock[0])
            body = b'{"value":"exact"}'
            signature = "sha256=" + hmac.new(
                secret,
                webhook_signature_payload(
                    source, timestamp, delivery, body
                ),
                hashlib.sha256,
            ).hexdigest()
            return body, {
                "X-MegaCache-Timestamp": timestamp,
                "X-MegaCache-Delivery": delivery,
                "X-MegaCache-Signature": signature,
            }

        body, first = headers("a", "b:c", b"x" * 32)
        auth.authenticate("a", first, body)
        with self.assertRaisesRegex(WebhookAuthError, "signature"):
            auth.authenticate(
                "a",
                dict(first, **{"X-MegaCache-Delivery": "different"}),
                body,
            )
        second_body, second = headers("a:b", "c", b"y" * 32)
        with self.assertRaises(EventBackpressure):
            auth.authenticate("a:b", second, second_body)
        clock[0] += 301
        second_body, second = headers("a:b", "c", b"y" * 32)
        auth.authenticate("a:b", second, second_body)

    def test_dead_letters_backpressure_by_bytes_and_lower_restart_limits(self):
        rule = EventRule(
            "broken", key_templates=("key:{payload.missing}",)
        )
        store = AtomicCheckpointStore(
            self.state_path,
            max_dead_letters=2,
            max_dead_letter_bytes=10_000,
        )
        ingestor = EventIngestor(
            CacheEngine(max_entries=100), store, rules=(rule,)
        )
        ingestor.ingest(_event("event-1", 1))
        ingestor.ingest(_event("event-2", 2))
        ingestor.close()
        with self.assertRaisesRegex(EventBackpressure, "existing|count"):
            AtomicCheckpointStore(
                self.state_path,
                max_dead_letters=1,
                max_dead_letter_bytes=10_000,
            )

        byte_path = os.path.join(self.directory, "byte-state.json")
        byte_store = AtomicCheckpointStore(
            byte_path,
            max_dead_letter_bytes=100,
        )
        byte_ingestor = EventIngestor(
            CacheEngine(max_entries=100), byte_store, rules=(rule,)
        )
        with self.assertRaisesRegex(EventBackpressure, "byte"):
            byte_ingestor.ingest(_event())
        self.assertIsNone(
            byte_store.checkpoint(_event().checkpoint_key)
        )
        self.assertEqual(0, len(byte_store.dead_letters()))
        byte_ingestor.close()

        error_path = os.path.join(self.directory, "error-state.json")
        error_store = AtomicCheckpointStore(
            error_path,
            max_error_bytes=20,
        )
        error_ingestor = EventIngestor(
            CacheEngine(max_entries=100), error_store, rules=(rule,)
        )
        error_ingestor.ingest(_event())
        retained_error = error_store.dead_letters()[0]["error"]
        self.assertLessEqual(len(retained_error.encode("utf-8")), 20)
        error_ingestor.close()

    def test_dependency_limit_rejects_before_invalidation_or_checkpoint(self):
        engine = CacheEngine(max_entries=100)
        for key in ("a", "b", "c"):
            engine.put(key, key)
        graph = DependencyGraph(
            max_nodes=10,
            max_edges=10,
            max_fanout=2,
            max_depth=10,
            max_invalidation_nodes=2,
        )
        graph.add_dependency("a", "b")
        graph.add_dependency("b", "c")
        ingestor = self.ingestor(
            engine,
            graph=graph,
            rules=(EventRule("all", key_templates=("a",)),),
        )
        event = _event()
        with self.assertRaisesRegex(DependencyGraphError, "node limit"):
            ingestor.ingest(event)
        self.assertEqual(3, engine.size())
        self.assertIsNone(ingestor.checkpoints.checkpoint(event.checkpoint_key))

    def test_structured_keys_do_not_alias_and_v1_migration_is_explicit(self):
        engine = CacheEngine(max_entries=100)
        ingestor = self.ingestor(
            engine,
            rules=(EventRule("all", key_templates=("fixed",)),),
        )
        first = _event("one", 1, source="a", stream="b:c")
        second = _event("two", 1, source="a:b", stream="c")
        ingestor.ingest(first)
        ingestor.ingest(second)
        checkpoints = ingestor.status()["checkpoints"]
        self.assertEqual(2, len(checkpoints))
        self.assertNotEqual(first.checkpoint_key, second.checkpoint_key)
        ingestor.close()

        legacy_path = os.path.join(self.directory, "legacy.json")
        with open(legacy_path, "w", encoding="utf-8") as destination:
            json.dump(
                {
                    "version": 1,
                    "checkpoints": {
                        "postgres:slot": {
                            "position": 1,
                            "cursor": None,
                            "event_id": "one",
                            "committed_at": 1,
                        }
                    },
                    "seen": ["one"],
                    "replay_tokens": [],
                    "dead_letters": [],
                    "dead_letter_dropped": 0,
                },
                destination,
            )
        migrated = AtomicCheckpointStore(legacy_path)
        self.assertEqual(2, migrated.snapshot()["state_version"])
        self.assertIsNotNone(
            migrated.checkpoint(
                _event(source="postgres", stream="slot").checkpoint_key
            )
        )
        migrated.close()
        with open(legacy_path, "r", encoding="utf-8") as source:
            self.assertEqual(2, json.load(source)["version"])

        ambiguous_path = os.path.join(self.directory, "ambiguous.json")
        with open(ambiguous_path, "w", encoding="utf-8") as destination:
            json.dump(
                {
                    "version": 1,
                    "checkpoints": {"a:b:c": {}},
                    "seen": [],
                    "replay_tokens": [],
                    "dead_letters": [],
                },
                destination,
            )
        with self.assertRaisesRegex(EventError, "ambiguous checkpoint"):
            AtomicCheckpointStore(ambiguous_path)

    def test_state_ownership_stream_and_event_size_limits_precede_mutation(self):
        store = AtomicCheckpointStore(
            self.state_path,
            max_streams=1,
            max_payload_bytes=100,
            max_cursor_bytes=3,
        )
        with self.assertRaisesRegex(EventError, "already owned"):
            AtomicCheckpointStore(self.state_path)
        engine = CacheEngine(max_entries=100)
        rule = EventRule("all", key_templates=("fixed",))
        ingestor = EventIngestor(engine, store, rules=(rule,))
        ingestor.ingest(_event("one", 1, source="a", stream="one"))
        engine.put("fixed", "still-present")
        with self.assertRaisesRegex(EventBackpressure, "stream"):
            ingestor.ingest(
                _event("two", 1, source="a", stream="two")
            )
        self.assertEqual("fresh", engine.get("fixed").state)
        with self.assertRaisesRegex(EventError, "payload"):
            ingestor.ingest(
                _event(
                    "three",
                    2,
                    source="a",
                    stream="one",
                    payload={"value": "x" * 100},
                )
            )
        with self.assertRaisesRegex(EventError, "cursor"):
            ingestor.ingest(
                _event(
                    "four",
                    2,
                    source="a",
                    stream="one",
                    cursor="long",
                )
            )
        ingestor.close()
        reopened = AtomicCheckpointStore(self.state_path)
        reopened.close()

        state_path = os.path.join(self.directory, "small-state.json")
        state_store = AtomicCheckpointStore(
            state_path, max_state_bytes=180
        )
        state_engine = CacheEngine(max_entries=100)
        state_engine.put("fixed", "still-present")
        state_ingestor = EventIngestor(
            state_engine, state_store, rules=(rule,)
        )
        with self.assertRaisesRegex(EventBackpressure, "state"):
            state_ingestor.ingest(_event())
        self.assertEqual("fresh", state_engine.get("fixed").state)
        self.assertIsNone(
            state_store.checkpoint(_event().checkpoint_key)
        )
        state_ingestor.close()

    def test_no_rules_disable_ingestion_without_checkpointing(self):
        ingestor = self.ingestor(CacheEngine(max_entries=100), rules=())
        event = _event()
        with self.assertRaises(EventIngestionDisabled):
            ingestor.ingest(event)
        self.assertFalse(ingestor.status()["enabled"])
        self.assertIsNone(ingestor.checkpoints.checkpoint(event.checkpoint_key))

    def test_example_configuration_loads_without_runtime_dependencies(self):
        with patch.dict(
            os.environ,
            {"MEGACACHE_CATALOG_WEBHOOK_SECRET": "x" * 32},
        ):
            automation = load_event_automation(
                CacheEngine(max_entries=100),
                os.path.join(os.getcwd(), "events.example.json"),
                self.state_path,
            )
        status = automation.event_status()
        self.assertEqual(1, status["rules"])
        self.assertEqual(("catalog",), status["webhooks"])


class AdapterTests(_Artifacts, unittest.TestCase):
    def setUp(self):
        super().setUp()
        self.engine = CacheEngine(max_entries=100)
        self.rule = EventRule(
            "products", key_templates=("product:{payload.id}",)
        )
        self.ingestor_instance = self.ingestor(
            self.engine, rules=(self.rule,)
        )

    def _put(self):
        self.engine.put("product:42", "old")

    def test_kafka_style_consumer_is_external_and_commits_next_offsets(self):
        class Consumer:
            def __init__(self):
                self.committed = None

            def poll(self, max_records, timeout_seconds):
                return [
                    KafkaRecord(
                        "changes",
                        0,
                        4,
                        json.dumps(
                            {
                                "operation": "update",
                                "payload": {"id": 42},
                            }
                        ).encode("utf-8"),
                    )
                ]

            def commit(self, offsets):
                self.committed = dict(offsets)

        self._put()
        consumer = Consumer()
        results = KafkaRecordAdapter(
            self.ingestor_instance
        ).consume_once(consumer)
        self.assertEqual("processed", results[0].outcome)
        self.assertEqual({("changes", 0): 5}, consumer.committed)

    def test_database_adapters_accept_external_driver_records(self):
        adapters_and_records = (
            (
                PostgresLogicalAdapter(self.ingestor_instance),
                PostgresLogicalRecord(
                    "slot",
                    "0/10",
                    "products",
                    "update",
                    {"id": 42},
                    ordinal=0,
                ),
            ),
            (
                MySQLBinlogAdapter(self.ingestor_instance),
                MySQLBinlogRecord(
                    "server",
                    17,
                    "mysql-bin.000001",
                    99,
                    "products",
                    "update",
                    {"id": 42},
                ),
            ),
            (
                MongoChangeStreamAdapter(self.ingestor_instance),
                MongoChangeRecord(
                    "catalog",
                    "products",
                    18,
                    "resume-token",
                    "update",
                    {"id": 42},
                ),
            ),
        )
        for index, (adapter, record) in enumerate(adapters_and_records):
            with self.subTest(adapter=adapter.__class__.__name__):
                self._put()
                result = adapter.feed((record,))[0]
                self.assertEqual("processed", result.outcome)
                self.assertEqual("miss", self.engine.get("product:42").state)

    def test_postgres_same_lsn_changes_use_stable_ordinals(self):
        adapter = PostgresLogicalAdapter(self.ingestor_instance)
        records = (
            PostgresLogicalRecord(
                "slot",
                "0/10",
                "products",
                "update",
                {"id": 42},
                ordinal=0,
            ),
            PostgresLogicalRecord(
                "slot",
                "0/10",
                "products",
                "update",
                {"id": 42},
                ordinal=1,
            ),
        )
        results = adapter.feed(records)
        self.assertEqual(("processed", "processed"), tuple(
            result.outcome for result in results
        ))
        self.assertNotEqual(results[0].event_id, results[1].event_id)
        checkpoint = self.ingestor_instance.checkpoints.checkpoint(
            ChangeEvent(
                "id",
                "postgres",
                "slot",
                0,
                "update",
                {},
                0,
            ).checkpoint_key
        )
        self.assertEqual(
            (parse_postgres_lsn("0/10") << 32) | 1,
            checkpoint["position"],
        )
        with self.assertRaisesRegex(EventError, "ordinal"):
            adapter.feed(
                (
                    PostgresLogicalRecord(
                        "slot",
                        "0/11",
                        "products",
                        "update",
                        {"id": 42},
                    ),
                )
            )
        with self.assertRaisesRegex(EventError, "strictly ordered"):
            adapter.feed(tuple(reversed(records)))


class ProtocolEventTests(_Artifacts, unittest.TestCase):
    def setUp(self):
        super().setUp()
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
            event_state_file=self.state_path,
        )
        storage = CacheEngine(max_entries=100)
        store = AtomicCheckpointStore(self.state_path)
        ingestor = EventIngestor(
            storage,
            store,
            rules=(
                EventRule(
                    "products", key_templates=("product:{payload.id}",)
                ),
            ),
        )
        webhook = WebhookAuthenticator(
            store, (WebhookSource("catalog", b"s" * 32),)
        )
        self.engine = EventAutomation(storage, ingestor, webhook)
        self.http = MegaCacheServer(("127.0.0.1", 0), config, self.engine)
        self.resp = MegaCacheRespServer(("127.0.0.1", 0), config, self.engine)
        self.http_thread = threading.Thread(
            target=self.http.serve_forever, daemon=True
        )
        self.resp_thread = threading.Thread(
            target=self.resp.serve_forever, daemon=True
        )
        self.http_thread.start()
        self.resp_thread.start()

    def tearDown(self):
        self.http.shutdown()
        self.resp.shutdown()
        self.http.server_close()
        self.resp.server_close()
        self.http_thread.join(timeout=1)
        self.resp_thread.join(timeout=1)
        self.engine.close()
        super().tearDown()

    @staticmethod
    def event(event_id, position):
        return {
            "event_id": event_id,
            "source": "postgres",
            "stream": "catalog-slot",
            "position": position,
            "operation": "update",
            "payload": {"id": 42},
        }

    def http_request(self, method, path, value, headers=None):
        connection = HTTPConnection(
            "127.0.0.1", self.http.server_address[1], timeout=2
        )
        body = None if value is None else json.dumps(value).encode("utf-8")
        supplied = {}
        if body is not None:
            supplied.update(
                {
                    "Content-Type": "application/json",
                    "Content-Length": str(len(body)),
                }
            )
        if headers:
            supplied.update(headers)
        connection.request(method, path, body=body, headers=supplied)
        response = connection.getresponse()
        payload = json.loads(response.read())
        connection.close()
        return response.status, payload

    def test_http_event_status_and_authenticated_webhook(self):
        self.engine.put("product:42", "old")
        status, result = self.http_request(
            "POST",
            "/v1/events",
            self.event("http-1", 1),
            {"Authorization": "Bearer secret"},
        )
        self.assertEqual(200, status)
        self.assertEqual("processed", result["outcome"])

        status, event_status = self.http_request(
            "GET",
            "/v1/events/status",
            None,
            {"Authorization": "Bearer secret"},
        )
        self.assertEqual(200, status)
        self.assertEqual(1, event_status["rules"])

        self.engine.put("product:42", "new")
        event = self.event("webhook-2", 2)
        event["source"] = "catalog"
        body = json.dumps(event).encode("utf-8")
        timestamp = str(int(time.time()))
        signature = "sha256=" + hmac.new(
            b"s" * 32,
            webhook_signature_payload(
                "catalog", timestamp, "delivery-2", body
            ),
            hashlib.sha256,
        ).hexdigest()
        headers = {
            "X-MegaCache-Timestamp": timestamp,
            "X-MegaCache-Signature": signature,
            "X-MegaCache-Delivery": "delivery-2",
        }
        status, result = self.http_request(
            "POST", "/v1/events/webhook/catalog", event, headers
        )
        self.assertEqual(200, status)
        self.assertEqual("processed", result["outcome"])
        status, result = self.http_request(
            "POST", "/v1/events/webhook/catalog", event, headers
        )
        self.assertEqual(401, status)
        self.assertEqual("invalid_webhook", result["error"])

    def test_resp_event_and_status_commands(self):
        self.engine.put("product:42", "old")
        sock = socket.create_connection(
            ("127.0.0.1", self.resp.server_address[1]), timeout=2
        )
        stream = sock.makefile("rwb")

        def command(*parts):
            encoded = [
                value
                if isinstance(value, bytes)
                else str(value).encode("utf-8")
                for value in parts
            ]
            payload = b"*" + str(len(encoded)).encode("ascii") + b"\r\n"
            payload += b"".join(
                b"$"
                + str(len(value)).encode("ascii")
                + b"\r\n"
                + value
                + b"\r\n"
                for value in encoded
            )
            stream.write(payload)
            stream.flush()
            marker = stream.read(1)
            line = stream.readline()[:-2]
            if marker == b"+":
                return line
            if marker == b"$":
                value = stream.read(int(line))
                stream.read(2)
                return value
            raise AssertionError((marker, line))

        self.assertEqual(b"OK", command("AUTH", "secret"))
        result = json.loads(
            command(
                "MC.EVENT",
                json.dumps(self.event("resp-1", 1)),
            )
        )
        self.assertEqual("processed", result["outcome"])
        status = json.loads(command("MC.EVENT.STATUS"))
        self.assertEqual(1, status["rules"])
        stream.close()
        sock.close()

    def test_http_event_endpoints_report_disabled_rules(self):
        disabled = EventAutomation(
            self.engine.storage,
            EventIngestor(
                self.engine.storage,
                self.engine.ingestor.checkpoints,
                rules=(),
            ),
        )
        self.http.engine = disabled
        status, result = self.http_request(
            "POST",
            "/v1/events",
            self.event("disabled", 1),
            {"Authorization": "Bearer " + self.http.config.api_key},
        )
        self.assertEqual(503, status)
        self.assertEqual("events_disabled", result["error"])
        status, result = self.http_request(
            "GET",
            "/v1/events/status",
            None,
            {"Authorization": "Bearer " + self.http.config.api_key},
        )
        self.assertEqual(200, status)
        self.assertFalse(result["enabled"])

    def test_native_cli_event_commands(self):
        path = os.path.join(self.directory, "event.json")
        with open(path, "w", encoding="utf-8") as destination:
            json.dump(self.event("cli-1", 1), destination)
        connection = [
            "--port",
            str(self.resp.server_address[1]),
            "--password",
            "secret",
            "--json",
        ]
        output = io.StringIO()
        with redirect_stdout(output):
            code = run(connection + ["event", path])
        self.assertEqual(0, code)
        self.assertEqual("processed", json.loads(output.getvalue())["outcome"])

        output = io.StringIO()
        with redirect_stdout(output):
            code = run(connection + ["events-status"])
        self.assertEqual(0, code)
        self.assertEqual(1, json.loads(output.getvalue())["rules"])


if __name__ == "__main__":
    unittest.main()
