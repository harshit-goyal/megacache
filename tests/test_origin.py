import json
import queue
import threading
import time
import unittest
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

from megacache import (
    BreakerState,
    CacheEngine,
    CircuitOpen,
    ClusterNode,
    ClusterStorage,
    HTTPOrigin,
    OriginCache,
    OriginOverloaded,
    OriginPolicyError,
    OriginResponse,
    OriginUnavailable,
    load_origin_definitions,
    validate_origin_path,
)
from megacache.intelligence import CacheIntelligence
from megacache.origin import _AgingPriorityQueue


class FakeClock:
    def __init__(self):
        self.value = 100.0
        self.lock = threading.Lock()

    def __call__(self):
        with self.lock:
            return self.value

    def advance(self, seconds):
        with self.lock:
            self.value += seconds


def make_origin(**overrides):
    value = {
        "name": "catalog",
        "base_url": "https://origin.example",
        "allowed_hosts": ["origin.example"],
        "allowed_ports": [443],
        "allowed_path_prefixes": ["/v1/"],
        "ttl_seconds": 10,
        "stale_while_revalidate_seconds": 5,
        "stale_if_error_seconds": 10,
        "refresh_ahead_seconds": 2,
        "negative_ttl_seconds": 4,
        "timeout_seconds": 1,
        "queue_timeout_seconds": 0.2,
        "max_concurrency": 4,
        "max_queue": 8,
        "retry_attempts": 0,
        "breaker_failure_threshold": 10,
    }
    value.update(overrides)
    return HTTPOrigin.from_dict(value)


class OriginTests(unittest.TestCase):
    def setUp(self):
        self.services = []
        self.clock = FakeClock()

    def tearDown(self):
        for service in self.services:
            service.close(1)

    def service(self, storage, origin, transport, **kwargs):
        service = OriginCache(
            storage,
            [origin],
            clock=self.clock,
            resolver=lambda host, port: ["93.184.216.34"],
            transport=transport,
            **kwargs
        )
        self.services.append(service)
        return service

    def test_example_origin_file_is_valid(self):
        origins = load_origin_definitions("origins.example.json")
        self.assertEqual(("catalog",), tuple(item.name for item in origins))

    def test_traceparent_is_forwarded_to_header_aware_transport(self):
        captured = []

        def transport(origin, path, addresses, headers):
            captured.append(headers)
            return OriginResponse(200, b"value")

        service = self.service(CacheEngine(clock=self.clock), make_origin(), transport)
        traceparent = (
            "00-4bf92f3577b34da6a3ce929d0e0e4736-"
            "00f067aa0ba902b7-01"
        )
        service.fetch(
            "product:trace",
            "catalog",
            "/v1/product/trace",
            traceparent=traceparent,
        )
        self.assertEqual(traceparent, captured[0]["traceparent"])

    def test_concurrent_miss_is_coalesced_across_one_cluster_coordinator(self):
        cluster = ClusterStorage(
            [
                ClusterNode("node-a", CacheEngine(clock=self.clock)),
                ClusterNode("node-b", CacheEngine(clock=self.clock)),
            ],
            replica_count=2,
            consistency="all",
            clock=self.clock,
        )
        started = threading.Event()
        release = threading.Event()
        calls = []

        def transport(origin, path, addresses):
            calls.append(path)
            started.set()
            release.wait(1)
            return OriginResponse(200, b"value")

        first = self.service(cluster, make_origin(), transport)
        second = self.service(cluster, make_origin(), transport)
        results = []

        threads = [
            threading.Thread(
                target=lambda service=service: results.append(
                    service.fetch("product:1", "catalog", "/v1/product/1")
                )
            )
            for service in (first, second)
        ]
        threads[0].start()
        self.assertTrue(started.wait(1))
        threads[1].start()
        time.sleep(0.02)
        release.set()
        for thread in threads:
            thread.join(1)

        self.assertEqual(1, len(calls))
        self.assertEqual([b"value", b"value"], [item.value for item in results])
        self.assertGreaterEqual(
            second.stats()["origin_coalesced_total"], 1
        )

    def test_stale_windows_refresh_in_background_and_serve_on_error(self):
        refreshed = threading.Event()
        responses = [OriginResponse(200, b"old"), OriginResponse(200, b"new")]

        def transport(origin, path, addresses):
            response = responses.pop(0)
            if response.body == b"new":
                refreshed.set()
            return response

        service = self.service(CacheEngine(clock=self.clock), make_origin(), transport)
        self.assertEqual(
            b"old", service.fetch("key", "catalog", "/v1/key").value
        )
        self.clock.advance(11)
        stale = service.fetch("key", "catalog", "/v1/key")
        self.assertEqual("stale", stale.state)
        self.assertTrue(refreshed.wait(1))
        self.assertEqual(b"new", service.get("key").value)
        deadline = time.time() + 1
        while service._scheduled and time.time() < deadline:
            time.sleep(0.005)

        self.clock.advance(16)

        def failing(origin, path, addresses):
            raise OSError("down")

        service._transport = failing
        fallback = service.fetch("key", "catalog", "/v1/key")
        self.assertEqual("stale_if_error", fallback.state)
        self.assertEqual(b"new", fallback.value)
        self.clock.advance(10)
        with self.assertRaises(OriginUnavailable):
            service.fetch("key", "catalog", "/v1/key")

    def test_stale_if_error_reloads_entry_after_failed_origin_request(self):
        responses = [OriginResponse(200, b"value")]

        def transport(origin, path, addresses):
            if responses:
                return responses.pop()
            self.clock.advance(2)
            raise OSError("down")

        service = self.service(
            CacheEngine(clock=self.clock),
            make_origin(
                ttl_seconds=1,
                stale_while_revalidate_seconds=0,
                stale_if_error_seconds=1,
                refresh_ahead_seconds=0,
            ),
            transport,
        )
        service.fetch("key", "catalog", "/v1/key")
        self.clock.advance(1.1)
        with self.assertRaises(OriginUnavailable):
            service.fetch("key", "catalog", "/v1/key")

    def test_refresh_ahead_schedules_a_worker(self):
        refreshed = threading.Event()
        responses = [OriginResponse(200, b"one"), OriginResponse(200, b"two")]

        def transport(origin, path, addresses):
            response = responses.pop(0)
            if response.body == b"two":
                refreshed.set()
            return response

        service = self.service(CacheEngine(clock=self.clock), make_origin(), transport)
        service.fetch("key", "catalog", "/v1/key")
        self.clock.advance(8)
        current = service.fetch("key", "catalog", "/v1/key")
        self.assertEqual("fresh", current.state)
        self.assertTrue(refreshed.wait(1))
        self.assertEqual(b"two", service.get("key").value)

    def test_negative_responses_are_cached_without_becoming_get_values(self):
        responses = [OriginResponse(404, b""), OriginResponse(200, b"found")]
        calls = []

        def transport(origin, path, addresses):
            calls.append(1)
            return responses.pop(0)

        service = self.service(CacheEngine(clock=self.clock), make_origin(), transport)
        first = service.fetch("missing", "catalog", "/v1/missing")
        second = service.fetch("missing", "catalog", "/v1/missing")
        self.assertEqual("negative", first.state)
        self.assertEqual(404, second.status_code)
        self.assertEqual("miss", service.get("missing").state)
        self.assertEqual(1, len(calls))

        self.clock.advance(5)
        found = service.fetch("missing", "catalog", "/v1/missing")
        self.assertEqual(b"found", found.value)
        self.assertEqual(2, len(calls))

    def test_origin_cache_lineage_prevents_path_aliasing(self):
        calls = []

        def transport(origin, path, addresses):
            calls.append(path)
            return OriginResponse(200, path.encode("ascii"))

        service = self.service(CacheEngine(clock=self.clock), make_origin(), transport)
        first = service.fetch("shared", "catalog", "/v1/first")
        second = service.fetch("shared", "catalog", "/v1/second")
        self.assertEqual(b"/v1/first", first.value)
        self.assertEqual(b"/v1/second", second.value)
        self.assertEqual(["/v1/first", "/v1/second"], calls)
        self.assertEqual(b"/v1/second", service.get("shared").value)

    def test_origin_lineage_omits_query_while_request_and_identity_keep_it(self):
        storage = CacheEngine(clock=self.clock)
        intelligence = CacheIntelligence(storage, enabled=True)
        calls = []

        def transport(origin, path, addresses):
            calls.append(path)
            return OriginResponse(200, path.encode("ascii"))

        service = self.service(intelligence, make_origin(), transport)
        first_path = "/v1/item?token=top-secret"
        second_path = "/v1/item?token=different"
        service.fetch("shared", "catalog", first_path)
        raw = storage.export_entries(("shared",))[0].value
        explanation = intelligence.explain("shared")

        self.assertEqual([first_path], calls)
        self.assertEqual("/v1/item", raw["path"])
        self.assertNotIn("top-secret", json.dumps(raw))
        self.assertEqual(
            "/v1/item", explanation["current"]["lineage"]["path"]
        )
        self.assertNotIn("top-secret", json.dumps(explanation))

        service.fetch("shared", "catalog", second_path)
        self.assertEqual([first_path, second_path], calls)

    def test_refresh_queue_promotes_old_jobs_under_sustained_high_priority(self):
        refresh_queue = _AgingPriorityQueue(2, self.clock, 1.0)
        low = ("low", "catalog", "/v1/low")
        refresh_queue.put_nowait((900, 1, self.clock(), low))
        self.clock.advance(0.5)
        refresh_queue.put_nowait(
            (0, 2, self.clock(), ("high-1", "catalog", "/v1/high-1"))
        )
        self.assertEqual("high-1", refresh_queue.get_nowait()[3][0])
        refresh_queue.task_done()

        self.clock.advance(0.6)
        refresh_queue.put_nowait(
            (0, 3, self.clock(), ("high-2", "catalog", "/v1/high-2"))
        )
        self.assertEqual(low, refresh_queue.get_nowait()[3])
        refresh_queue.task_done()
        refresh_queue.put_nowait(
            (0, 4, self.clock(), ("high-3", "catalog", "/v1/high-3"))
        )
        with self.assertRaises(queue.Full):
            refresh_queue.put_nowait(
                (0, 5, self.clock(), ("overflow", "catalog", "/v1/overflow"))
            )

    def test_delete_cancels_refresh_ownership_without_resurrection(self):
        started = threading.Event()
        release = threading.Event()
        errors = []

        def transport(origin, path, addresses):
            started.set()
            release.wait(1)
            return OriginResponse(200, b"late")

        service = self.service(CacheEngine(clock=self.clock), make_origin(), transport)

        def fetch():
            try:
                service.fetch("key", "catalog", "/v1/key")
            except Exception as exc:
                errors.append(exc)

        thread = threading.Thread(target=fetch)
        thread.start()
        self.assertTrue(started.wait(1))
        service.delete("key")
        release.set()
        thread.join(1)

        self.assertEqual("miss", service.get("key").state)
        self.assertEqual(1, len(errors))
        self.assertRegex(str(errors[0]), "lease ownership was lost")

    def test_tag_invalidation_cancels_in_flight_refresh(self):
        origin = make_origin(tags=["products"])
        responses = [OriginResponse(200, b"old")]
        started = threading.Event()
        release = threading.Event()

        def transport(origin, path, addresses):
            if responses:
                return responses.pop(0)
            started.set()
            release.wait(1)
            return OriginResponse(200, b"late")

        service = self.service(CacheEngine(clock=self.clock), origin, transport)
        service.fetch("key", "catalog", "/v1/key")
        errors = []

        def refresh():
            try:
                service.fetch(
                    "key", "catalog", "/v1/key", force_refresh=True
                )
            except Exception as exc:
                errors.append(exc)

        thread = threading.Thread(target=refresh)
        thread.start()
        self.assertTrue(started.wait(1))
        self.assertEqual(1, service.invalidate_tags(["products"]))
        release.set()
        thread.join(1)

        self.assertEqual("miss", service.get("key").state)
        self.assertEqual(1, len(errors))
        self.assertIsInstance(errors[0], OriginUnavailable)

    def test_invalidation_cancels_stale_if_error_fallback(self):
        started = threading.Event()
        release = threading.Event()

        def initial(origin, path, addresses):
            return OriginResponse(200, b"old")

        service = self.service(CacheEngine(clock=self.clock), make_origin(), initial)
        service.fetch("key", "catalog", "/v1/key")
        self.clock.advance(16)

        def failing(origin, path, addresses):
            started.set()
            release.wait(1)
            raise OSError("down")

        service._transport = failing
        errors = []

        def fetch():
            try:
                service.fetch("key", "catalog", "/v1/key")
            except Exception as exc:
                errors.append(exc)

        thread = threading.Thread(target=fetch)
        thread.start()
        self.assertTrue(started.wait(1))
        service.delete("key")
        release.set()
        thread.join(1)

        self.assertEqual("miss", service.get("key").state)
        self.assertEqual(1, len(errors))
        self.assertIsInstance(errors[0], OriginUnavailable)

    def test_breaker_has_closed_open_and_half_open_transitions(self):
        outcomes = [OSError("one"), OSError("two"), OriginResponse(200, b"ok")]

        def transport(origin, path, addresses):
            outcome = outcomes.pop(0)
            if isinstance(outcome, Exception):
                raise outcome
            return outcome

        origin = make_origin(
            breaker_failure_threshold=2,
            breaker_open_seconds=5,
        )
        service = self.service(CacheEngine(clock=self.clock), origin, transport)
        for key in ("one", "two"):
            with self.assertRaises(OriginUnavailable):
                service.fetch(key, "catalog", "/v1/{}".format(key))
        self.assertEqual(
            BreakerState.OPEN.value,
            service.origins()["catalog"]["breaker_state"],
        )
        with self.assertRaises(CircuitOpen):
            service.fetch("three", "catalog", "/v1/three")
        self.assertEqual(1, len(outcomes))

        self.clock.advance(6)
        result = service.fetch("three", "catalog", "/v1/three")
        self.assertEqual(b"ok", result.value)
        self.assertEqual(
            BreakerState.CLOSED.value,
            service.origins()["catalog"]["breaker_state"],
        )

    def test_retry_budget_limits_exponential_retry_attempts(self):
        calls = []
        sleeps = []

        def transport(origin, path, addresses):
            calls.append(1)
            raise OSError("down")

        origin = make_origin(
            retry_attempts=5,
            retry_budget_capacity=2,
            retry_budget_refill_per_second=0.01,
            retry_backoff_seconds=0.1,
            retry_max_backoff_seconds=1,
            retry_jitter=0,
        )
        service = OriginCache(
            CacheEngine(clock=self.clock),
            [origin],
            clock=self.clock,
            sleeper=sleeps.append,
            random_source=lambda: 0.5,
            resolver=lambda host, port: ["93.184.216.34"],
            transport=transport,
        )
        self.services.append(service)
        with self.assertRaises(OriginUnavailable):
            service.fetch("key", "catalog", "/v1/key")
        self.assertEqual(3, len(calls))
        self.assertEqual([0.1, 0.2], sleeps)
        self.assertEqual(2, service.stats()["origin_retry_total"])
        self.assertEqual(
            1, service.stats()["origin_retry_budget_exhausted_total"]
        )

    def test_concurrency_and_queue_limits_shed_excess_load(self):
        origin = make_origin(max_concurrency=1, max_queue=1)
        release = threading.Event()
        active = 0
        maximum_active = 0
        lock = threading.Lock()

        def transport(origin, path, addresses):
            nonlocal active, maximum_active
            with lock:
                active += 1
                maximum_active = max(maximum_active, active)
            release.wait(1)
            with lock:
                active -= 1
            return OriginResponse(200, path.encode("ascii"))

        service = self.service(CacheEngine(clock=self.clock), origin, transport)
        errors = []
        results = []

        def run(key):
            try:
                results.append(service.fetch(key, "catalog", "/v1/" + key))
            except Exception as exc:
                errors.append(exc)

        first = threading.Thread(target=run, args=("one",))
        second = threading.Thread(target=run, args=("two",))
        first.start()
        deadline = time.time() + 1
        while active != 1 and time.time() < deadline:
            time.sleep(0.005)
        second.start()
        deadline = time.time() + 1
        while (
            service.origins()["catalog"]["queue_depth"] != 1
            and time.time() < deadline
        ):
            time.sleep(0.005)
        third = threading.Thread(target=run, args=("three",))
        third.start()
        third.join(1)
        release.set()
        first.join(1)
        second.join(1)

        self.assertEqual(1, maximum_active)
        self.assertEqual(2, len(results))
        self.assertEqual(1, len(errors))
        self.assertIsInstance(errors[0], OriginOverloaded)
        self.assertGreaterEqual(service.stats()["origin_load_shed_total"], 1)

    def test_coalesced_followers_are_bounded_and_time_out(self):
        origin = make_origin(max_queue=1, queue_timeout_seconds=0.05)
        started = threading.Event()
        release = threading.Event()

        def transport(origin, path, addresses):
            started.set()
            release.wait(1)
            return OriginResponse(200, b"value")

        service = self.service(
            CacheEngine(clock=self.clock),
            origin,
            transport,
            global_max_queue=1,
        )
        leader_errors = []

        def leader():
            try:
                service.fetch("key", "catalog", "/v1/key")
            except Exception as exc:
                leader_errors.append(exc)

        thread = threading.Thread(target=leader)
        thread.start()
        self.assertTrue(started.wait(1))

        started_wait = time.monotonic()
        with self.assertRaisesRegex(OriginOverloaded, "wait timed out"):
            service.fetch("key", "catalog", "/v1/key")
        self.assertLess(time.monotonic() - started_wait, 0.5)

        waiting = threading.Thread(
            target=lambda: self._capture_fetch_error(
                service, "key", "/v1/key", leader_errors
            )
        )
        waiting.start()
        time.sleep(0.01)
        with self.assertRaisesRegex(OriginOverloaded, "follower limit"):
            service.fetch("key", "catalog", "/v1/key")
        release.set()
        thread.join(1)
        waiting.join(1)
        self.assertGreaterEqual(service.stats()["origin_load_shed_total"], 2)

    def test_coalesced_followers_share_a_global_bound_across_flights(self):
        origin = make_origin(max_queue=2, queue_timeout_seconds=0.5)
        release = threading.Event()
        started = threading.Condition()
        active = []
        errors = []

        def transport(origin, path, addresses):
            with started:
                active.append(path)
                started.notify_all()
            release.wait(1)
            return OriginResponse(200, path.encode("ascii"))

        service = self.service(
            CacheEngine(clock=self.clock),
            origin,
            transport,
            global_max_queue=1,
        )
        leaders = [
            threading.Thread(
                target=lambda key=key: self._capture_fetch_error(
                    service, key, "/v1/" + key, errors
                )
            )
            for key in ("one", "two")
        ]
        for thread in leaders:
            thread.start()
        with started:
            started.wait_for(lambda: len(active) == 2, timeout=1)
        self.assertEqual(2, len(active))

        follower = threading.Thread(
            target=lambda: self._capture_fetch_error(
                service, "one", "/v1/one", errors
            )
        )
        follower.start()
        deadline = time.time() + 1
        while (
            service.stats()["origin_follower_depth"] != 1
            and time.time() < deadline
        ):
            time.sleep(0.005)
        with self.assertRaisesRegex(OriginOverloaded, "follower limit"):
            service.fetch("two", "catalog", "/v1/two")

        release.set()
        for thread in leaders:
            thread.join(1)
        follower.join(1)
        self.assertEqual([], errors)

    def test_unmarked_and_malformed_values_do_not_satisfy_lineage(self):
        storage = CacheEngine(clock=self.clock)
        calls = []

        def transport(origin, path, addresses):
            calls.append(path)
            return OriginResponse(200, path.encode("ascii"))

        service = self.service(storage, make_origin(), transport)
        values = (
            b"ordinary",
            {
                "$megacache_origin_value_v1": True,
                "origin": "catalog",
                "path": "/v1/two",
                "status": 200,
                "body": "not-base64!",
            },
            {
                "$megacache_origin_negative_v1": True,
                "origin": "catalog",
                "path": "/v1/three",
            },
        )
        for index, value in enumerate(values, 1):
            key = "key:{}".format(index)
            path = "/v1/{}".format(index)
            storage.put(key, value)
            result = service.fetch(key, "catalog", path)
            self.assertEqual(path.encode("ascii"), result.value)
        self.assertEqual(["/v1/1", "/v1/2", "/v1/3"], calls)

    def test_ssrf_policy_requires_exact_authority_path_and_network_allows(self):
        with self.assertRaisesRegex(OriginPolicyError, "allowed_hosts"):
            HTTPOrigin.from_dict(
                {
                    "name": "bad",
                    "base_url": "http://127.0.0.1",
                    "allowed_ports": [80],
                    "allowed_path_prefixes": ["/"],
                }
            )
        origin = make_origin()
        for path in (
            "https://attacker.example/v1/key",
            "//attacker.example/v1/key",
            "/v1/../admin",
            "/v1/%2f%2fattacker.example",
            "/v10/not-v1",
        ):
            with self.subTest(path=path), self.assertRaises(OriginPolicyError):
                validate_origin_path(origin, path)

        blocked = OriginCache(
            CacheEngine(clock=self.clock),
            [origin],
            clock=self.clock,
            resolver=lambda host, port: ["127.0.0.1"],
            transport=lambda *args: OriginResponse(200, b"unsafe"),
        )
        self.services.append(blocked)
        with self.assertRaisesRegex(OriginPolicyError, "non-public"):
            blocked.fetch("key", "catalog", "/v1/key")
        with self.assertRaisesRegex(OriginPolicyError, "arbitrary URLs"):
            blocked.fetch("key", "https://attacker.example", "/v1/key")

        private = make_origin(
            base_url="http://internal.example:8080",
            allowed_hosts=["internal.example"],
            allowed_ports=[8080],
            allowed_ip_networks=["10.0.0.0/8"],
        )
        allowed = OriginCache(
            CacheEngine(clock=self.clock),
            [private],
            clock=self.clock,
            resolver=lambda host, port: ["10.1.2.3"],
            transport=lambda *args: OriginResponse(200, b"internal"),
        )
        self.services.append(allowed)
        self.assertEqual(
            b"internal",
            allowed.fetch("key", "catalog", "/v1/key").value,
        )

    def test_ssrf_policy_blocks_special_and_transition_addresses(self):
        origin = make_origin()
        blocked = (
            "0.0.0.0",
            "169.254.1.1",
            "192.0.2.1",
            "224.0.0.1",
            "::",
            "fe80::1",
            "ff02::1",
            "::ffff:10.1.2.3",
            "::ffff:0:10.1.2.3",
            "64:ff9b::10.1.2.3",
            "64:ff9b:1::10.1.2.3",
            "2002:0a01:0203::",
            "2001:0000:4136:e378:8000:63bf:f5fe:fdfc",
            "2001:4860:4860:0:0:5efe:0a01:0203",
        )
        service = self.service(
            CacheEngine(clock=self.clock),
            origin,
            lambda *args: OriginResponse(200, b"unsafe"),
        )
        for index, address in enumerate(blocked):
            service._resolver = lambda host, port, value=address: [value]
            with self.subTest(address=address), self.assertRaisesRegex(
                OriginPolicyError, "special-use"
            ):
                service.fetch(
                    "blocked:{}".format(index),
                    "catalog",
                    "/v1/blocked/{}".format(index),
                )

        explicitly_allowed = make_origin(
            allowed_ip_networks=["10.0.0.0/8"]
        )
        allowed = self.service(
            CacheEngine(clock=self.clock),
            explicitly_allowed,
            lambda *args: OriginResponse(200, b"mapped"),
        )
        allowed._resolver = lambda host, port: ["::ffff:10.1.2.3"]
        self.assertEqual(
            b"mapped",
            allowed.fetch("mapped", "catalog", "/v1/mapped").value,
        )

    def test_native_transport_enforces_timeout_size_and_no_redirects(self):
        final_requests = []

        class Handler(BaseHTTPRequestHandler):
            def do_GET(self):
                if self.path == "/v1/slow":
                    time.sleep(0.08)
                    body = b"slow"
                    self.send_response(200)
                elif self.path == "/v1/large":
                    body = b"x" * 32
                    self.send_response(200)
                elif self.path == "/v1/redirect":
                    body = b""
                    self.send_response(302)
                    self.send_header("Location", "/v1/final")
                else:
                    final_requests.append(self.path)
                    body = b"final"
                    self.send_response(200)
                self.send_header("Content-Length", str(len(body)))
                self.end_headers()
                try:
                    self.wfile.write(body)
                except OSError:
                    pass

            def log_message(self, message, *args):
                return

        server = ThreadingHTTPServer(("127.0.0.1", 0), Handler)
        thread = threading.Thread(target=server.serve_forever, daemon=True)
        thread.start()
        port = server.server_address[1]
        origin = make_origin(
            base_url="http://127.0.0.1:{}".format(port),
            allowed_hosts=["127.0.0.1"],
            allowed_ports=[port],
            allowed_ip_networks=["127.0.0.0/8"],
            timeout_seconds=0.02,
            max_response_bytes=8,
        )
        service = OriginCache(CacheEngine(clock=self.clock), [origin], clock=self.clock)
        self.services.append(service)
        try:
            with self.assertRaisesRegex(OriginUnavailable, "exceeds"):
                service.fetch("large", "catalog", "/v1/large")
            with self.assertRaisesRegex(OriginUnavailable, "HTTP 302"):
                service.fetch("redirect", "catalog", "/v1/redirect")
            self.assertEqual([], final_requests)
            with self.assertRaises(OriginUnavailable):
                service.fetch("slow", "catalog", "/v1/slow")
        finally:
            server.shutdown()
            server.server_close()
            thread.join(1)

    def test_shutdown_wakes_queued_requests_and_rejects_new_work(self):
        origin = make_origin(max_concurrency=1, max_queue=1)
        release = threading.Event()
        started = threading.Event()
        errors = []

        def transport(origin, path, addresses):
            started.set()
            release.wait(1)
            return OriginResponse(200, b"ok")

        service = self.service(CacheEngine(clock=self.clock), origin, transport)
        first_errors = []
        first = threading.Thread(
            target=lambda: self._capture_fetch_error(
                service, "one", "/v1/one", first_errors
            )
        )

        def queued():
            try:
                service.fetch("two", "catalog", "/v1/two")
            except Exception as exc:
                errors.append(exc)

        second = threading.Thread(target=queued)
        first.start()
        self.assertTrue(started.wait(1))
        second.start()
        deadline = time.time() + 1
        while (
            service.origins()["catalog"]["queue_depth"] != 1
            and time.time() < deadline
        ):
            time.sleep(0.005)
        service.close(0.05)
        second.join(1)
        with self.assertRaisesRegex(OriginUnavailable, "shutting down"):
            service.fetch("three", "catalog", "/v1/three")
        release.set()
        first.join(1)
        self.assertEqual(1, len(errors))
        self.assertIsInstance(errors[0], OriginUnavailable)
        self.assertEqual(1, len(first_errors))
        self.assertIsInstance(first_errors[0], OriginUnavailable)

    def test_shutdown_wakes_coalesced_followers(self):
        origin = make_origin(queue_timeout_seconds=1)
        started = threading.Event()
        release = threading.Event()
        leader_errors = []
        follower_errors = []

        def transport(origin, path, addresses):
            started.set()
            release.wait(1)
            return OriginResponse(200, b"value")

        service = self.service(CacheEngine(clock=self.clock), origin, transport)
        leader = threading.Thread(
            target=lambda: self._capture_fetch_error(
                service, "key", "/v1/key", leader_errors
            )
        )
        follower = threading.Thread(
            target=lambda: self._capture_fetch_error(
                service, "key", "/v1/key", follower_errors
            )
        )
        leader.start()
        self.assertTrue(started.wait(1))
        follower.start()
        time.sleep(0.02)
        service.begin_shutdown()
        follower.join(0.5)
        release.set()
        leader.join(1)

        self.assertEqual(1, len(follower_errors))
        self.assertRegex(str(follower_errors[0]), "shutting down")
        self.assertEqual(1, len(leader_errors))
        self.assertRegex(str(leader_errors[0]), "shutting down")

    def test_long_origin_request_renews_refresh_ownership(self):
        renewed = threading.Event()
        renewals = []
        clock = self.clock

        class TrackingCache(CacheEngine):
            def renew_lease(self, key, lease_token):
                clock.advance(0.75)
                result = super().renew_lease(key, lease_token)
                if result:
                    renewals.append(1)
                    if len(renewals) == 2:
                        renewed.set()
                return result

        storage = TrackingCache(
            clock=self.clock,
            lease_seconds=1,
        )

        def transport(origin, path, addresses):
            self.assertTrue(renewed.wait(1))
            return OriginResponse(200, b"renewed")

        service = self.service(storage, make_origin(), transport)
        result = service.fetch("key", "catalog", "/v1/key")
        self.assertEqual(b"renewed", result.value)
        self.assertGreaterEqual(storage.stats()["lease_renewals_total"], 1)

    @staticmethod
    def _capture_fetch_error(service, key, path, errors):
        try:
            service.fetch(key, "catalog", path)
        except Exception as exc:
            errors.append(exc)


if __name__ == "__main__":
    unittest.main()
