import json
import logging
import unittest

from megacache.engine import CacheEngine
from megacache.observability import JsonFormatter


class ObservabilityTests(unittest.TestCase):
    def test_request_histogram_contains_bounded_labels(self):
        cache = CacheEngine()
        cache.observe_request("resp", "GET", 0.004, True)
        cache.observe_request("resp", "GET", 0.02, False)
        metrics = cache.prometheus_metrics()
        self.assertIn(
            'megacache_requests_total{protocol="resp",operation="GET",status="success"} 1',
            metrics,
        )
        self.assertIn(
            'megacache_request_duration_seconds_bucket{protocol="resp",operation="GET",le="0.025"} 2',
            metrics,
        )
        self.assertIn(
            'megacache_request_duration_seconds_count{protocol="resp",operation="GET"} 2',
            metrics,
        )

    def test_json_formatter_emits_structured_fields(self):
        record = logging.LogRecord(
            "megacache",
            logging.INFO,
            __file__,
            1,
            "request",
            (),
            None,
        )
        record.protocol = "http"
        record.operation = "GET /healthz"
        record.status = 200
        record.traceparent = (
            "00-4bf92f3577b34da6a3ce929d0e0e4736-"
            "00f067aa0ba902b7-01"
        )
        record.event_source = "catalog"
        record.event_outcome = "processed"
        record.experiment_id = "ttl-v1"
        record.reason = "guardrail"
        document = json.loads(JsonFormatter().format(record))
        self.assertEqual("request", document["message"])
        self.assertEqual("http", document["protocol"])
        self.assertEqual(200, document["status"])
        self.assertEqual(record.traceparent, document["traceparent"])
        self.assertEqual("catalog", document["event_source"])
        self.assertEqual("processed", document["event_outcome"])
        self.assertEqual("ttl-v1", document["experiment_id"])
        self.assertEqual("guardrail", document["reason"])


if __name__ == "__main__":
    unittest.main()
