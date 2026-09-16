import os
import unittest
from unittest.mock import patch

from megacache.config import Config


class ConfigTests(unittest.TestCase):
    def test_cluster_environment_is_parsed(self):
        with patch.dict(
            os.environ,
            {
                "MEGACACHE_NODE_ID": "node-a",
                "MEGACACHE_CLUSTER_NODES": "node-a,node-b,node-c",
                "MEGACACHE_REPLICA_COUNT": "3",
                "MEGACACHE_CONSISTENCY": "all",
            },
            clear=True,
        ):
            config = Config.from_env()
        self.assertEqual(("node-a", "node-b", "node-c"), config.cluster_nodes)
        self.assertEqual(3, config.replica_count)
        self.assertEqual("all", config.consistency)

    def test_cluster_list_must_include_local_identity(self):
        with patch.dict(
            os.environ,
            {
                "MEGACACHE_NODE_ID": "node-a",
                "MEGACACHE_CLUSTER_NODES": "node-b,node-c",
            },
            clear=True,
        ):
            with self.assertRaisesRegex(ValueError, "must include"):
                Config.from_env()

    def test_heartbeat_interval_must_be_below_timeout(self):
        with patch.dict(
            os.environ,
            {
                "MEGACACHE_NODE_ID": "node-a",
                "MEGACACHE_HEARTBEAT_INTERVAL_SECONDS": "10",
                "MEGACACHE_HEARTBEAT_TIMEOUT_SECONDS": "10",
            },
            clear=True,
        ):
            with self.assertRaisesRegex(ValueError, "less than"):
                Config.from_env()

    def test_heartbeat_durations_must_be_finite(self):
        for name, value in (
            ("MEGACACHE_HEARTBEAT_INTERVAL_SECONDS", "nan"),
            ("MEGACACHE_HEARTBEAT_INTERVAL_SECONDS", "inf"),
            ("MEGACACHE_HEARTBEAT_TIMEOUT_SECONDS", "-inf"),
            ("MEGACACHE_HEARTBEAT_TIMEOUT_SECONDS", "infinity"),
        ):
            with self.subTest(name=name, value=value), patch.dict(
                os.environ,
                {"MEGACACHE_NODE_ID": "node-a", name: value},
                clear=True,
            ):
                with self.assertRaisesRegex(ValueError, "finite"):
                    Config.from_env()


if __name__ == "__main__":
    unittest.main()
