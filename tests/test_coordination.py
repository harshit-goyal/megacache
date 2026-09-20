import unittest

from megacache.coordination import MutationClock
from megacache.engine import CacheEngine


class MutationClockTests(unittest.TestCase):
    def test_successful_mutations_advance_cursor(self):
        storage = MutationClock(CacheEngine(max_entries=10))
        self.assertEqual(0, storage.invalidation_cursor())
        storage.put("key", b"value", tags=("tag",))
        self.assertEqual(1, storage.invalidation_cursor())
        self.assertTrue(storage.expire("key", 10))
        self.assertEqual(2, storage.invalidation_cursor())
        self.assertEqual(1, storage.invalidate_tags(("tag",)))
        self.assertEqual(3, storage.invalidation_cursor())

    def test_noop_mutations_do_not_advance_cursor(self):
        storage = MutationClock(CacheEngine(max_entries=10))
        self.assertFalse(storage.delete("missing"))
        self.assertEqual(0, storage.invalidation_cursor())
        self.assertEqual(0, storage.flush())
        self.assertEqual(0, storage.invalidation_cursor())
