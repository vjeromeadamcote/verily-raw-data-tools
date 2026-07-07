"""Tests for DataSourceCache stub.

Per amendment 4: the stub must raise NotImplementedError on all queries,
never silently return wrong timezones.
"""

import unittest

from verily.raw_data_tools.utils.data_source_cache import DataSourceCache


class DataSourceCacheStubTest(unittest.TestCase):

    def test_constructor_without_redis(self):
        cache = DataSourceCache()
        self.assertEqual({}, cache.data_source_mappings)

    def test_constructor_with_mappings(self):
        cache = DataSourceCache(data_source_mappings={1: 'value'})
        self.assertEqual({1: 'value'}, cache.data_source_mappings)

    def test_redis_rejected(self):
        with self.assertRaises(NotImplementedError):
            DataSourceCache(redis_end_point='redis://localhost:6379')

    def test_get_data_source_raises(self):
        cache = DataSourceCache()
        with self.assertRaises(NotImplementedError):
            cache.get_data_source(1)

    def test_get_raises(self):
        cache = DataSourceCache()
        with self.assertRaises(NotImplementedError):
            cache.get(1)

    def test_getitem_raises(self):
        cache = DataSourceCache()
        with self.assertRaises(NotImplementedError):
            _ = cache[1]

    def test_get_data_source_with_none_raises(self):
        cache = DataSourceCache()
        with self.assertRaises(NotImplementedError):
            cache.get_data_source(None)

    def test_equality(self):
        a = DataSourceCache(data_source_mappings={1: 'x'})
        b = DataSourceCache(data_source_mappings={1: 'x'})
        self.assertEqual(a, b)

    def test_inequality(self):
        a = DataSourceCache(data_source_mappings={1: 'x'})
        b = DataSourceCache(data_source_mappings={2: 'y'})
        self.assertNotEqual(a, b)


if __name__ == '__main__':
    unittest.main()
