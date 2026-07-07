"""Acceptance tests for verily-raw-data-tools standalone package.

AC1: Zero ds_sdk imports in production code.
AC2: Fresh import works with ds_sdk blocked; schema registry has 200+ entries.
AC3: End-to-end DirectRunner pipeline: Create -> Key -> GroupIntoDataFrames.
AC4: Examples execute (synthetic-data path).
AC5: Walk every submodule without ds_sdk.
AC6: DataSourceCache stub raises NotImplementedError.
"""

import importlib
import os
import pkgutil
import re
import runpy
import subprocess
import sys
import unittest


class AC1_NoDsSdkImports(unittest.TestCase):

    def test_no_ds_sdk_imports_in_production_code(self):
        root = os.path.join(os.path.dirname(__file__), '..',
                            'verily', 'raw_data_tools')
        root = os.path.abspath(root)
        violations = []
        for dirpath, _, filenames in os.walk(root):
            for fn in filenames:
                if not fn.endswith('.py') or fn.endswith('_test.py'):
                    continue
                filepath = os.path.join(dirpath, fn)
                with open(filepath) as f:
                    for lineno, line in enumerate(f, 1):
                        if re.search(
                                r'verily\.ds_sdk|verily\s+import\s+ds_sdk',
                                line):
                            rel = os.path.relpath(filepath, root)
                            violations.append(f'{rel}:{lineno}: {line.rstrip()}')
        self.assertEqual(
            violations, [],
            f'Found {len(violations)} ds_sdk import(s) in production code:\n'
            + '\n'.join(violations))


class AC2_FreshImport(unittest.TestCase):

    def test_import_blocker_is_active(self):
        with self.assertRaises(ImportError) as ctx:
            import verily.ds_sdk  # noqa: F401
        self.assertIn('ds_sdk import blocker', str(ctx.exception))

    def test_import_package(self):
        import verily.raw_data_tools  # noqa: F401

    def test_schema_registry_count(self):
        from verily.raw_data_tools.schemas.schemas import (
            DATA_SPEC_NAME_TO_SCHEMA_CLASS)
        self.assertGreater(
            len(DATA_SPEC_NAME_TO_SCHEMA_CLASS), 200,
            f'Schema registry has only '
            f'{len(DATA_SPEC_NAME_TO_SCHEMA_CLASS)} entries, expected >200')

    def test_version_attribute(self):
        import verily.raw_data_tools as rdt
        self.assertTrue(rdt.__version__,
                        'Package __version__ should be non-empty')

    def test_version_metadata(self):
        try:
            from importlib.metadata import version
            v = version('verily-raw-data-tools')
            self.assertTrue(v, 'Installed version should be non-empty')
        except Exception:
            self.skipTest(
                'Package not pip-installed; '
                'importlib.metadata.version unavailable')


class AC3_EndToEndPipeline(unittest.TestCase):

    def test_unpack_key_build_dataframes(self):
        import apache_beam as beam
        from apache_beam.testing.test_pipeline import TestPipeline
        from apache_beam.testing.util import assert_that
        from apache_beam.testing.util import is_not_empty
        from apache_beam.utils.timestamp import Timestamp
        import pandas as pd

        from verily.raw_data_tools.schemas.schemas.shared_schemas import (
            DataPoint, DataPointMetadata, _STATE_KEY)
        from verily.raw_data_tools.transforms.key_by import KeyDataPointsBy
        from verily.raw_data_tools.transforms.group_into_data_frames import (
            GroupIntoDataFrames)

        metadata = DataPointMetadata(
            data_source_id=1,
            device_id='dev1',
            participant_id='part1',
            participant_namespace=1,
            echo_metadata=None,
            sensor_store_metadata=None,
            annotation_labels=set(),
            _state_key=_STATE_KEY.CREATED_USING_BUILDER)

        ts = Timestamp.from_utc_datetime(
            pd.Timestamp('2024-01-01 12:00:00', tz='UTC'))

        data_points = [
            DataPoint(
                data_point_metadata=metadata,
                measurement_timestamp_utc=ts),
        ]

        with TestPipeline() as p:
            output = (
                p
                | beam.Create(data_points)
                | KeyDataPointsBy(by_device=True, by_participant=True)
                | GroupIntoDataFrames()
            )
            assert_that(output, is_not_empty())


class AC4_ExamplesExecute(unittest.TestCase):
    """Each example should run its synthetic-data fallback with no credentials."""

    _EXAMPLES_DIR = os.path.join(os.path.dirname(__file__), '..', 'examples')

    _REPO_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))

    def _run_example(self, filename):
        filepath = os.path.abspath(
            os.path.join(self._EXAMPLES_DIR, filename))
        env = os.environ.copy()
        env.pop('GOOGLE_PROJECT', None)
        env.pop('GOOGLE_APPLICATION_CREDENTIALS', None)
        env['PYTHONPATH'] = (
            self._REPO_ROOT + os.pathsep + env.get('PYTHONPATH', ''))
        result = subprocess.run(
            [sys.executable, filepath],
            capture_output=True, text=True, timeout=120, env=env)
        self.assertEqual(
            result.returncode, 0,
            f'{filename} failed (rc={result.returncode}):\n'
            f'stdout:\n{result.stdout}\nstderr:\n{result.stderr}')

    def test_basic_data_read(self):
        self._run_example('basic_data_read.py')

    def test_unpack_and_transform(self):
        self._run_example('unpack_and_transform.py')

    def test_custom_algorithm(self):
        self._run_example('custom_algorithm.py')

    def test_full_pipeline(self):
        self._run_example('full_pipeline.py')


class AC5_WalkAllModules(unittest.TestCase):

    def test_walk_and_import_all_modules(self):
        import verily.raw_data_tools as rdt
        failures = []
        skipped_prefixes = (
            'verily.raw_data_tools.schemas.schemas.gen.',
            'verily.raw_data_tools.pipeline.docker.',
        )
        for importer, modname, ispkg in pkgutil.walk_packages(
                rdt.__path__, 'verily.raw_data_tools.'):
            if any(modname.startswith(p) for p in skipped_prefixes):
                continue
            if modname.endswith('_test'):
                continue
            try:
                importlib.import_module(modname)
            except Exception as e:
                failures.append(f'{modname}: {e}')
        self.assertEqual(
            failures, [],
            f'{len(failures)} module(s) failed to import:\n'
            + '\n'.join(failures))


class AC6_DataSourceCacheStub(unittest.TestCase):

    def test_get_data_source_raises(self):
        from verily.raw_data_tools.utils.data_source_cache import DataSourceCache
        cache = DataSourceCache()
        with self.assertRaises(NotImplementedError):
            cache.get_data_source(1)

    def test_get_raises(self):
        from verily.raw_data_tools.utils.data_source_cache import DataSourceCache
        cache = DataSourceCache()
        with self.assertRaises(NotImplementedError):
            cache.get(1)

    def test_getitem_raises(self):
        from verily.raw_data_tools.utils.data_source_cache import DataSourceCache
        cache = DataSourceCache()
        with self.assertRaises(NotImplementedError):
            _ = cache[1]

    def test_redis_raises(self):
        from verily.raw_data_tools.utils.data_source_cache import DataSourceCache
        with self.assertRaises(NotImplementedError):
            DataSourceCache(redis_end_point='redis://localhost:6379')


if __name__ == '__main__':
    unittest.main()
