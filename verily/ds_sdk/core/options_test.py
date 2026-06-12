"""Tests for ds_sdk.core.options.py."""

import json
import os
import shutil
import tempfile
import unittest
from unittest import mock
import uuid

import pandas as pd

from verily.ds_sdk.core import options


class FakeCred:
    # TODO impl this for GCS testing

    def get_credentials(self):
        return None, None


class FakeBlob:
    """FakeBlob for testing."""

    def __init__(self, full_path) -> None:
        self.full_path = full_path

    def download_as_bytes(self):
        return open(os.path.join(self.full_path, 'registry.json'),
                    mode='rb').read()

    def upload_from_string(self, to_upload):
        with open(os.path.join(self.full_path, 'registry.json'),
                  mode='w',
                  encoding='utf-8') as f:
            f.write(to_upload)


class FakeBucket:
    """FakeBucket for testing."""

    def __init__(self, expected_path, full_path) -> None:
        self.expected_path = expected_path
        self.full_path = full_path

    def get_blob(self, path: str) -> FakeBlob:
        if path != self.expected_path:
            raise AssertionError(
                f'unexpected path. want: {self.expected_path} got: {path}')
        if not os.path.isfile(self.full_path + '/registry.json'):
            return None
        return FakeBlob(self.full_path)

    def blob(self, path: str) -> FakeBlob:
        del path
        return FakeBlob(self.full_path)


class FakeGcsClient:
    """FakeGcsClient for testing."""

    def __init__(self, expected_bucket, expected_path, full_path) -> None:
        self.expected_bucket = expected_bucket
        self.expected_path = expected_path
        self.full_path = full_path

    def bucket(self, name: str) -> FakeBucket:
        if name != self.expected_bucket:
            raise AssertionError(
                f'unexpected bucket name. want: {self.expected_bucket} got: '
                f'{name}')

        return FakeBucket(self.expected_path, self.full_path)


class OptionsTest(unittest.TestCase):

    def setUp(self):
        self.temp_path = tempfile.mkdtemp()
        gcs_path_suffix = str(uuid.uuid4())
        self.temp_gcs_full_path = '/'.join([self.temp_path, gcs_path_suffix])
        os.mkdir(self.temp_gcs_full_path)
        split_path = self.temp_gcs_full_path.split('/', 1)
        self.gcs_full_path = 'gs://' + self.temp_gcs_full_path
        self.bucket = split_path[0]
        self.blob_path = split_path[1] + '/registry.json'
        self.want_end_time = pd.Timestamp('2020-01-01')

    def tearDown(self):
        shutil.rmtree(self.temp_path)

    @mock.patch('pandas.Timestamp.now', return_value=pd.Timestamp('2021-01-01'))
    def test_incremental_options_state_file_local(self, time_mock):
        del time_mock
        with open(os.path.join(self.temp_path, 'registry.json'),
                  'w',
                  encoding='utf-8') as f:
            json.dump({'last_write_end_time': str(self.want_end_time)}, f)

        new_inc_opts = options.IncrementalQueryOptions(
            state_file_path=self.temp_path)
        new_inc_opts.update_from_state_file('registry', FakeCred(), 'project')
        self.assertEqual(new_inc_opts.write_start_time, self.want_end_time)

        new_inc_opts.write_state_file('registry', FakeCred(), 'project')
        with open(os.path.join(self.temp_path, 'registry.json'),
                  'r',
                  encoding='utf-8') as f:
            state = json.load(f)
            self.assertEqual(state,
                             {'last_write_end_time': '2021-01-01 00:00:00'})

    @mock.patch('google.cloud.storage.Client')
    @mock.patch('pandas.Timestamp.now', return_value=pd.Timestamp('2021-01-01'))
    def test_incremental_options_state_file_gcs(
            self, time_mock, gcs_client_mock: mock.MagicMock):
        del time_mock
        with open(os.path.join(self.temp_gcs_full_path, 'registry,json'),
                  'w',
                  encoding='utf-8') as f:
            json.dump({'last_write_end_time': str(self.want_end_time)}, f)
        with open(os.path.join(self.temp_gcs_full_path, 'registry.json'),
                  'w',
                  encoding='utf-8') as f:
            json.dump({'last_write_end_time': str(self.want_end_time)}, f)
        gcs_client_mock.return_value = FakeGcsClient(self.bucket,
                                                     self.blob_path,
                                                     self.temp_gcs_full_path)

        new_inc_opts = options.IncrementalQueryOptions(
            state_file_path=self.gcs_full_path)
        new_inc_opts.update_from_state_file('registry', FakeCred(), 'project')
        self.assertEqual(new_inc_opts.write_start_time, self.want_end_time)

        with open(os.path.join(self.temp_gcs_full_path, 'registry,json'),
                  'r',
                  encoding='utf-8') as f:
            state = json.load(f)
        new_inc_opts.write_state_file('registry', FakeCred(), 'project')
        with open(os.path.join(self.temp_gcs_full_path, 'registry.json'),
                  'r',
                  encoding='utf-8') as f:
            state = json.load(f)
            self.assertEqual(state,
                             {'last_write_end_time': '2021-01-01 00:00:00'})

    @mock.patch('pandas.Timestamp.now', return_value=pd.Timestamp('2021-01-01'))
    def test_incremental_options_state_file_local_no_file(self, time_mock):
        del time_mock
        new_inc_opts = options.IncrementalQueryOptions(
            state_file_path=self.temp_path)
        new_inc_opts.update_from_state_file('registry', FakeCred(), 'project')
        self.assertEqual(new_inc_opts.write_start_time,
                         pd.Timestamp('2020-12-29'))
        self.assertEqual(new_inc_opts.write_end_time,
                         pd.Timestamp('2021-01-01'))

    @mock.patch('google.cloud.storage.Client')
    @mock.patch('pandas.Timestamp.now', return_value=pd.Timestamp('2021-01-01'))
    def test_incremental_options_state_file_gcs_no_file(
            self, time_mock, gcs_client_mock: mock.MagicMock):
        del time_mock
        gcs_client_mock.return_value = FakeGcsClient(self.bucket,
                                                     self.blob_path,
                                                     self.temp_gcs_full_path)
        new_inc_opts = options.IncrementalQueryOptions(
            state_file_path=self.gcs_full_path)
        new_inc_opts.update_from_state_file('registry', FakeCred(), 'project')
        self.assertEqual(new_inc_opts.write_start_time,
                         pd.Timestamp('2020-12-29'))
        self.assertEqual(new_inc_opts.write_end_time,
                         pd.Timestamp('2021-01-01'))


if __name__ == '__main__':
    unittest.main()
