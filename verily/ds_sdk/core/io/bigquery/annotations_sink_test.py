"""Tests for annotations_sink."""

import dataclasses
import os
import tempfile
from typing import Any, Iterable, Optional
import unittest
from unittest import mock

import apache_beam as beam
import avro.datafile as avro_datafile
import avro.io as avro_io
import google.api_core
from google.cloud import bigquery
import pandas as pd

from verily.ds_sdk.core import schemas
from verily.ds_sdk.core.gcp import credentials
from verily.ds_sdk.core.io.bigquery import annotations_sink
from verily.ds_sdk.core.utils import timestamps
from verily.ds_sdk.core.utils import timezone_utils
from verily.ds_sdk.protos import types_pb2


class FakeDsSdkCredentials(credentials.DsSdkCredentials):

    def get_credentials(self):
        return None, None


class FakeGcsBlob(object):
    """Fake Blob uploaded to GCS."""

    def __init__(self, temp_path: str):
        self._temp_path = temp_path

    def exists(self) -> bool:
        return True

    def upload_from_string(self, to_write: str):
        temp_dir = '/'.join(self._temp_path.split('/')[0:-1])
        os.makedirs(temp_dir, exist_ok=True)
        with open(self._temp_path, 'wb') as f:
            f.write(to_write)
        return self


class FakeGcsBucket(object):

    def __init__(self, name: str, temp_path: str):
        self._temp_path = temp_path
        self.name = name

    def blob(self, name: str):
        return FakeGcsBlob(os.path.join(self._temp_path, name))


class FakeGcsClient(object):
    """Fake client for calling GCS."""

    def __init__(self, temp_path: str):
        """Creates a FakeGcsClient.

    Args:
      temp_path: The temp directory to upload files to.
    """
        self._temp_path = temp_path

    def bucket(self, name: str):
        return FakeGcsBucket(name, os.path.join(self._temp_path, name))


class FakeLoadJob(object):
    """Fake load job returned by the fake BQ client."""

    def __init__(self, refresh_error):
        self.refresh_error = refresh_error
        self.job_id = 'job_id'

    def result(self):
        if self.refresh_error:
            raise self.refresh_error
        return self


@dataclasses.dataclass
class FakeBigqueryTable:
    schema: Iterable[bigquery.SchemaField]
    num_rows: int = 0


@dataclasses.dataclass(frozen=True)
class FakeBigQueryClient:
    """Fake BQ client."""
    expected_table: str
    expected_rows: Any = None
    rpc_error: Any = None
    refresh_error: Any = None
    table: Optional[FakeBigqueryTable] = None

    def get_table(self, table):
        del table
        if self.table is None:
            raise google.api_core.exceptions.NotFound('not-found')
        else:
            return self.table

    def create_table(self, table):
        del table
        pass

    def load_table_from_uri(self, file_path: str, table: str, job_config):
        del job_config

        if self.expected_table != table:
            raise AssertionError(
                f'table: {table} did not match expected: {self.expected_table}')
        if self.rpc_error is not None:
            raise self.rpc_error
        # Remove the glob parameter.
        file_path = file_path.replace('*', '')
        file_path = file_path.replace('gs:/', tempfile.gettempdir())

        if not self.refresh_error:
            expected_row_idx = 0
            files = os.listdir(file_path)
            files.sort()
            for file in files:
                with avro_datafile.DataFileReader(
                        open(os.path.join(file_path, file), 'rb'),
                        avro_io.DatumReader()) as reader:
                    for row in reader:
                        if expected_row_idx is None:
                            raise ValueError('no expected rows to compare to.')
                        expected_row = self.expected_rows[expected_row_idx]
                        for key, val in expected_row.items():
                            if row[key] != val:
                                raise AssertionError(
                                    f'rows not equal: got {row[key]} expected: '
                                    '{val}')
                        expected_row_idx += 1
            # Verify that we compared all rows.
            if len(self.expected_rows) != expected_row_idx:
                raise AssertionError(
                    f'compared row count did not match: got: {expected_row_idx} expected: {len(self.expected_rows)}'  # pylint: disable=line-too-long
                )
        return FakeLoadJob(self.refresh_error)

    def insert_rows(self, table, rows, **kwargs):
        del kwargs
        table_id = f'{table.project}.{table.dataset_id}.{table.table_id}'
        if self.expected_table != table_id:
            raise AssertionError(f'table: {table_id} did not match expected: '
                                 '{self.expected_table}')
        for row in rows:
            if row not in self.expected_rows:
                raise AssertionError(
                    f'row: {row} not found in: {self.expected_rows}')


class AnnotationsSinkTest(unittest.TestCase):

    def setUp(self):
        super().setUp()

        temp_dir = tempfile.gettempdir()
        self.gcs_patch = mock.patch(
            'verily.ds_sdk.core.io.bigquery.utils.bigquery_sink_utils.get_gcs_client'  # pylint: disable=line-too-long
        )
        mock_gcs = self.gcs_patch.start()
        mock_gcs.return_value = FakeGcsClient(temp_path=temp_dir)

        self.input_data = beam.Create([
            schemas.Annotation(
                annotation_label='label',
                start_timestamp_utc=timestamps.datetime_to_beam_timestamp(
                    pd.Timestamp('2020-01-01')),
                end_timestamp_utc=timestamps.datetime_to_beam_timestamp(
                    pd.Timestamp('2021-01-01')),
                annotation_metadata=schemas.AnnotationMetadata(
                    device_id='device',
                    participant_id='participant',
                    participant_namespace=1,
                    version_name='vname',
                    version_number=1,
                    input_data_info=[
                        schemas.InputDataInfo(version_number=1,
                                              version_name='namev',
                                              metric_type=schemas.MetricType(
                                                  stream_item_type=1,
                                                  derived_data_type=None,
                                                  annotation_type=None))
                    ]))
        ])

        self.data_source_cache = {
            123: types_pb2.DataSource(name='data_source_name')
        }

    def tearDown(self):
        self.gcs_patch.stop()

    @mock.patch(
        'verily.ds_sdk.core.io.bigquery.utils.bigquery_sink_utils.get_bigquery_client'  # pylint: disable=line-too-long
    )
    def test_write_to_bigquery(self, bq_mock):
        bq_mock.return_value = FakeBigQueryClient(
            'proj.dataset.table',
            expected_rows=[{
                'device_id':
                    'device',
                'annotation_label':
                    'label',
                'start_timestamp_utc':
                    timezone_utils.timestamp_to_ms(pd.Timestamp('2020-01-01')),
                'end_timestamp_utc':
                    timezone_utils.timestamp_to_ms(pd.Timestamp('2021-01-01')),
                'participant_id':
                    'participant',
                'participant_namespace':
                    'GAIA',
                'version_name':
                    'vname',
                'version_number':
                    1,
                'input_data_info': [{
                    'version_number': 1,
                    'version_name': 'namev',
                    'metric_type': {
                        'stream_item_type': 'MEASUREMENT_ALTITUDE',
                        'derived_data_type': None,
                        'annotation_type': None
                    }
                }]
            }])

        with beam.Pipeline(beam.runners.direct.DirectRunner()) as p:
            _ = (p | self.input_data |
                 annotations_sink.WriteAnnotationsToBigQuery(
                     table_id='proj.dataset.table',
                     creds=FakeDsSdkCredentials(
                         runner='', service_account='', billing_project=''),
                     temp_gcs_bucket='bucket',
                     bigquery_location='US'))


if __name__ == '__main__':
    unittest.main()
