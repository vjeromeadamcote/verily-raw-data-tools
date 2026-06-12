"""Tests for data_points_sink."""

import dataclasses
import datetime
import os
import tempfile
from typing import Any, Iterable, List, Optional, Set, Union
import unittest
from unittest import mock

import apache_beam as beam
from apache_beam.utils import timestamp
import avro.datafile as avro_datafile
import avro.io as avro_io
import google.api_core
from google.cloud import bigquery
import numpy as np
import pandas as pd
import pytz

from verily.ds_sdk.core import schemas
from verily.ds_sdk.core.gcp import credentials
from verily.ds_sdk.core.io.bigquery import data_points_sink
from verily.ds_sdk.core.io.data_source_cache import DataSourceCache
from verily.ds_sdk.core.utils import timezone_utils
from verily.ds_sdk.protos import types_pb2


def build_timestamp(timestamp_str: str) -> timestamp.Timestamp:
    return timestamp.Timestamp.from_utc_datetime(
        pd.Timestamp(timestamp_str, tz='UTC'))


@dataclasses.dataclass
class TestData(schemas.DataPoint):
    """Beam RowSchema for com.verily.pressure."""
    int_field: int
    string_field: str
    bool_field: bool
    float_field: float
    bytes_field: bytes
    num_py_int64: np.int64
    num_py_int32: np.int32
    num_py_int16: np.int16
    num_py_int8: np.int8
    num_py_float128: np.longdouble
    num_py_float64: np.longdouble
    num_py_float32: np.longdouble
    num_py_float16: np.longdouble
    timestamp_field: timestamp.Timestamp
    iterable_field: Iterable[int]
    list_field: List[str]
    set_field: Set[float]
    optional_field: Optional[int]
    union_field: Union[int, None]
    optional_list_unset: Optional[List[int]]
    optional_list_set: Optional[List[int]]
    list_containing_optionals: List[Optional[int]]


_EXPECTED_BQ_SCHEMA = [
    bigquery.SchemaField('DeviceID', 'STRING', 'NULLABLE'),
    bigquery.SchemaField('DataPointTime', 'TIMESTAMP', 'NULLABLE'),
    bigquery.SchemaField('DataPoint',
                         'RECORD',
                         'NULLABLE',
                         fields=(bigquery.SchemaField('pressure', 'INTEGER',
                                                      'NULLABLE'),)),
    bigquery.SchemaField(
        'DataSource',
        'RECORD',
        'NULLABLE',
        fields=(bigquery.SchemaField('name', 'STRING', 'NULLABLE'),
                bigquery.SchemaField(
                    'application',
                    'RECORD',
                    'NULLABLE',
                    fields=(bigquery.SchemaField('id', 'INTEGER', 'NULLABLE'),
                            bigquery.SchemaField('version', 'STRING',
                                                 'NULLABLE'))),
                bigquery.SchemaField(
                    'device',
                    'RECORD',
                    'NULLABLE',
                    fields=(bigquery.SchemaField('serial_number', 'STRING',
                                                 'NULLABLE'),
                            bigquery.SchemaField('name', 'STRING', 'NULLABLE'),
                            bigquery.SchemaField('hardware_version', 'STRING',
                                                 'NULLABLE'),
                            bigquery.SchemaField('firmware_version', 'STRING',
                                                 'NULLABLE'),
                            bigquery.SchemaField('software_version', 'STRING',
                                                 'NULLABLE'),
                            bigquery.SchemaField('time_zone_name', 'STRING',
                                                 'NULLABLE'),
                            bigquery.SchemaField('manufacturer', 'STRING',
                                                 'NULLABLE'),
                            bigquery.SchemaField('model', 'STRING', 'NULLABLE'),
                            bigquery.SchemaField('os_version', 'STRING',
                                                 'NULLABLE'),
                            bigquery.SchemaField('data_session_id', 'STRING',
                                                 'NULLABLE'),
                            bigquery.SchemaField('android_metadata',
                                                 'RECORD',
                                                 'NULLABLE',
                                                 fields=(bigquery.SchemaField(
                                                     'brand', 'STRING',
                                                     'NULLABLE'),
                                                         bigquery.SchemaField(
                                                             'manufacturer',
                                                             'STRING',
                                                             'NULLABLE'),
                                                         bigquery.SchemaField(
                                                             'model', 'STRING',
                                                             'NULLABLE'),
                                                         bigquery.SchemaField(
                                                             'os_version',
                                                             'STRING',
                                                             'NULLABLE'))),
                            bigquery.SchemaField('daylight_saving_time',
                                                 'INTEGER', 'NULLABLE'))),
                bigquery.SchemaField(
                    'data_spec',
                    'RECORD',
                    'NULLABLE',
                    fields=(bigquery.SchemaField('name', 'STRING', 'NULLABLE'),
                            bigquery.SchemaField(
                                'field_specs',
                                'RECORD',
                                'REPEATED',
                                fields=(bigquery.SchemaField(
                                    'name', 'STRING', 'NULLABLE'),
                                        bigquery.SchemaField(
                                            'units',
                                            'RECORD',
                                            'REPEATED',
                                            fields=(bigquery.SchemaField(
                                                'type', 'INTEGER', 'NULLABLE'),
                                                    bigquery.SchemaField(
                                                        'scale', 'INTEGER',
                                                        'NULLABLE'),
                                                    bigquery.SchemaField(
                                                        'power', 'INTEGER',
                                                        'NULLABLE'),
                                                    bigquery.SchemaField(
                                                        'scale_factor', 'FLOAT',
                                                        'NULLABLE'))),
                                        bigquery.SchemaField(
                                            'primitive', 'INTEGER', 'NULLABLE'),
                                        bigquery.SchemaField(
                                            'is_optional', 'BOOLEAN',
                                            'NULLABLE'),
                                        bigquery.SchemaField(
                                            'accepted_string_values', 'STRING',
                                            'REPEATED'))),
                            bigquery.SchemaField('canonical_data_spec',
                                                 'STRING'))),
                bigquery.SchemaField('sensor',
                                     'RECORD',
                                     'NULLABLE',
                                     fields=(bigquery.SchemaField(
                                         'id', 'STRING', 'NULLABLE'),)),
                bigquery.SchemaField(
                    'algorithm',
                    'RECORD',
                    'NULLABLE',
                    fields=(bigquery.SchemaField('name', 'STRING', 'NULLABLE'),
                            bigquery.SchemaField('version', 'STRING',
                                                 'NULLABLE'))),
                bigquery.SchemaField('registry', 'STRING', 'NULLABLE')))
]


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
    project: str = 'proj'
    dataset_id: str = 'dataset'
    table_id: str = 'table'


@dataclasses.dataclass(frozen=True)
class FakeBigQueryClient:
    """Fake BQ client."""
    expected_table: str
    expected_rows: Any = None
    rpc_error: Any = None
    refresh_error: Any = None
    expected_schema: Optional[Iterable[bigquery.SchemaField]] = None
    table: Optional[FakeBigqueryTable] = None

    def get_table(self, table):
        del table
        if self.table is None:
            raise google.api_core.exceptions.NotFound('not-found')
        else:
            return self.table

    def create_table(self, table):
        if self.expected_schema is not None:
            if table.schema != self.expected_schema:
                raise AssertionError(
                    f'Schemas did not match. wanted: {self.expected_schema} '
                    f'got: {table.schema}')

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
                                    f'rows not equal: got {row[key]} '
                                    f'expected: {val}')
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
                                 f'{self.expected_table}')
        for row in rows:
            if row not in self.expected_rows:
                raise AssertionError(
                    f'row: {row} not found in: {self.expected_rows}')


class BigQuerySinkTest(unittest.TestCase):

    def setUp(self):
        super().setUp()

        temp_dir = tempfile.gettempdir()
        self.gcs_patch = mock.patch(
            'verily.ds_sdk.core.io.bigquery.utils.bigquery_sink_utils.get_gcs_client'  # pylint: disable=line-too-long
        )
        mock_gcs = self.gcs_patch.start()
        mock_gcs.return_value = FakeGcsClient(temp_path=temp_dir)

        self.input_data = beam.Create([
            schemas.Pressure(
                schemas.data_point_metadata_for_raw_data(
                    data_source_id=123,
                    device_id='123',
                    participant_id='123',
                    participant_namespace=1,
                    echo_metadata=None,
                    sensor_store_metadata=None,
                    annotation_labels=set()),
                measurement_timestamp_utc=timestamp.Timestamp.from_utc_datetime(
                    datetime.datetime(year=2020,
                                      month=1,
                                      day=1,
                                      tzinfo=pytz.utc)),
                pressure=1)
        ]).with_output_types(schemas.Pressure)

        self.data_source_cache = DataSourceCache({
            123:
                types_pb2.DataSource(
                    # DataSource name should be eplaced with Ds_Sdk_Derived.
                    name='data_source_name',
                    # DataSpec should be dropped by the sink
                    data_spec=types_pb2.DataSpec(name='foo'),
                    # Algorithm should be dropped by the sink
                    algorithm=types_pb2.Algorithm(name='foo'),
                    # Application should be dropped by the sink
                    application=types_pb2.Application(version='foo'),
                    device=types_pb2.Device(time_zone_name='US/Eastern'))
        })

    def tearDown(self):
        self.gcs_patch.stop()

    @mock.patch(
        'verily.ds_sdk.core.io.bigquery.utils.bigquery_sink_utils.get_bigquery_client'  # pylint: disable=line-too-long
    )
    def test_write_to_bigquery(self, bq_mock):
        bq_mock.return_value = FakeBigQueryClient(
            'proj.dataset.table',
            expected_rows=[{
                'DeviceID':
                    '123',
                'DataPointTime':
                    timezone_utils.timestamp_to_ms(
                        datetime.datetime(year=2020,
                                          month=1,
                                          day=1,
                                          tzinfo=pytz.utc)),
                'DataPoint': {
                    'pressure': 1
                },
                'DataSource': {
                    'name': 'Ds_Sdk_Derived',
                    'application': None,
                    'device': {
                        'serial_number': '',
                        'name': '',
                        'hardware_version': '',
                        'firmware_version': '',
                        'software_version': '',
                        'time_zone_name': 'US/Eastern',
                        'manufacturer': '',
                        'model': '',
                        'os_version': '',
                        'data_session_id': '',
                        'android_metadata': None,
                        'daylight_saving_time': 0
                    },
                    'data_spec': None,
                    'sensor': None,
                    'algorithm': None,
                    'registry': ''
                }
            }],
            expected_schema=_EXPECTED_BQ_SCHEMA)

        with beam.Pipeline(beam.runners.direct.DirectRunner()) as p:
            _ = (p | self.input_data |
                 data_points_sink.WriteDataPointsToBigQuery(
                     table_id='proj.dataset.table',
                     project_id='proj',
                     schema=schemas.Pressure,
                     creds=FakeDsSdkCredentials(
                         runner='', service_account='', billing_project=''),
                     temp_gcs_bucket='bucket/with/path',
                     data_source_cache=self.data_source_cache,
                     streaming=False,
                     bigquery_location='US').with_input_types(schemas.Pressure))

    @mock.patch(
        'verily.ds_sdk.core.io.bigquery.utils.bigquery_sink_utils.get_bigquery_client'  # pylint: disable=line-too-long
    )
    def test_write_to_bigquery_type_mismatch(self, bq_mock):
        bq_mock.return_value = FakeBigQueryClient(
            'proj.dataset.table',
            expected_rows=[],
            expected_schema=_EXPECTED_BQ_SCHEMA)

        with self.assertRaisesRegex(ValueError, 'DataPoint.pressure'):
            with beam.Pipeline(beam.runners.direct.DirectRunner()) as p:
                _ = (
                    p | beam.Create([
                        schemas.Pressure(
                            schemas.data_point_metadata_for_raw_data(
                                data_source_id=123,
                                device_id='123',
                                participant_id='123',
                                participant_namespace=1,
                                echo_metadata=None,
                                sensor_store_metadata=None,
                                annotation_labels=set()),
                            measurement_timestamp_utc=timestamp.Timestamp.
                            from_utc_datetime(
                                datetime.datetime(
                                    year=2020, month=1, day=1,
                                    tzinfo=pytz.utc)),
                            # NOTE: wrong type, avro will barf, which we want.
                            pressure=np.bool8(True))
                    ]) | data_points_sink.WriteDataPointsToBigQuery(
                        table_id='proj.dataset.table',
                        project_id='proj',
                        schema=schemas.Pressure,
                        creds=FakeDsSdkCredentials(
                            runner='', service_account='', billing_project=''),
                        temp_gcs_bucket='bucket/with/path',
                        data_source_cache=self.data_source_cache,
                        streaming=False,
                        bigquery_location='US').with_input_types(
                            schemas.Pressure))

    @mock.patch(
        'verily.ds_sdk.core.io.bigquery.utils.bigquery_sink_utils.get_bigquery_client'  # pylint: disable=line-too-long
    )
    def test_write_using_avro_refresh_job_error(self, bq_mock):
        bq_mock.return_value = FakeBigQueryClient(
            'proj.dataset.table',
            refresh_error=google.api_core.exceptions.GoogleAPICallError(
                'testing'))

        with self.assertRaises(ValueError):
            with beam.Pipeline(beam.runners.direct.DirectRunner()) as p:
                _ = (p | self.input_data |
                     data_points_sink.WriteDataPointsToBigQuery(
                         table_id='proj.dataset.table',
                         project_id='proj',
                         schema=schemas.Pressure,
                         creds=FakeDsSdkCredentials(
                             runner='', service_account='', billing_project=''),
                         temp_gcs_bucket='bucket',
                         data_source_cache=self.data_source_cache,
                         streaming=False,
                         bigquery_location='US').with_input_types(
                             schemas.Pressure))

    @mock.patch(
        'verily.ds_sdk.core.io.bigquery.utils.bigquery_sink_utils.get_bigquery_client'  # pylint: disable=line-too-long
    )
    def test_write_using_avro_rpc_error(self, bq_mock):
        bq_mock.return_value = FakeBigQueryClient(
            'proj.dataset.table',
            rpc_error=google.api_core.exceptions.GoogleAPICallError('testing'))

        with self.assertRaises(ValueError):
            with beam.Pipeline(beam.runners.direct.DirectRunner()) as p:
                _ = (p | self.input_data |
                     data_points_sink.WriteDataPointsToBigQuery(
                         table_id='proj.dataset.table',
                         project_id='proj',
                         schema=schemas.Pressure,
                         creds=FakeDsSdkCredentials(
                             runner='', service_account='', billing_project=''),
                         temp_gcs_bucket='bucket',
                         data_source_cache=self.data_source_cache,
                         streaming=False,
                         bigquery_location='US').with_input_types(
                             schemas.Pressure))

    @mock.patch(
        'verily.ds_sdk.core.io.bigquery.utils.bigquery_sink_utils.get_bigquery_client'  # pylint: disable=line-too-long
    )
    def test_write_to_bigquery_all_types(self, bq_mock):

        input_data = beam.Create([
            TestData(
                schemas.data_point_metadata_for_raw_data(
                    data_source_id=123,
                    device_id='123',
                    participant_id='123',
                    participant_namespace=1,
                    echo_metadata=None,
                    sensor_store_metadata=None,
                    annotation_labels=set()),
                measurement_timestamp_utc=timestamp.Timestamp.from_utc_datetime(
                    datetime.datetime(year=2020,
                                      month=1,
                                      day=1,
                                      tzinfo=pytz.utc)),
                int_field=1,
                string_field='str',
                bool_field=True,
                float_field=1.1,
                bytes_field='byte_string'.encode(),
                num_py_int64=np.int64(64),
                num_py_int32=np.int32(32),
                num_py_int16=np.int16(16),
                num_py_int8=np.int8(8),
                num_py_float128=np.float128(128.123),
                num_py_float64=np.float128(64.64),
                num_py_float32=np.float128(32.32),
                num_py_float16=np.float128(16.16),
                timestamp_field=timestamp.Timestamp.from_utc_datetime(
                    datetime.datetime(year=2020,
                                      month=1,
                                      day=1,
                                      tzinfo=pytz.utc)),
                iterable_field=[1, 2, 3],
                list_field=['a', 'b'],
                set_field={1.1, 2.2},
                optional_field=None,
                union_field=None,
                optional_list_unset=None,
                optional_list_set=[1, 2],
                list_containing_optionals=[1, None])
        ])

        bq_mock.return_value = FakeBigQueryClient(
            'proj.dataset.table',
            expected_rows=[{
                'DeviceID':
                    '123',
                'DataPointTime':
                    timezone_utils.timestamp_to_ms(
                        datetime.datetime(year=2020,
                                          month=1,
                                          day=1,
                                          tzinfo=pytz.utc)),
                'DataPoint': {
                    'int_field': 1,
                    'string_field': 'str',
                    'bool_field': True,
                    'float_field': 1.1,
                    'bytes_field': 'byte_string'.encode(),
                    'num_py_int64': 64,
                    'num_py_int32': 32,
                    'num_py_int16': 16,
                    'num_py_int8': 8,
                    'num_py_float128': 128.123,
                    'num_py_float64': 64.64,
                    'num_py_float32': 32.32,
                    'num_py_float16': 16.16,
                    'timestamp_field': 1577836800000,
                    'iterable_field': [1, 2, 3],
                    'list_field': ['a', 'b'],
                    'set_field': [1.1, 2.2],
                    'optional_field': None,
                    'union_field': None,
                    'optional_list_set': [1, 2],
                    'optional_list_unset': None,
                    'list_containing_optionals': [1, None],
                }
            }])

        with beam.Pipeline(beam.runners.direct.DirectRunner()) as p:
            _ = (p | input_data | data_points_sink.WriteDataPointsToBigQuery(
                table_id='proj.dataset.table',
                project_id='proj',
                schema=TestData,
                creds=FakeDsSdkCredentials(
                    runner='', service_account='', billing_project=''),
                temp_gcs_bucket='bucket',
                data_source_cache=self.data_source_cache,
                streaming=False,
                bigquery_location='US').with_input_types(TestData))

    @mock.patch(
        'verily.ds_sdk.core.io.bigquery.utils.bigquery_sink_utils.get_bigquery_client'  # pylint: disable=line-too-long
    )
    def test_write_to_bigquery_table_append_same_schema(self, bq_mock):
        bq_mock.return_value = FakeBigQueryClient(
            'proj.dataset.table',
            expected_rows=[{
                'DeviceID':
                    '123',
                'DataPointTime':
                    timezone_utils.timestamp_to_ms(
                        datetime.datetime(year=2020,
                                          month=1,
                                          day=1,
                                          tzinfo=pytz.utc)),
                'DataPoint': {
                    'pressure': 1
                },
                'DataSource': {
                    'name': 'Ds_Sdk_Derived',
                    'application': None,
                    'device': {
                        'serial_number': '',
                        'name': '',
                        'hardware_version': '',
                        'firmware_version': '',
                        'software_version': '',
                        'time_zone_name': 'US/Eastern',
                        'manufacturer': '',
                        'model': '',
                        'os_version': '',
                        'data_session_id': '',
                        'android_metadata': None,
                        'daylight_saving_time': 0
                    },
                    'data_spec': None,
                    'sensor': None,
                    'algorithm': None,
                    'registry': ''
                }
            }],
            expected_schema=_EXPECTED_BQ_SCHEMA,
            table=FakeBigqueryTable(_EXPECTED_BQ_SCHEMA))

        with beam.Pipeline(beam.runners.direct.DirectRunner()) as p:
            _ = (p | self.input_data |
                 data_points_sink.WriteDataPointsToBigQuery(
                     table_id='proj.dataset.table',
                     project_id='proj',
                     schema=schemas.Pressure,
                     creds=FakeDsSdkCredentials(
                         runner='', service_account='', billing_project=''),
                     temp_gcs_bucket='bucket',
                     data_source_cache=self.data_source_cache,
                     write_disposition=bigquery.WriteDisposition.WRITE_APPEND,
                     streaming=False,
                     bigquery_location='US').with_input_types(schemas.Pressure))

    @mock.patch(
        'verily.ds_sdk.core.io.bigquery.utils.bigquery_sink_utils.get_bigquery_client'  # pylint: disable=line-too-long
    )
    def test_write_to_bigquery_table_append_diff_schema(self, bq_mock):
        bq_mock.return_value = FakeBigQueryClient(
            'proj.dataset.table',
            expected_rows=[],
            expected_schema=_EXPECTED_BQ_SCHEMA,
            table=FakeBigqueryTable([
                bigquery.SchemaField('field_name', 'STRING', 'NULLABLE'),
            ]))

        with self.assertRaisesRegex(
                ValueError,
                'If appending to an existing table the schemas must be '
                'identical'):
            with beam.Pipeline(beam.runners.direct.DirectRunner()) as p:
                _ = (p | self.input_data |
                     data_points_sink.WriteDataPointsToBigQuery(
                         table_id='proj.dataset.table',
                         project_id='proj',
                         schema=schemas.Pressure,
                         creds=FakeDsSdkCredentials(
                             runner='', service_account='', billing_project=''),
                         temp_gcs_bucket='bucket',
                         data_source_cache=self.data_source_cache,
                         write_disposition=bigquery.WriteDisposition.
                         WRITE_APPEND,
                         streaming=False,
                         bigquery_location='US').with_input_types(
                             schemas.Pressure))

    @mock.patch(
        'verily.ds_sdk.core.io.bigquery.utils.bigquery_sink_utils.get_bigquery_client'  # pylint: disable=line-too-long
    )
    def test_write_to_bigquery_table_write_empty_table_has_data(self, bq_mock):
        bq_mock.return_value = FakeBigQueryClient(
            'proj.dataset.table',
            expected_rows=[],
            expected_schema=_EXPECTED_BQ_SCHEMA,
            table=FakeBigqueryTable(_EXPECTED_BQ_SCHEMA, 1))

        with self.assertRaisesRegex(
                ValueError, 'already exists and WRITE_EMPTY was provided'):
            with beam.Pipeline(beam.runners.direct.DirectRunner()) as p:
                _ = (
                    p | self.input_data |
                    data_points_sink.WriteDataPointsToBigQuery(
                        table_id='proj.dataset.table',
                        project_id='proj',
                        schema=schemas.Pressure,
                        creds=FakeDsSdkCredentials(
                            runner='', service_account='', billing_project=''),
                        temp_gcs_bucket='bucket',
                        data_source_cache=self.data_source_cache,
                        write_disposition=bigquery.WriteDisposition.WRITE_EMPTY,
                        streaming=False,
                        bigquery_location='US').with_input_types(
                            schemas.Pressure))

    @mock.patch(
        'verily.ds_sdk.core.io.bigquery.utils.bigquery_sink_utils.get_bigquery_client'  # pylint: disable=line-too-long
    )
    def test_write_to_bigquery_table_write_empty_table_has_no_data(
            self, bq_mock):
        bq_mock.return_value = FakeBigQueryClient(
            'proj.dataset.table',
            expected_rows=[{
                'DeviceID':
                    '123',
                'DataPointTime':
                    timezone_utils.timestamp_to_ms(
                        datetime.datetime(year=2020,
                                          month=1,
                                          day=1,
                                          tzinfo=pytz.utc)),
                'DataPoint': {
                    'pressure': 1
                },
                'DataSource': {
                    'name': 'Ds_Sdk_Derived',
                    'application': None,
                    'device': {
                        'serial_number': '',
                        'name': '',
                        'hardware_version': '',
                        'firmware_version': '',
                        'software_version': '',
                        'time_zone_name': 'US/Eastern',
                        'manufacturer': '',
                        'model': '',
                        'os_version': '',
                        'data_session_id': '',
                        'android_metadata': None,
                        'daylight_saving_time': 0
                    },
                    'data_spec': None,
                    'sensor': None,
                    'algorithm': None,
                    'registry': ''
                }
            }],
            expected_schema=_EXPECTED_BQ_SCHEMA,
            table=FakeBigqueryTable(_EXPECTED_BQ_SCHEMA, 0))

        with beam.Pipeline(beam.runners.direct.DirectRunner()) as p:
            _ = (p | self.input_data |
                 data_points_sink.WriteDataPointsToBigQuery(
                     table_id='proj.dataset.table',
                     project_id='proj',
                     schema=schemas.Pressure,
                     creds=FakeDsSdkCredentials(
                         runner='', service_account='', billing_project=''),
                     temp_gcs_bucket='bucket',
                     data_source_cache=self.data_source_cache,
                     write_disposition=bigquery.WriteDisposition.WRITE_EMPTY,
                     streaming=False,
                     bigquery_location='US').with_input_types(schemas.Pressure))

    @mock.patch(
        'verily.ds_sdk.core.io.bigquery.utils.bigquery_sink_utils.get_bigquery_client'  # pylint: disable=line-too-long
    )
    def test_write_to_bigquery_with_custom_windowing_upstream(self, bq_mock):
        input_data = beam.Create([
            schemas.Pressure(
                schemas.data_point_metadata_for_raw_data(
                    data_source_id=123,
                    device_id='123',
                    participant_id='123',
                    participant_namespace=1,
                    echo_metadata=None,
                    sensor_store_metadata=None,
                    annotation_labels=set()),
                measurement_timestamp_utc=timestamp.Timestamp.from_utc_datetime(
                    datetime.datetime(year=2020,
                                      month=1,
                                      day=1,
                                      hour=10,
                                      tzinfo=pytz.utc)),
                pressure=1),
            schemas.Pressure(
                schemas.data_point_metadata_for_raw_data(
                    data_source_id=123,
                    device_id='123',
                    participant_id='123',
                    participant_namespace=1,
                    echo_metadata=None,
                    sensor_store_metadata=None,
                    annotation_labels=set()),
                measurement_timestamp_utc=timestamp.Timestamp.from_utc_datetime(
                    datetime.datetime(year=2020,
                                      month=1,
                                      day=1,
                                      hour=15,
                                      tzinfo=pytz.utc)),
                pressure=2)
        ]).with_output_types(schemas.Pressure)

        bq_mock.return_value = FakeBigQueryClient(
            'proj.dataset.table',
            expected_rows=[{
                'DeviceID':
                    '123',
                'DataPointTime':
                    timezone_utils.timestamp_to_ms(
                        datetime.datetime(year=2020,
                                          month=1,
                                          day=1,
                                          hour=10,
                                          tzinfo=pytz.utc)),
                'DataPoint': {
                    'pressure': 1
                },
                'DataSource': {
                    'name': 'Ds_Sdk_Derived',
                    'application': None,
                    'device': {
                        'serial_number': '',
                        'name': '',
                        'hardware_version': '',
                        'firmware_version': '',
                        'software_version': '',
                        'time_zone_name': 'US/Eastern',
                        'manufacturer': '',
                        'model': '',
                        'os_version': '',
                        'data_session_id': '',
                        'android_metadata': None,
                        'daylight_saving_time': 0
                    },
                    'data_spec': None,
                    'sensor': None,
                    'algorithm': None,
                    'registry': ''
                }
            }, {
                'DeviceID':
                    '123',
                'DataPointTime':
                    timezone_utils.timestamp_to_ms(
                        datetime.datetime(year=2020,
                                          month=1,
                                          day=1,
                                          hour=15,
                                          tzinfo=pytz.utc)),
                'DataPoint': {
                    'pressure': 2
                },
                'DataSource': {
                    'name': 'Ds_Sdk_Derived',
                    'application': None,
                    'device': {
                        'serial_number': '',
                        'name': '',
                        'hardware_version': '',
                        'firmware_version': '',
                        'software_version': '',
                        'time_zone_name': 'US/Eastern',
                        'manufacturer': '',
                        'model': '',
                        'os_version': '',
                        'data_session_id': '',
                        'android_metadata': None,
                        'daylight_saving_time': 0
                    },
                    'data_spec': None,
                    'sensor': None,
                    'algorithm': None,
                    'registry': ''
                }
            }],
            expected_schema=_EXPECTED_BQ_SCHEMA)

        with beam.Pipeline(beam.runners.direct.DirectRunner()) as p:
            _ = (p | input_data |
                 beam.WindowInto(beam.window.FixedWindows(60 * 60)) |
                 data_points_sink.WriteDataPointsToBigQuery(
                     table_id='proj.dataset.table',
                     project_id='proj',
                     schema=schemas.Pressure,
                     creds=FakeDsSdkCredentials(
                         runner='', service_account='', billing_project=''),
                     temp_gcs_bucket='bucket',
                     data_source_cache=self.data_source_cache,
                     streaming=False,
                     bigquery_location='US').with_input_types(
                         Union[beam.window.TimestampedValue, schemas.Pressure]))

    @mock.patch(
        'verily.ds_sdk.core.io.bigquery.utils.bigquery_sink_utils.get_bigquery_client'  # pylint: disable=line-too-long
    )
    def test_streaming_writes(self, bq_mock):
        bq_mock.return_value = FakeBigQueryClient(
            'proj.dataset.table',
            expected_rows=[{
                'DeviceID': '123',
                'DataPointTime': datetime.datetime(year=2020, month=1, day=1),
                'DataPoint': {
                    'pressure': 1
                },
                'DataSource': {
                    'name': 'Ds_Sdk_Derived',
                    'device': {
                        'time_zone_name': 'US/Eastern',
                        'serial_number': '',
                        'name': '',
                        'hardware_version': '',
                        'firmware_version': '',
                        'software_version': '',
                        'manufacturer': '',
                        'model': '',
                        'os_version': '',
                        'data_session_id': '',
                        'daylight_saving_time': 0
                    },
                    'registry': ''
                }
            }],
            expected_schema=_EXPECTED_BQ_SCHEMA,
            table=FakeBigqueryTable(_EXPECTED_BQ_SCHEMA))

        with beam.Pipeline(beam.runners.direct.DirectRunner()) as p:
            _ = (p | self.input_data |
                 data_points_sink.WriteDataPointsToBigQuery(
                     table_id='proj.dataset.table',
                     project_id='proj',
                     schema=schemas.Pressure,
                     creds=FakeDsSdkCredentials(
                         runner='', service_account='', billing_project=''),
                     temp_gcs_bucket='bucket',
                     data_source_cache=self.data_source_cache,
                     write_disposition=bigquery.WriteDisposition.WRITE_APPEND,
                     streaming=True,
                     bigquery_location='US').with_input_types(schemas.Pressure))


if __name__ == '__main__':
    unittest.main()
