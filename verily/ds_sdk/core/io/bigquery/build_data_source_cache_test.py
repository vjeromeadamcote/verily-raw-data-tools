"""Tests for build_data_source_cache."""

import unittest
from unittest import mock

import apache_beam as beam
from apache_beam.testing.test_pipeline import TestPipeline
from apache_beam.testing.util import assert_that
from apache_beam.testing.util import equal_to
from apache_beam.utils.timestamp import Timestamp
import pandas as pd

from verily.ds_sdk.core import schemas
from verily.ds_sdk.core.io.bigquery import build_data_source_cache
from verily.ds_sdk.core.io.data_source_cache import DataSourceCache
from verily.ds_sdk.protos import types_pb2


def build_timestamp(timestamp_str: str) -> Timestamp:
    return Timestamp.from_utc_datetime(pd.Timestamp(timestamp_str, tz='UTC'))


def build_data_point(timestamp_str: str, device_id: str,
                     data_source_id: int) -> schemas.DataPointMetadata:
    return schemas.Pressure(
        data_point_metadata=schemas.data_point_metadata_for_raw_data(
            data_source_id=data_source_id,
            device_id=device_id,
            participant_id='12345',
            participant_namespace=1,
            echo_metadata=None,
            sensor_store_metadata=None,
            annotation_labels=set()),
        measurement_timestamp_utc=build_timestamp(timestamp_str),
        pressure=1)


class BuildDataSourceCacheTest(unittest.TestCase):

    def test_build_cache_single_device(self):

        data_points = [
            build_data_point('2020-01-01 12:00:00',
                             device_id='C2Q12345',
                             data_source_id=123),
            build_data_point('2020-01-01 13:00:00',
                             device_id='C2Q12345',
                             data_source_id=123),
        ]
        data_source_protos = [
            types_pb2.DataSource(
                name='test', device=types_pb2.Device(serial_number='C2Q12345')),
            types_pb2.DataSource(
                name='test', device=types_pb2.Device(serial_number='C2Q12345'))
        ]

        input_tuples = []
        for dp, ds in zip(data_points, data_source_protos):
            input_tuples.append((ds.SerializeToString(), dp))

        expected = [
            DataSourceCache({
                123:
                    types_pb2.DataSource(
                        name='test',
                        device=types_pb2.Device(serial_number='C2Q12345'))
            })
        ]

        with TestPipeline() as p:

            output = (p | beam.Create(input_tuples) |
                      build_data_source_cache.BuildDataSourceCache())

            assert_that(output, equal_to(expected))

    def test_build_cache_multiple_devices(self):

        data_points = [
            build_data_point('2020-01-01 12:00:00',
                             device_id='C2Q12345',
                             data_source_id=123),
            build_data_point('2020-01-01 13:00:00',
                             device_id='C2Q12345',
                             data_source_id=123),
            build_data_point('2020-01-01 13:00:00',
                             device_id='C2Q54321',
                             data_source_id=321),
        ]
        data_source_protos = [
            types_pb2.DataSource(
                name='test', device=types_pb2.Device(serial_number='C2Q12345')),
            types_pb2.DataSource(
                name='test', device=types_pb2.Device(serial_number='C2Q12345')),
            types_pb2.DataSource(
                name='test', device=types_pb2.Device(serial_number='C2Q54321'))
        ]

        input_tuples = []
        for dp, ds in zip(data_points, data_source_protos):
            input_tuples.append((ds.SerializeToString(), dp))

        expected = [
            DataSourceCache({
                123:
                    types_pb2.DataSource(
                        name='test',
                        device=types_pb2.Device(serial_number='C2Q12345')),
                321:
                    types_pb2.DataSource(
                        name='test',
                        device=types_pb2.Device(serial_number='C2Q54321')),
            })
        ]

        with TestPipeline() as p:

            output = (p | beam.Create(input_tuples) |
                      build_data_source_cache.BuildDataSourceCache())

            assert_that(output, equal_to(expected))


class BuildDataSourceCacheFromInternalTest(unittest.TestCase):

    @mock.patch(
        'verily.ds_sdk.core.gcp.bigquery_source_wrapper.GcpBigquerySourceWrapper'  # pylint: disable=line-too-long
    )
    def test_build_cache_single_device(self, bq_source_mock):
        data_source_table_row = {
            'DataSourceID': 123,
            'DataSource': {
                'name': 'data_source_name',
                'device': {
                    'serial_number': 'C2Q12345',
                }
            },
        }
        bq_source_mock.side_effect = [beam.Create([data_source_table_row])]

        expected = [
            DataSourceCache({
                123:
                    types_pb2.DataSource(
                        name='data_source_name',
                        device=types_pb2.Device(serial_number='C2Q12345'),
                        data_spec=types_pb2.DataSpec())
            })
        ]

        with TestPipeline() as p:

            output = (p |
                      build_data_source_cache.BuildDataSourceCacheFromInternal(
                          'bigquery_table_id', 'project_id', 'service_account',
                          None, 'bigquery_location', 'data_spec_name'))

            assert_that(output, equal_to(expected))

    @mock.patch(
        'verily.ds_sdk.core.gcp.bigquery_source_wrapper.GcpBigquerySourceWrapper'  # pylint: disable=line-too-long
    )
    def test_build_cache_multiple_devices(self, bq_source_mock):
        data_source_table_rows = [{
            'DataSourceID': 123,
            'DataSource': {
                'name': 'data_source_name',
                'device': {
                    'serial_number': 'C2Q12345',
                }
            },
        }, {
            'DataSourceID': 321,
            'DataSource': {
                'name': 'data_source_name',
                'device': {
                    'serial_number': 'C2Q54321',
                }
            },
        }]
        bq_source_mock.side_effect = [beam.Create(data_source_table_rows)]

        expected = [
            DataSourceCache({
                123:
                    types_pb2.DataSource(
                        name='data_source_name',
                        device=types_pb2.Device(serial_number='C2Q12345'),
                        data_spec=types_pb2.DataSpec()),
                321:
                    types_pb2.DataSource(
                        name='data_source_name',
                        device=types_pb2.Device(serial_number='C2Q54321'),
                        data_spec=types_pb2.DataSpec()),
            })
        ]

        with TestPipeline() as p:

            output = (p |
                      build_data_source_cache.BuildDataSourceCacheFromInternal(
                          'bigquery_table_id', 'project_id', 'service_account',
                          None, 'bigquery_location', 'data_spec_name'))

            assert_that(output, equal_to(expected))


if __name__ == '__main__':
    unittest.main()
