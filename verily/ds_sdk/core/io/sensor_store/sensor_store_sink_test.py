"""Tests for overwrite_keys.py"""

from dataclasses import dataclass
import json
import os
import shutil
import tempfile
import time
import unittest
from unittest import mock

import apache_beam as beam
from apache_beam.testing.test_pipeline import TestPipeline
from apache_beam.testing.util import assert_that
from apache_beam.testing.util import equal_to
import pandas as pd

from verily.ds_sdk.core import options
from verily.ds_sdk.core import schemas
from verily.ds_sdk.core.gcp import credentials
from verily.ds_sdk.core.io.data_source_cache import DataSourceCache
from verily.ds_sdk.core.io.sensor_store import sensor_store_sink
from verily.ds_sdk.core.schemas import dataspec
from verily.ds_sdk.core.sensorsuite import overwrite_keys
from verily.ds_sdk.core.sensorsuite import sensor_store_client
from verily.ds_sdk.core.utils import timestamps


@dataspec('com.verily.pressure')
@dataclass
class CustomPressure(schemas.DataPoint):
    pressure: int


class FakeCreds(credentials.DsSdkCredentials):

    def __init__(self):
        super().__init__('runner', 'service_account', 'billing_project')

    def get_credentials(self):
        return None, None


def create_pressure_data_point(
        timestamp: pd.Timestamp = pd.Timestamp('2020-01-01'),
        device_id: str = 'device') -> schemas.Pressure:
    return CustomPressure(
        data_point_metadata=schemas.data_point_metadata_for_raw_data(
            data_source_id='123',
            device_id=device_id,
            participant_id='part',
            participant_namespace=1,
            echo_metadata=None,
            sensor_store_metadata=None,
            annotation_labels=set()),
        measurement_timestamp_utc=timestamps.datetime_to_beam_timestamp(
            timestamp),
        pressure=1)


class SensorStoreSinkTest(unittest.TestCase):

    def setUp(self):
        self.temp_path = tempfile.mkdtemp()

    def tearDown(self):
        shutil.rmtree(self.temp_path)

    def test_group_by_overwrite_key_device(self):
        input_data_point = [create_pressure_data_point()]

        want = [((overwrite_keys.OverwriteKey(
            'com.verily.pressure:my_algo:v1:1577836800.0-1577840400.0'),
                  'device'), input_data_point)]

        with TestPipeline() as p:
            got = (
                p | beam.Create(input_data_point) | beam.ParDo(
                    sensor_store_sink._KeyByOverwriteKeyAndDevice(  # pylint: disable=protected-access
                        algorithm_name='my_algo',
                        algorithm_version='v1',
                        overwrite_key_generator=overwrite_keys.
                        OverWriteKeyGenerators.TimeWindow('1h')),
                    DataSourceCache({})) | beam.GroupByKey())

            assert_that(got, equal_to(want))

    @mock.patch('verily.ds_sdk.core.sensorsuite.sensor_store_client.SensorStoreClient')  # pylint: disable=line-too-long
    def test_write_to_sensor_store(self, sensor_store_mock: mock.MagicMock):
        device1_hour1 = [
            create_pressure_data_point(
                device_id='device1',
                timestamp=pd.Timestamp('2020-01-01 12:00:00')),
            create_pressure_data_point(
                device_id='device1',
                timestamp=pd.Timestamp('2020-01-01 12:01:00')),
        ]
        device1_hour2 = [
            create_pressure_data_point(
                device_id='device1',
                timestamp=pd.Timestamp('2020-01-01 13:00:00')),
            create_pressure_data_point(
                device_id='device1',
                timestamp=pd.Timestamp('2020-01-01 13:01:00')),
        ]
        device2_hour1 = [
            create_pressure_data_point(
                device_id='device2',
                timestamp=pd.Timestamp('2020-01-01 12:00:00')),
            create_pressure_data_point(
                device_id='device2',
                timestamp=pd.Timestamp('2020-01-01 12:01:00')),
        ]
        device2_hour2 = [
            create_pressure_data_point(
                device_id='device2',
                timestamp=pd.Timestamp('2020-01-01 13:00:00')),
            create_pressure_data_point(
                device_id='device2',
                timestamp=pd.Timestamp('2020-01-01 13:01:00'))
        ]
        input_data_point = (device1_hour1 + device1_hour2 + device2_hour1 +
                            device2_hour2)

        with TestPipeline() as p:
            _ = (p | beam.Create(input_data_point) |
                 sensor_store_sink.SensorStoreSink(
                     schema=CustomPressure,
                     algorithm_name='algo_name',
                     algorithm_version='algo_version',
                     overwrite_key_generator=overwrite_keys.
                     OverWriteKeyGenerators.TimeWindow('1h'),
                     data_source_cache=DataSourceCache({}),
                     env='prod',
                     creds=FakeCreds(),
                     api_key='api_key',
                     study='DevTeam',
                     request_retry_timeout=pd.Timedelta('1h'),
                     fail_fast=True,
                     log_process_time_metrics=False,
                     incremental_options=None,
                     billing_project='project',
                     dataflow_job_name=None,
                     streaming=False,
                     global_qps_limit=1,
                     dataflow_region=None))

        want_calls = [
            mock.call(
                'com.verily.pressure',
                'algo_name',
                'algo_version',
                device1_hour1,
                DataSourceCache({}),
                overwrite_keys.OverwriteKey(
                    key=
                    'com.verily.pressure:algo_name:algo_version:1577880000.0-1577883600.0',  # pylint: disable=line-too-long
                    version=None),
                'DevTeam'),
            mock.call(
                'com.verily.pressure',
                'algo_name',
                'algo_version',
                device1_hour2,
                DataSourceCache({}),
                overwrite_keys.OverwriteKey(
                    key=
                    'com.verily.pressure:algo_name:algo_version:1577883600.0-1577887200.0',  # pylint: disable=line-too-long
                    version=None),
                'DevTeam'),
            mock.call(
                'com.verily.pressure',
                'algo_name',
                'algo_version',
                device2_hour1,
                DataSourceCache({}),
                overwrite_keys.OverwriteKey(
                    key=
                    'com.verily.pressure:algo_name:algo_version:1577880000.0-1577883600.0',  # pylint: disable=line-too-long
                    version=None),
                'DevTeam'),
            mock.call(
                'com.verily.pressure',
                'algo_name',
                'algo_version',
                device2_hour2,
                DataSourceCache({}),
                overwrite_keys.OverwriteKey(
                    key=
                    'com.verily.pressure:algo_name:algo_version:1577883600.0-1577887200.0',  # pylint: disable=line-too-long
                    version=None),
                'DevTeam')
        ]

        sensor_store_mock.return_value.overwrite_data_point_batches.assert_has_calls(  # pylint: disable=line-too-long
            want_calls)

    @mock.patch('verily.ds_sdk.core.sensorsuite.sensor_store_client.SensorStoreClient')  # pylint: disable=line-too-long
    def test_write_to_sensor_store_no_fail_fast(
            self, sensor_store_mock: mock.MagicMock):
        device1_hour1 = [
            create_pressure_data_point(
                device_id='device1',
                timestamp=pd.Timestamp('2020-01-01 12:00:00')),
            create_pressure_data_point(
                device_id='device1',
                timestamp=pd.Timestamp('2020-01-01 12:01:00')),
        ]
        device1_hour2 = [
            create_pressure_data_point(
                device_id='device1',
                timestamp=pd.Timestamp('2020-01-01 13:00:00')),
            create_pressure_data_point(
                device_id='device1',
                timestamp=pd.Timestamp('2020-01-01 13:01:00')),
        ]

        input_data_point = device1_hour1 + device1_hour2

        batch_mock = sensor_store_mock.return_value.overwrite_data_point_batches
        batch_mock.side_effect = [
            sensor_store_client.SensorStoreError('write failed', 'device1'),
            'passed'
        ]

        with self.assertRaisesRegex(
                ValueError,
                'SensorStore writes failed. All errors have been dumped to the '
                'logs.'):
            with TestPipeline() as p:
                _ = (p | beam.Create(input_data_point) |
                     sensor_store_sink.SensorStoreSink(
                         schema=CustomPressure,
                         algorithm_name='algo_name',
                         algorithm_version='algo_version',
                         overwrite_key_generator=overwrite_keys.
                         OverWriteKeyGenerators.TimeWindow('1h'),
                         data_source_cache=DataSourceCache({}),
                         env='prod',
                         creds=FakeCreds(),
                         api_key='api_key',
                         study='DevTeam',
                         request_retry_timeout=pd.Timedelta('1h'),
                         fail_fast=False,
                         log_process_time_metrics=False,
                         incremental_options=None,
                         billing_project='project',
                         dataflow_job_name=None,
                         streaming=False,
                         global_qps_limit=1,
                         dataflow_region=None))

        want_calls = [
            mock.call(
                'com.verily.pressure',
                'algo_name',
                'algo_version',
                device1_hour1,
                DataSourceCache({}),
                overwrite_keys.OverwriteKey(
                    key=
                    'com.verily.pressure:algo_name:algo_version:1577880000.0-1577883600.0',  # pylint: disable=line-too-long
                    version=None),
                'DevTeam'),
            mock.call(
                'com.verily.pressure',
                'algo_name',
                'algo_version',
                device1_hour2,
                DataSourceCache({}),
                overwrite_keys.OverwriteKey(
                    key=
                    'com.verily.pressure:algo_name:algo_version:1577883600.0-1577887200.0',  # pylint: disable=line-too-long
                    version=None),
                'DevTeam'),
        ]

        # Verify that mock was called with both devices even though one failed.
        sensor_store_mock.return_value.overwrite_data_point_batches.assert_has_calls(  # pylint: disable=line-too-long
            want_calls)

    @mock.patch('verily.ds_sdk.core.sensorsuite.sensor_store_client.SensorStoreClient')  # pylint: disable=line-too-long
    @mock.patch('pandas.Timestamp.now', return_value=pd.Timestamp('2020-01-01'))
    def test_write_to_sensor_store_state_updated(
            self, time_mock, sensor_store_mock: mock.MagicMock):
        del time_mock
        del sensor_store_mock
        device1_hour1 = [
            create_pressure_data_point(
                device_id='device1',
                timestamp=pd.Timestamp('2020-01-01 12:00:00')),
            create_pressure_data_point(
                device_id='device1',
                timestamp=pd.Timestamp('2020-01-01 12:01:00')),
        ]
        device1_hour2 = [
            create_pressure_data_point(
                device_id='device1',
                timestamp=pd.Timestamp('2020-01-01 13:00:00')),
            create_pressure_data_point(
                device_id='device1',
                timestamp=pd.Timestamp('2020-01-01 13:01:00')),
        ]

        input_data_point = device1_hour1 + device1_hour2

        inc_options = options.IncrementalQueryOptions(
            state_file_path=self.temp_path)
        inc_options.write_end_time = pd.Timestamp('2020-01-01')

        with TestPipeline() as p:
            _ = (p | beam.Create(input_data_point) |
                 sensor_store_sink.SensorStoreSink(
                     schema=CustomPressure,
                     algorithm_name='algo_name',
                     algorithm_version='algo_version',
                     overwrite_key_generator=overwrite_keys.
                     OverWriteKeyGenerators.TimeWindow('1h'),
                     data_source_cache=DataSourceCache({}),
                     env='prod',
                     creds=FakeCreds(),
                     api_key='api_key',
                     study='DevTeam',
                     request_retry_timeout=pd.Timedelta('1h'),
                     fail_fast=False,
                     log_process_time_metrics=False,
                     incremental_options=inc_options,
                     billing_project='project',
                     dataflow_job_name=None,
                     streaming=False,
                     global_qps_limit=1,
                     dataflow_region=None))

        with open(os.path.join(self.temp_path, 'DevTeam.json'),
                  encoding='utf-8') as f:
            state = json.load(f)
            self.assertEqual(state,
                             {'last_write_end_time': '2020-01-01 00:00:00'})



    @mock.patch('verily.ds_sdk.core.sensorsuite.sensor_store_client.SensorStoreClient')  # pylint: disable=line-too-long
    def test_write_to_sensor_store_fail_fast(self,
                                             sensor_store_mock: mock.MagicMock):
        device1_hour1 = [
            create_pressure_data_point(
                device_id='device1',
                timestamp=pd.Timestamp('2020-01-01 12:00:00')),
            create_pressure_data_point(
                device_id='device1',
                timestamp=pd.Timestamp('2020-01-01 12:01:00')),
        ]
        device1_hour2 = [
            create_pressure_data_point(
                device_id='device1',
                timestamp=pd.Timestamp('2020-01-01 13:00:00')),
            create_pressure_data_point(
                device_id='device1',
                timestamp=pd.Timestamp('2020-01-01 13:01:00')),
        ]

        input_data_point = device1_hour1 + device1_hour2

        batch_mock = sensor_store_mock.return_value.overwrite_data_point_batches
        batch_mock.side_effect = [
            sensor_store_client.SensorStoreError('write failed', 'device1'),
            'passed'
        ]

        with self.assertRaisesRegex(
                ValueError, 'SensorStore writes failed for device: device1'):
            with TestPipeline() as p:
                _ = (p | beam.Create(input_data_point) |
                     sensor_store_sink.SensorStoreSink(
                         schema=CustomPressure,
                         algorithm_name='algo_name',
                         algorithm_version='algo_version',
                         overwrite_key_generator=overwrite_keys.
                         OverWriteKeyGenerators.TimeWindow('1h'),
                         data_source_cache=DataSourceCache({}),
                         env='prod',
                         creds=FakeCreds(),
                         api_key='api_key',
                         study='DevTeam',
                         request_retry_timeout=pd.Timedelta('1h'),
                         fail_fast=True,
                         log_process_time_metrics=False,
                         incremental_options=None,
                         billing_project='project',
                         dataflow_job_name=None,
                         streaming=False,
                         global_qps_limit=1,
                         dataflow_region=None))

        want_calls = [
            mock.call(
                'com.verily.pressure',
                'algo_name',
                'algo_version',
                device1_hour1,
                DataSourceCache({}),
                overwrite_keys.OverwriteKey(
                    key=
                    'com.verily.pressure:algo_name:algo_version:1577880000.0-1577883600.0',  # pylint: disable=line-too-long
                    version=None),
                'DevTeam'),
        ]

        # Verify that mock was called with both devices even though one failed.
        sensor_store_mock.return_value.overwrite_data_point_batches.assert_has_calls(  # pylint: disable=line-too-long
            want_calls)

    @mock.patch('verily.ds_sdk.core.sensorsuite.sensor_store_client.SensorStoreClient')  # pylint: disable=line-too-long
    def test_write_to_sensor_store_log_process_time(
            self, sensor_store_mock: mock.MagicMock):
        del sensor_store_mock

        input_data_point = create_pressure_data_point(
            device_id='device1', timestamp=pd.Timestamp('2020-01-01 12:00:00'))

        with self.assertLogs(level='INFO') as log:
            with TestPipeline() as p:
                _ = (p | beam.Create([input_data_point]) | beam.Map(
                    lambda x: beam.window.TimestampedValue(x, time.time())) |
                     beam.WindowInto(beam.window.FixedWindows(1)) |
                     sensor_store_sink.SensorStoreSink(
                         schema=CustomPressure,
                         algorithm_name='algo_name',
                         algorithm_version='algo_version',
                         overwrite_key_generator=overwrite_keys.
                         OverWriteKeyGenerators.TimeWindow('1h'),
                         data_source_cache=DataSourceCache({}),
                         env='prod',
                         creds=FakeCreds(),
                         api_key='api_key',
                         study='DevTeam',
                         request_retry_timeout=pd.Timedelta('1h'),
                         fail_fast=True,
                         log_process_time_metrics=True,
                         incremental_options=None,
                         billing_project='project',
                         dataflow_job_name=None,
                         streaming=False,
                         global_qps_limit=1,
                         dataflow_region=None))
            log_found = False
            for log_line in log.output:
                if 'pipeline process latency=' in log_line:
                    log_found = True
            if not log_found:
                raise AssertionError('Pipeline process latency log not found!')


if __name__ == '__main__':
    unittest.main()
