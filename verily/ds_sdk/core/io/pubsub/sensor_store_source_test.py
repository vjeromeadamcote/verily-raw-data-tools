"""Tests for sensor_store_source."""

import base64
import copy
import unittest
from unittest import mock

import apache_beam as beam
from apache_beam.io.gcp.pubsub import PubsubMessage
from apache_beam.testing.test_pipeline import TestPipeline
from apache_beam.testing.util import assert_that
from apache_beam.testing.util import equal_to
import frozendict
import pandas as pd

from verily.ds_sdk.core import options
from verily.ds_sdk.core import schemas
from verily.ds_sdk.core.io.pubsub import sensor_store_source
from verily.ds_sdk.core.transforms import key_by
from verily.ds_sdk.core.utils import timestamps
from verily.ds_sdk.core.utils import timezone_utils
from verily.ds_sdk.protos import types_pb2


class FakeDsSdkCredentials:

    def get_credentials(self):
        return (mock.MagicMock(), None)


class FakeSensorStore:
    """Fake SensorStore class."""

    def __init__(self, data_sets):
        self._data_sets = data_sets
        self.read_call_count = 0

    def read_data_points(self, *args, **kwargs):
        self.read_call_count += 1
        del kwargs
        data_spec_name = args[3]
        start_time = args[4]
        if start_time == PD_TIMESTAMP_1.floor('1h'):
            return copy.deepcopy(self._data_sets[data_spec_name])
        return {}


class FakeRedis:
    """Fake Redis client."""

    def __init__(self):
        self.ds_cache = {}

    def set(self, key, val):
        self.ds_cache[key] = val


PD_TIMESTAMP_1 = pd.Timestamp('2020-01-01 12:15:00', tz='UTC')
PD_TIMESTAMP_2 = pd.Timestamp('2020-01-01 12:20:00', tz='UTC')
BEAM_TIMESTAMP_1 = timestamps.datetime_to_beam_timestamp(PD_TIMESTAMP_1)
BEAM_TIMESTAMP_2 = timestamps.datetime_to_beam_timestamp(PD_TIMESTAMP_2)


def get_data_point_metadata(write_time_millis: int = 100000,
                            data_source_id: int = 6915976350645747950):
    return schemas.data_point_metadata_for_raw_data(
        data_source_id=data_source_id,
        device_id='123',
        participant_id='321',
        participant_namespace=1,
        echo_metadata=None,
        sensor_store_metadata=schemas.SensorStoreMetadata(
            sensor_store_write_time=timestamps.datetime_to_beam_timestamp(
                pd.Timestamp(write_time_millis, unit='ms', tz='UTC'))),
        annotation_labels=set())


def create_all_field_types_pub_sub():
    return PubsubMessage(
        data=None,
        attributes={
            'participantId':
                '321',
            'participantNamespace':
                'GAIA',
            'deviceId':
                '123',
            'dataSpecs':
                'com.verily.test_data_point.all_field_types',
            'startMillis':
                timezone_utils.timestamp_to_ms(PD_TIMESTAMP_1 -
                                               pd.Timedelta('1m')),
            'endMillis':
                timezone_utils.timestamp_to_ms(PD_TIMESTAMP_2 +
                                               pd.Timedelta('1m')),
        })


def create_all_field_types_and_pressure_pub_sub():
    return PubsubMessage(
        data=None,
        attributes={
            'participantId':
                '321',
            'participantNamespace':
                'GAIA',
            'deviceId':
                '123',
            'dataSpecs':
                'com.verily.test_data_point.all_field_types,com.verily.pressure',  # pylint: disable=line-too-long
            'startMillis':
                timezone_utils.timestamp_to_ms(PD_TIMESTAMP_1 -
                                               pd.Timedelta('1h')),
            'endMillis':
                timezone_utils.timestamp_to_ms(PD_TIMESTAMP_2 +
                                               pd.Timedelta('1h')),
        })


def create_sensor_store_data_sets(data_spec_name: str,
                                  sensor_id: str = '1',
                                  time_zone_name: str = '-08:00',
                                  write_time_millis: int = 100000):
    if data_spec_name == 'com.verily.test_data_point.all_field_types':
        return {
            'dataSets': [{
                'source': {
                    'name': 'sensorsim',
                    'application': {},
                    'sensor': {
                        'id': sensor_id
                    },
                    'device': {
                        'name': 'test-device',
                        'serialNumber': 'test-device',
                        'hardwareVersion': 'sensorsim-v0',
                        'firmwareVersion': 'sensorsim-v0',
                        'timeZoneName': time_zone_name,
                    },
                },
                'dataPoints': [{
                    'measurementTimeMillis':
                        str(timezone_utils.timestamp_to_ms(PD_TIMESTAMP_1)),
                    'writeTime':
                        pd.Timestamp(
                            write_time_millis, unit='ms',
                            tz='UTC').strftime('%Y-%m-%d %H:%M:%S.%f %Z'),
                    'fields': [{
                        'fieldName': 'int_field',
                        'int64Value': '1'
                    }, {
                        'fieldName': 'int_list_field',
                        'int64List': ['1', '2']
                    }, {
                        'fieldName': 'float_field',
                        'float64Value': '1.0'
                    }, {
                        'fieldName': 'float_list_field',
                        'float64List': ['1.0', '1.1']
                    }, {
                        'fieldName': 'string_field',
                        'stringValue': 'this is a string'
                    }, {
                        'fieldName': 'string_list_field',
                        'stringList': ['this', 'is', 'a', 'string']
                    }, {
                        'fieldName': 'bool_field',
                        'booleanValue': 'true'
                    }, {
                        'fieldName': 'bool_list_field',
                        'booleanList': ['true', 'false']
                    }, {
                        'fieldName':
                            'blob_field',
                        'blobValue':
                            base64.b64encode('blob'.encode('ascii')
                                            ).decode('ascii')
                    }, {
                        'fieldName':
                            'blob_list_field',
                        'blobList': [
                            base64.b64encode(
                                'blob1'.encode('ascii')).decode('ascii'),
                            base64.b64encode(
                                'blob2'.encode('ascii')).decode('ascii'),
                        ]
                    }]
                }, {
                    'measurementTimeMillis':
                        str(timezone_utils.timestamp_to_ms(PD_TIMESTAMP_2)),
                    'writeTime':
                        pd.Timestamp(
                            write_time_millis, unit='ms',
                            tz='UTC').strftime('%Y-%m-%d %H:%M:%S.%f %Z'),
                    'fields': [{
                        'fieldName': 'int_field',
                        'int64Value': '2'
                    }, {
                        'fieldName': 'int_list_field',
                        'int64List': ['2', '3']
                    }, {
                        'fieldName': 'float_field',
                        'float64Value': '2.0'
                    }, {
                        'fieldName': 'float_list_field',
                        'float64List': ['2.0', '2.1']
                    }, {
                        'fieldName': 'string_field',
                        'stringValue': 'this is a string again'
                    }, {
                        'fieldName': 'string_list_field',
                        'stringList': ['this', 'is', 'a', 'string', 'again']
                    }, {
                        'fieldName': 'bool_field',
                        'booleanValue': 'false'
                    }, {
                        'fieldName': 'bool_list_field',
                        'booleanList': ['true', 'true']
                    }, {
                        'fieldName':
                            'blob_field',
                        'blobValue':
                            base64.b64encode('blob_again'.encode('ascii')
                                            ).decode('ascii')
                    }, {
                        'fieldName':
                            'blob_list_field',
                        'blobList': [
                            base64.b64encode(
                                'blob_again1'.encode('ascii')).decode('ascii'),
                            base64.b64encode(
                                'blob_again2'.encode('ascii')).decode('ascii'),
                        ]
                    }]
                }]
            }]
        }
    if data_spec_name == 'com.verily.pressure':
        return {
            'dataSets': [{
                'source': {
                    'name': 'sensorsim',
                    'application': {},
                    'sensor': {
                        'id': sensor_id
                    },
                    'device': {
                        'name': 'test-device',
                        'serialNumber': 'test-device',
                        'hardwareVersion': 'sensorsim-v0',
                        'firmwareVersion': 'sensorsim-v0',
                        'timeZoneName': time_zone_name,
                    },
                },
                'dataPoints': [{
                    'measurementTimeMillis':
                        str(timezone_utils.timestamp_to_ms(PD_TIMESTAMP_1)),
                    'writeTime':
                        pd.Timestamp(
                            write_time_millis, unit='ms',
                            tz='UTC').strftime('%Y-%m-%d %H:%M:%S.%f %Z'),
                    'fields': [{
                        'fieldName': 'pressure',
                        'int64Value': '10'
                    }]
                }]
            }]
        }


class DataPointsSourceTest(unittest.TestCase):

    @mock.patch('redis.Redis')
    @mock.patch('apache_beam.io.ReadFromPubSub')
    @mock.patch('verily.ds_sdk.core.sensorsuite.sensor_store_client.SensorStoreClient')  # pylint: disable=line-too-long
    def test_parse_sensor_store_response(self, sensor_store_mock, pub_sub_mock,
                                         redis_mock):
        sensor_store_mock.return_value = FakeSensorStore({
            'com.verily.test_data_point.all_field_types':
                create_sensor_store_data_sets(
                    'com.verily.test_data_point.all_field_types'),
        })

        pub_sub_mock.side_effect = [
            beam.Create([create_all_field_types_pub_sub()])
        ]

        fake_redis = FakeRedis()
        redis_mock.return_value = fake_redis

        expected_ds_cache = {
            6915976350645747950:
                types_pb2.DataSource(
                    name='sensorsim',
                    application=types_pb2.Application(),
                    sensor=types_pb2.Sensor(id='1'),
                    device=types_pb2.Device(
                        name='test-device',
                        serial_number='test-device',
                        hardware_version='sensorsim-v0',
                        firmware_version='sensorsim-v0',
                        time_zone_name='-08:00')).SerializeToString()
        }

        expected_data_points = [
            schemas.Test_Data_PointAll_Field_Types(
                data_point_metadata=get_data_point_metadata(),
                measurement_timestamp_utc=BEAM_TIMESTAMP_1,
                int_field=1,
                int_list_field=[1, 2],
                float_field=1.0,
                float_list_field=[1.0, 1.1],
                string_field='this is a string',
                string_list_field=['this', 'is', 'a', 'string'],
                bool_field=True,
                bool_list_field=[True, False],
                blob_field=b'blob',
                blob_list_field=[b'blob1', b'blob2']),
            schemas.Test_Data_PointAll_Field_Types(
                data_point_metadata=get_data_point_metadata(),
                measurement_timestamp_utc=BEAM_TIMESTAMP_2,
                int_field=2,
                int_list_field=[2, 3],
                float_field=2.0,
                float_list_field=[2.0, 2.1],
                string_field='this is a string again',
                string_list_field=['this', 'is', 'a', 'string', 'again'],
                bool_field=False,
                bool_list_field=[True, True],
                blob_field=b'blob_again',
                blob_list_field=[b'blob_again1', b'blob_again2'])
        ]

        with TestPipeline() as p:
            data_points_pcol_dict = (
                p | sensor_store_source.StreamingSensorStoreSource(
                    data_spec_names=[
                        'com.verily.test_data_point.all_field_types'
                    ],
                    registry='Testing',
                    pubsub_message_window_into=beam.WindowInto(
                        beam.window.FixedWindows(1)),
                    condition=None,
                    source_options=options.StreamingSourceOptions(
                        cache_data_source=True,
                        redis_endpoint='swelad:1337',
                        topic='topic'),
                    creds=FakeDsSdkCredentials(),
                    env='prod',
                    api_key='api_key',
                    request_retry_timeout=pd.Timedelta('1h')))

            assert_that(
                data_points_pcol_dict[
                    'com.verily.test_data_point.all_field_types'],
                equal_to(expected_data_points))

        self.assertEqual(fake_redis.ds_cache, expected_ds_cache)

    @mock.patch('apache_beam.io.ReadFromPubSub')
    @mock.patch('verily.ds_sdk.core.sensorsuite.sensor_store_client.SensorStoreClient')  # pylint: disable=line-too-long
    def test_parse_sensor_store_response_multiple_data_specs(
            self, sensor_store_mock, pub_sub_mock):
        sensor_store_mock.return_value = FakeSensorStore({
            'com.verily.pressure':
                create_sensor_store_data_sets('com.verily.pressure'),
            'com.verily.test_data_point.all_field_types':
                create_sensor_store_data_sets(
                    'com.verily.test_data_point.all_field_types'),
        })

        pub_sub_mock.side_effect = [
            beam.Create([create_all_field_types_and_pressure_pub_sub()])
        ]

        expected_data_points = [
            schemas.Test_Data_PointAll_Field_Types(
                data_point_metadata=get_data_point_metadata(),
                measurement_timestamp_utc=BEAM_TIMESTAMP_1,
                int_field=1,
                int_list_field=[1, 2],
                float_field=1.0,
                float_list_field=[1.0, 1.1],
                string_field='this is a string',
                string_list_field=['this', 'is', 'a', 'string'],
                bool_field=True,
                bool_list_field=[True, False],
                blob_field=b'blob',
                blob_list_field=[b'blob1', b'blob2']),
            schemas.Test_Data_PointAll_Field_Types(
                data_point_metadata=get_data_point_metadata(),
                measurement_timestamp_utc=BEAM_TIMESTAMP_2,
                int_field=2,
                int_list_field=[2, 3],
                float_field=2.0,
                float_list_field=[2.0, 2.1],
                string_field='this is a string again',
                string_list_field=['this', 'is', 'a', 'string', 'again'],
                bool_field=False,
                bool_list_field=[True, True],
                blob_field=b'blob_again',
                blob_list_field=[b'blob_again1', b'blob_again2']),
            schemas.Pressure(data_point_metadata=get_data_point_metadata(),
                             measurement_timestamp_utc=BEAM_TIMESTAMP_1,
                             pressure=10),
        ]

        with TestPipeline() as p:
            data_points_pcol_dict = (
                p | sensor_store_source.StreamingSensorStoreSource(
                    data_spec_names=[
                        'com.verily.test_data_point.all_field_types',
                        'com.verily.pressure'
                    ],
                    registry='Testing',
                    pubsub_message_window_into=beam.WindowInto(
                        beam.window.FixedWindows(1)),
                    condition=None,
                    source_options=options.StreamingSourceOptions(
                        cache_data_source=False, topic='topic'),
                    creds=FakeDsSdkCredentials(),
                    env='prod',
                    api_key='api_key',
                    request_retry_timeout=pd.Timedelta('1h')))

            self.assertCountEqual(data_points_pcol_dict.keys(), [
                'com.verily.test_data_point.all_field_types',
                'com.verily.pressure'
            ])
            all_data_points = data_points_pcol_dict.values() | beam.Flatten()

            assert_that(all_data_points, equal_to(expected_data_points))

        self.assertEqual(6, sensor_store_mock.return_value.read_call_count)

    @mock.patch('redis.Redis')
    @mock.patch('apache_beam.io.ReadFromPubSub')
    @mock.patch('verily.ds_sdk.core.sensorsuite.sensor_store_client.SensorStoreClient')  # pylint: disable=line-too-long
    def test_parse_sensor_store_response_grouped(self, sensor_store_mock,
                                                 pub_sub_mock, redis_mock):
        sensor_store_mock.return_value = FakeSensorStore({
            'com.verily.test_data_point.all_field_types':
                create_sensor_store_data_sets(
                    'com.verily.test_data_point.all_field_types'),
        })

        pub_sub_mock.side_effect = [
            beam.Create([create_all_field_types_pub_sub()])
        ]

        fake_redis = FakeRedis()
        redis_mock.return_value = fake_redis

        expected_ds_cache = {
            6915976350645747950:
                types_pb2.DataSource(
                    name='sensorsim',
                    application=types_pb2.Application(),
                    sensor=types_pb2.Sensor(id='1'),
                    device=types_pb2.Device(
                        name='test-device',
                        serial_number='test-device',
                        hardware_version='sensorsim-v0',
                        firmware_version='sensorsim-v0',
                        time_zone_name='-08:00')).SerializeToString()
        }

        expected_output = [
            (key_by.Key(device_id='123',
                        participant_id='321',
                        participant_namespace=1,
                        additional_keys=frozendict.FrozenOrderedDict({
                            'data_source_id': 6915976350645747950,
                            'sensor_id': '1',
                            'start_time_range_micros': 1577880000000000,
                            'end_time_range_micros': 1577883600000000,
                        })),
             [
                 schemas.Test_Data_PointAll_Field_Types(
                     data_point_metadata=get_data_point_metadata(),
                     measurement_timestamp_utc=BEAM_TIMESTAMP_1,
                     int_field=1,
                     int_list_field=[1, 2],
                     float_field=1.0,
                     float_list_field=[1.0, 1.1],
                     string_field='this is a string',
                     string_list_field=['this', 'is', 'a', 'string'],
                     bool_field=True,
                     bool_list_field=[True, False],
                     blob_field=b'blob',
                     blob_list_field=[b'blob1', b'blob2']),
                 schemas.Test_Data_PointAll_Field_Types(
                     data_point_metadata=get_data_point_metadata(),
                     measurement_timestamp_utc=BEAM_TIMESTAMP_2,
                     int_field=2,
                     int_list_field=[2, 3],
                     float_field=2.0,
                     float_list_field=[2.0, 2.1],
                     string_field='this is a string again',
                     string_list_field=['this', 'is', 'a', 'string', 'again'],
                     bool_field=False,
                     bool_list_field=[True, True],
                     blob_field=b'blob_again',
                     blob_list_field=[b'blob_again1', b'blob_again2'])
             ])
        ]

        with TestPipeline() as p:
            data_points_pcol_dict = (
                p | sensor_store_source.StreamingSensorStoreSource(
                    data_spec_names=[
                        'com.verily.test_data_point.all_field_types'
                    ],
                    registry='Testing',
                    pubsub_message_window_into=beam.WindowInto(
                        beam.window.FixedWindows(1)),
                    condition=None,
                    source_options=options.StreamingSourceOptions(
                        cache_data_source=True,
                        redis_endpoint='swelad:1337',
                        topic='topic',
                        group_returned_points=True),
                    creds=FakeDsSdkCredentials(),
                    env='prod',
                    api_key='api_key',
                    request_retry_timeout=pd.Timedelta('1h')))

            assert_that(
                data_points_pcol_dict[
                    'com.verily.test_data_point.all_field_types'],
                equal_to(expected_output))

        self.assertEqual(fake_redis.ds_cache, expected_ds_cache)


if __name__ == '__main__':
    unittest.main()
