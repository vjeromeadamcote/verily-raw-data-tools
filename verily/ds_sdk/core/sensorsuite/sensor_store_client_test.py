"""Tests for sensor_store_client.py"""

import dataclasses
import datetime
from typing import Any, Dict, List, Optional
import unittest
from unittest import mock

from apache_beam.utils import timestamp
from googleapiclient import errors
import pandas as pd

from verily.ds_sdk.core import schemas
from verily.ds_sdk.core.io.data_source_cache import DataSourceCache
from verily.ds_sdk.core.sensorsuite import overwrite_keys
from verily.ds_sdk.core.sensorsuite import sensor_store_client
from verily.ds_sdk.core.utils import timestamps
from verily.ds_sdk.protos import types_pb2


class FakeSensorStoreService:

    def __init__(self, data_mock):
        self.data_mock = data_mock

    def data(self):
        return self.data_mock


class FakeCreds:

    def get_impersonated_credentials(self):
        return None


@dataclasses.dataclass
class AllDataTypes(schemas.DataPoint):
    """Beam RowSchema for com.verily.pressure."""
    int_field: int
    string_field: str
    bool_field: bool
    float_field: float
    bytes_field: bytes
    timestamp_field: timestamp.Timestamp
    list_field: List[str]
    optional_field: Optional[int]


def create_input_data_point(data_source_id: int, device_id: str):
    return AllDataTypes(
        schemas.data_point_metadata_for_raw_data(data_source_id=data_source_id,
                                                 device_id=device_id,
                                                 participant_id='123',
                                                 participant_namespace=1,
                                                 echo_metadata=None,
                                                 sensor_store_metadata=None,
                                                 annotation_labels=set()),
        measurement_timestamp_utc=timestamps.datetime_to_beam_timestamp(
            datetime.datetime(year=2020, month=1, day=1)),
        int_field=1,
        string_field='str',
        bool_field=True,
        float_field=1.1,
        bytes_field='byte_string'.encode(),
        timestamp_field=timestamps.datetime_to_beam_timestamp(
            datetime.datetime(year=2020, month=1, day=1)),
        list_field=['a', 'b'],
        optional_field=None)


def create_expected_overwrite_http_request() -> Dict[str, Any]:
    return {
        'new_data': [{
            'overwrite_token': {
                'overwrite_key': 'key',
                'source_data_version': 'version'
            },
            'datasets': [{
                'source': {
                    'name':
                        'Ds_Sdk_Derived',
                    'device': {
                        'serial_number': '123',
                        'name': '',
                        'hardware_version': '',
                        'firmware_version': '',
                        'software_version': '',
                        'time_zone_name': '',
                        'manufacturer': '',
                        'model': '',
                        'os_version': '',
                        'data_session_id': '',
                        # 'daylight_saving_time': 0,
                    },
                    'data_spec': {
                        'name': 'com.verily.pressure',
                        'field_specs': [],
                        'canonical_data_spec': ''
                    },
                    'algorithm': {
                        'name': 'algo',
                        'version': 'version'
                    },
                    'registry':
                        'registries/e86e5bca-cee6-3ab4-ffab-e80ab456812e',
                    'application': {
                        'id': 'STUDY_KIT'
                    }
                },
                'data_points': [{
                    # 'measurement_time_millis':
                    #     123,
                    'fields': [{
                        'field_name': 'int_field',
                        'int64_value': 1
                    }, {
                        'field_name': 'string_field',
                        'string_value': 'str'
                    }, {
                        'field_name': 'bool_field',
                        'boolean_value': True
                    }, {
                        'field_name': 'float_field',
                        'float64_value': 1.1
                    }, {
                        'field_name': 'bytes_field',
                        'blob': 'Ynl0ZV9zdHJpbmc='
                    }, {
                        'field_name': 'timestamp_field',
                        'int64_value': 1577836800000
                    }, {
                        'field_name': 'list_field',
                        'string_list': {
                            'values': ['a', 'b']
                        }
                    }],
                    'measurement_time_millis': 1577836800000
                }],
            }]
        }]
    }


def create_input_data_source(data_spec_name: str, algo_name: str,
                             algo_version: str,
                             device_id: str) -> types_pb2.DataSource:
    return types_pb2.DataSource(
        name='my_data_source',
        data_spec=types_pb2.DataSpec(name=data_spec_name),
        algorithm=types_pb2.Algorithm(name=algo_name, version=algo_version),
        device=types_pb2.Device(serial_number=device_id))


class FakeResponse:

    def __init__(self, status):
        self.status = status
        self.reason = 'failed'


class SensorStoreClientTest(unittest.TestCase):

    def setUp(self):
        self.data_spec_name = 'com.verily.pressure'
        self.algo_name = 'algo'
        self.algo_version = 'version'
        self.device_id = '123'

    @mock.patch(
        'verily.ds_sdk.core.sensorsuite.sensor_store_client.fetch_discovery_document', return_value={})  # pylint: disable=line-too-long
    @mock.patch(
        'verily.ds_sdk.core.sensorsuite.sensor_store_client.build_from_document')  # pylint: disable=line-too-long
    def test_overwrite_request_generation(self, service_mock, fetch_doc_mock):
        del fetch_doc_mock
        data_mock = mock.MagicMock()
        service_mock.side_effect = [FakeSensorStoreService(data_mock)]

        data_source_id = 123
        input_data_point = create_input_data_point(data_source_id,
                                                   self.device_id)
        data_sources = DataSourceCache({
            data_source_id:
                create_input_data_source(self.data_spec_name, self.algo_name,
                                         self.algo_version, self.device_id)
        })

        client = sensor_store_client.SensorStoreClient('prod', FakeCreds(),
                                                       'api_key')

        client.overwrite_data_point_batches(
            self.data_spec_name, self.algo_name, self.algo_version,
            [input_data_point], data_sources,
            overwrite_keys.OverwriteKey('key', 'version'), 'DevTeam')

        overwrite_mock = data_mock.overwriteDataPointBatches
        self.assertEqual(1, overwrite_mock.return_value.execute.call_count)

        overwrite_call = overwrite_mock.call_args
        self.assertEqual(
            overwrite_call,
            mock.call(body=create_expected_overwrite_http_request()))

    @mock.patch(
        'verily.ds_sdk.core.sensorsuite.sensor_store_client.fetch_discovery_document', return_value={})  # pylint: disable=line-too-long
    @mock.patch(
        'verily.ds_sdk.core.sensorsuite.sensor_store_client.build_from_document')  # pylint: disable=line-too-long
    def test_read_data_points_request_generation_user_id(
            self, service_mock, fetch_doc_mock):
        del fetch_doc_mock
        data_mock = mock.MagicMock()
        service_mock.side_effect = [FakeSensorStoreService(data_mock)]

        client = sensor_store_client.SensorStoreClient('prod', FakeCreds(),
                                                       'api_key')

        client.read_data_points('321', 'GAIA', 'C2Q123', 'com.verily.data',
                                pd.Timestamp('2020-01-01 12:00:00 UTC'),
                                pd.Timestamp('2020-01-01 13:00:00 UTC'))

        expected_http_request = {
            'userId': {
                # GAIA should be converted to GAIA_ID
                'keyspace': 'GAIA_ID',
                # GAIA_ID -> userId should be set instead of userString
                'userId': 321,
            },
            'timeInterval': {
                'start': pd.Timestamp('2020-01-01 12:00:00 UTC').isoformat(),
                'end': pd.Timestamp('2020-01-01 13:00:00 UTC').isoformat(),
            },
            'dataSources': {
                'dataSpec': {
                    'name': 'com.verily.data',
                },
                'device': {
                    'serialNumber': 'C2Q123',
                }
            }
        }

        read_mock = data_mock.read
        self.assertEqual(1, read_mock.return_value.execute.call_count)

        self.assertEqual(read_mock.call_args,
                         mock.call(body=expected_http_request))

    @mock.patch(
        'verily.ds_sdk.core.sensorsuite.sensor_store_client.fetch_discovery_document', return_value={})  # pylint: disable=line-too-long
    @mock.patch(
        'verily.ds_sdk.core.sensorsuite.sensor_store_client.build_from_document')  # pylint: disable=line-too-long
    def test_read_data_points_request_generation_user_string(
            self, service_mock, fetch_doc_mock):
        del fetch_doc_mock
        data_mock = mock.MagicMock()
        service_mock.side_effect = [FakeSensorStoreService(data_mock)]

        client = sensor_store_client.SensorStoreClient('prod', FakeCreds(),
                                                       'api_key')

        client.read_data_points('321', 'CSP', 'C2Q123', 'com.verily.data',
                                pd.Timestamp('2020-01-01 12:00:00 UTC'),
                                pd.Timestamp('2020-01-01 13:00:00 UTC'))

        expected_http_request = {
            'userId': {
                # CSP should be converted to CSP_INTERNAL_ID
                'keyspace': 'CSP_INTERNAL_ID',
                # CSP_INTERNAL_ID -> userString should be set instead of userId
                'userString': '321',
            },
            'timeInterval': {
                'start': pd.Timestamp('2020-01-01 12:00:00 UTC').isoformat(),
                'end': pd.Timestamp('2020-01-01 13:00:00 UTC').isoformat(),
            },
            'dataSources': {
                'dataSpec': {
                    'name': 'com.verily.data',
                },
                'device': {
                    'serialNumber': 'C2Q123',
                }
            }
        }

        read_mock = data_mock.read
        self.assertEqual(1, read_mock.return_value.execute.call_count)

        self.assertEqual(read_mock.call_args,
                         mock.call(body=expected_http_request))

    @mock.patch(
        'verily.ds_sdk.core.sensorsuite.sensor_store_client.fetch_discovery_document', return_value={})  # pylint: disable=line-too-long
    @mock.patch(
        'verily.ds_sdk.core.sensorsuite.sensor_store_client.build_from_document')  # pylint: disable=line-too-long
    def test_errors_retried(self, service_mock, fetch_doc_mock):
        del fetch_doc_mock
        data_mock = mock.MagicMock()

        data_mock.overwriteDataPointBatches.return_value.execute.side_effect = [
            errors.HttpError(resp=FakeResponse(302), content=''.encode()),
            errors.HttpError(resp=FakeResponse(405), content=''.encode()),
            errors.HttpError(resp=FakeResponse(429), content=''.encode()),
            errors.HttpError(resp=FakeResponse(500), content=''.encode()),
            errors.HttpError(resp=FakeResponse(502), content=''.encode()),
            errors.HttpError(resp=FakeResponse(503), content=''.encode()),
            'success',
        ]

        service_mock.side_effect = [FakeSensorStoreService(data_mock)]

        data_source_id = 123
        input_data_point = create_input_data_point(data_source_id,
                                                   self.device_id)
        data_sources = DataSourceCache({
            data_source_id:
                create_input_data_source(self.data_spec_name, self.algo_name,
                                         self.algo_version, self.device_id)
        })

        client = sensor_store_client.SensorStoreClient('prod', FakeCreds(),
                                                       'api_key')
        # set backoff time to 0 to speed up test.
        sensor_store_client._RETRY_BACK_OFF_UPPER_BOUND_SECS = 0  # pylint: disable=protected-access

        client.overwrite_data_point_batches(
            self.data_spec_name, self.algo_name, self.algo_version,
            [input_data_point], data_sources,
            overwrite_keys.OverwriteKey('key', 'version'), 'DevTeam')

        overwrite_mock = data_mock.overwriteDataPointBatches
        self.assertEqual(7, overwrite_mock.return_value.execute.call_count)

    @mock.patch(
        'verily.ds_sdk.core.sensorsuite.sensor_store_client.fetch_discovery_document', return_value={})  # pylint: disable=line-too-long
    @mock.patch(
        'verily.ds_sdk.core.sensorsuite.sensor_store_client.build_from_document')  # pylint: disable=line-too-long
    def test_errors_timeout_reached(self, service_mock, fetch_doc_mock):
        del fetch_doc_mock
        data_mock = mock.MagicMock()

        data_mock.overwriteDataPointBatches.return_value.execute.side_effect = [
            errors.HttpError(resp=FakeResponse(302), content=''.encode())
        ]
        data_mock.overwriteDataPointBatches.return_value.body = (
            '{"serial_number":'
            ' "123"}')

        service_mock.side_effect = [FakeSensorStoreService(data_mock)]

        data_source_id = 123
        input_data_point = create_input_data_point(data_source_id,
                                                   self.device_id)
        data_sources = DataSourceCache({
            data_source_id:
                create_input_data_source(self.data_spec_name, self.algo_name,
                                         self.algo_version, self.device_id)
        })

        client = sensor_store_client.SensorStoreClient(
            'prod',
            FakeCreds(),
            'api_key',
            request_retry_timeout=pd.Timedelta('1s'))
        # set backoff time to 1 to speed up test.
        sensor_store_client._RETRY_BACK_OFF_UPPER_BOUND_SECS = 1  # pylint: disable=protected-access

        with self.assertRaises(sensor_store_client.SensorStoreError) as ctx:
            client.overwrite_data_point_batches(
                self.data_spec_name, self.algo_name, self.algo_version,
                [input_data_point], data_sources,
                overwrite_keys.OverwriteKey('key', 'version'), 'DevTeam')

        self.assertEqual(ctx.exception.device_id, '123')

    @mock.patch(
        'verily.ds_sdk.core.sensorsuite.sensor_store_client.fetch_discovery_document', return_value={})  # pylint: disable=line-too-long
    @mock.patch(
        'verily.ds_sdk.core.sensorsuite.sensor_store_client.build_from_document')  # pylint: disable=line-too-long
    def test_non_retryable_error_thrown(self, service_mock, fetch_doc_mock):
        del fetch_doc_mock
        data_mock = mock.MagicMock()

        data_mock.overwriteDataPointBatches.return_value.execute.side_effect = [
            errors.HttpError(resp=FakeResponse(600), content=''.encode())
        ]
        data_mock.overwriteDataPointBatches.return_value.body = (
            '{"serial_number":'
            ' "123"}')

        service_mock.side_effect = [FakeSensorStoreService(data_mock)]

        data_source_id = 123
        input_data_point = create_input_data_point(data_source_id,
                                                   self.device_id)
        data_sources = DataSourceCache({
            data_source_id:
                create_input_data_source(self.data_spec_name, self.algo_name,
                                         self.algo_version, self.device_id)
        })

        client = sensor_store_client.SensorStoreClient(
            'prod',
            FakeCreds(),
            'api_key',
            request_retry_timeout=pd.Timedelta('1s'))

        with self.assertRaises(sensor_store_client.SensorStoreError) as ctx:
            client.overwrite_data_point_batches(
                self.data_spec_name, self.algo_name, self.algo_version,
                [input_data_point], data_sources,
                overwrite_keys.OverwriteKey('key', 'version'), 'DevTeam')

        self.assertEqual(ctx.exception.device_id, '123')

    @mock.patch(
        'verily.ds_sdk.core.sensorsuite.sensor_store_client.fetch_discovery_document', return_value={})  # pylint: disable=line-too-long
    @mock.patch(
        'verily.ds_sdk.core.sensorsuite.sensor_store_client.build_from_document')  # pylint: disable=line-too-long
    def test_no_matching_data_source(self, service_mock, fetch_doc_mock):
        del fetch_doc_mock
        data_mock = mock.MagicMock()
        service_mock.side_effect = [FakeSensorStoreService(data_mock)]

        data_source_id = 123
        input_data_point = create_input_data_point(data_source_id,
                                                   self.device_id)
        data_sources = DataSourceCache({
            data_source_id:
                create_input_data_source(self.data_spec_name, self.algo_name,
                                         self.algo_version, self.device_id)
        })

        client = sensor_store_client.SensorStoreClient(
            'prod',
            FakeCreds(),
            'api_key',
            request_retry_timeout=pd.Timedelta('1s'))

        client.overwrite_data_point_batches(
            self.data_spec_name, self.algo_name, self.algo_version,
            [input_data_point], data_sources,
            overwrite_keys.OverwriteKey('key', 'version'), 'DevTeam')

        overwrite_mock = data_mock.overwriteDataPointBatches
        self.assertEqual(1, overwrite_mock.return_value.execute.call_count)

        want_body = {
            'new_data': [{
                'overwrite_token': {
                    'overwrite_key': 'key',
                    'source_data_version': 'version'
                },
                'datasets': [{
                    'source': {
                        'name':
                            'Ds_Sdk_Derived',
                        'device': {
                            'serial_number': '123',
                            'name': '',
                            'hardware_version': '',
                            'firmware_version': '',
                            'software_version': '',
                            'time_zone_name': '',
                            'manufacturer': '',
                            'model': '',
                            'os_version': '',
                            'data_session_id': '',
                            # 'daylight_saving_time': 0
                        },
                        'data_spec': {
                            'name': self.data_spec_name,
                            'field_specs': [],
                            'canonical_data_spec': ''
                        },
                        'algorithm': {
                            'name': self.algo_name,
                            'version': self.algo_version
                        },
                        'registry':
                            'registries/e86e5bca-cee6-3ab4-ffab-e80ab456812e',
                        'application': {
                            'id': 'STUDY_KIT'
                        }
                    },
                    'data_points': [{
                        # 'measurement_time_millis':
                        #     123,
                        'fields': [{
                            'field_name': 'int_field',
                            'int64_value': 1
                        }, {
                            'field_name': 'string_field',
                            'string_value': 'str'
                        }, {
                            'field_name': 'bool_field',
                            'boolean_value': True
                        }, {
                            'field_name': 'float_field',
                            'float64_value': 1.1
                        }, {
                            'field_name': 'bytes_field',
                            'blob': 'Ynl0ZV9zdHJpbmc='
                        }, {
                            'field_name': 'timestamp_field',
                            'int64_value': 1577836800000
                        }, {
                            'field_name': 'list_field',
                            'string_list': {
                                'values': ['a', 'b']
                            }
                        }],
                        'measurement_time_millis': 1577836800000
                    }],
                }]
            }]
        }

        overwrite_call = overwrite_mock.call_args
        self.assertEqual(overwrite_call, mock.call(body=want_body))


if __name__ == '__main__':
    unittest.main()
