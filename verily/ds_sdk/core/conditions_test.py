"""Tests ds_sdk.core.conditions.py."""

from typing import Dict, Optional, Set, Tuple
import unittest

from apache_beam.io.gcp.pubsub import PubsubMessage
import ibis
import ibis_bigquery
import pandas as pd
from parameterized import parameterized
import pytz

from verily.ds_sdk.core import conditions
from verily.ds_sdk.core import schemas
from verily.ds_sdk.core.io.data_source_cache import DataSourceCache
from verily.ds_sdk.core.utils import timestamps
from verily.ds_sdk.protos import enums_pb2
from verily.ds_sdk.protos import types_pb2

_TIME_RANGE_CONDITION = conditions.TimeRangeCondition(
    time_range=conditions.TimeRange(
        start_time=pd.Timestamp(year=2001, month=2, day=3),
        end_time=pd.Timestamp(year=2002, month=3, day=4)))
_ANNOTATION_CONDITION = conditions.AnnotationCondition('TEST')
_USER_IDS_CONDITION = conditions.UsersCondition(['USERA', 'USERB'])
_DEVICE_IDS_CONDITION = conditions.DevicesCondition(['ABC', 'XYZ'])
_SENSOR_IDS_CONDITION = conditions.SensorsCondition(['IJK', 'MNO'])
_ALGORITHM_CONDITION = conditions.AlgorithmCondition('name', 'version')
_DEVICE_TYPE_CONDITION = conditions.DeviceTypeCondition(
    conditions.DeviceType.COPPA)

_RAW_DATA_POINTS_SCHEMA = [
    ('UserID', 'string'), ('DeviceID', 'string'), ('UserNamespace', 'string'),
    ('DataPointTime', 'timestamp'), ('DataPointWriteTime', 'timestamp'),
    (
        'DataSource',
        ibis.expr.datatypes.Struct(['sensor', 'algorithm'], [
            ibis.expr.datatypes.Struct(['id'], ['string']),
            ibis.expr.datatypes.Struct(['name', 'version'],
                                       ['string', 'string'])
        ]),
    ), ('annotation_labels', 'string')
]
_ANNOTATION_SCHEMA = [
    ('user_id', 'string'), ('device_id', 'string'),
    ('user_namespace', 'string'), ('start_timestamp_utc', 'timestamp'),
    ('end_timestamp_utc', 'timestamp'), ('annotation_label', 'string'),
    ('version_name', 'string'), ('version_number', 'int64'),
    ('input_data_info',
     ibis.expr.datatypes.Array(
         ibis.expr.datatypes.Struct([
             'version_number', 'version_name', 'metric_type'
         ], [
             'int64', 'string',
             ibis.expr.datatypes.Struct(
                 ['stream_item_type', 'derived_data_type', 'annotation_type'],
                 ['int64', 'int64', 'int64'])
         ])))
]

_ANN_TABLE = ibis.table(_ANNOTATION_SCHEMA, 'ann_table')
_RAW_DP_TABLE = ibis.table(_RAW_DATA_POINTS_SCHEMA, 'raw_dp_table')


def build_data_source(sensor_id: int = 1,
                      algorithm_name: str = 'algo_name',
                      algorithm_version: str = 'algo_version'):
    return types_pb2.DataSource(name='DataSourceName',
                                sensor=types_pb2.Sensor(id=str(sensor_id)),
                                algorithm=types_pb2.Algorithm(
                                    name=algorithm_name,
                                    version=algorithm_version))


def create_schema_object_and_ds_cache(
        user_id: str = 'user',  # pylint: disable=dangerous-default-value
        device: str = 'device',
        user_namespace: int = 1,
        annotation_labels: Optional[Set[str]] = None,
        sensor_id: str = '1',
        algorithm_name: str = 'algo_name',
        algorithm_version: str = 'algo_version',
        timestamp=pd.Timestamp('2010-01-01', tz='UTC'),
        write_time: Optional[pd.Timestamp] = None):
    data_source = types_pb2.DataSource(name='DataSourceName',
                                       sensor=types_pb2.Sensor(id=sensor_id),
                                       algorithm=types_pb2.Algorithm(
                                           name=algorithm_name,
                                           version=algorithm_version))
    data_source_id = hash(data_source.SerializeToString())

    echo_metadata = None if write_time is None else schemas.EchoMetadata(
        bucket_start=timestamps.datetime_to_beam_timestamp(write_time),
        bucket_write_time=timestamps.datetime_to_beam_timestamp(write_time),
        deleted_time=None,
        snapshot_time=None)
    pressure = schemas.Pressure(
        data_point_metadata=schemas.data_point_metadata_for_raw_data(
            device_id=device,
            participant_id=user_id,
            participant_namespace=user_namespace,
            data_source_id=data_source_id,
            echo_metadata=echo_metadata,
            sensor_store_metadata=None,
            annotation_labels=annotation_labels or set()),
        measurement_timestamp_utc=timestamps.datetime_to_beam_timestamp(
            timestamp),
        pressure=1)
    return (pressure, {data_source_id: data_source})


class ConditionsTest(unittest.TestCase):

    @parameterized.expand([
        ('Test time range condition.', _TIME_RANGE_CONDITION,
         (_ANN_TABLE.end_timestamp_utc >
          ibis.timestamp('2001-02-03 00:00:00+00:00')) &
         (_ANN_TABLE.start_timestamp_utc <=
          ibis.timestamp('2002-03-04 00:00:00+00:00')),
         (_RAW_DP_TABLE.DataPointTime >=
          ibis.timestamp('2001-02-03 00:00:00+00:00')) &
         (_RAW_DP_TABLE.DataPointTime <
          ibis.timestamp('2002-03-04 00:00:00+00:00')), None),
        ('Test user ids condition', _USER_IDS_CONDITION,
         _ANN_TABLE.user_id.isin(['USERA', 'USERB']),
         _RAW_DP_TABLE.UserID.isin(['USERA', 'USERB']), None),
        ('Test device ids condition', _DEVICE_IDS_CONDITION,
         _ANN_TABLE.device_id.isin(['ABC', 'XYZ']),
         _RAW_DP_TABLE.DeviceID.isin(['ABC', 'XYZ']), None),
        ('Test algorithm condition for annotation',
         conditions.AnnotationAlgorithmCondition(
             annotation_algo_name='foo-anno-algo',
             annotation_algo_version=5555),
         (_ANN_TABLE.version_name.isnull() & _ANN_TABLE.version_number.isnull()
          | ((_ANN_TABLE.version_name == 'foo-anno-algo') &
             (_ANN_TABLE.version_number == 5555))), None, None),
        ('Test and conditions',
         (_TIME_RANGE_CONDITION & _USER_IDS_CONDITION & _DEVICE_IDS_CONDITION),
         ((_ANN_TABLE.end_timestamp_utc >
           ibis.timestamp('2001-02-03 00:00:00+00:00')) &
          (_ANN_TABLE.start_timestamp_utc <=
           ibis.timestamp('2002-03-04 00:00:00+00:00'))) &
         (_ANN_TABLE.user_id.isin(['USERA', 'USERB'])) &
         (_ANN_TABLE.device_id.isin(['ABC', 'XYZ'])),
         (((_RAW_DP_TABLE.DataPointTime >=
            ibis.timestamp('2001-02-03 00:00:00+00:00')) &
           (_RAW_DP_TABLE.DataPointTime <
            ibis.timestamp('2002-03-04 00:00:00+00:00'))) &
          (_RAW_DP_TABLE.UserID.isin(['USERA', 'USERB'])) &
          (_RAW_DP_TABLE.DeviceID.isin(['ABC', 'XYZ']))), None),
        ('Test or conditions',
         (_TIME_RANGE_CONDITION | _USER_IDS_CONDITION | _DEVICE_IDS_CONDITION),
         ((_ANN_TABLE.end_timestamp_utc >
           ibis.timestamp('2001-02-03 00:00:00+00:00')) &
          (_ANN_TABLE.start_timestamp_utc <=
           ibis.timestamp('2002-03-04 00:00:00+00:00'))) |
         (_ANN_TABLE.user_id.isin(['USERA', 'USERB'])) |
         (_ANN_TABLE.device_id.isin(['ABC', 'XYZ'])),
         (((_RAW_DP_TABLE.DataPointTime >=
            ibis.timestamp('2001-02-03 00:00:00+00:00')) &
           (_RAW_DP_TABLE.DataPointTime <
            ibis.timestamp('2002-03-04 00:00:00+00:00'))) |
          (_RAW_DP_TABLE.UserID.isin(['USERA', 'USERB'])) |
          (_RAW_DP_TABLE.DeviceID.isin(['ABC', 'XYZ']))), None),
        (
            'Test raw data point conditions',
            (_USER_IDS_CONDITION & _DEVICE_IDS_CONDITION &
             _TIME_RANGE_CONDITION),
            None,
            (
                # user ID
                _RAW_DP_TABLE.UserID.isin(['USERA', 'USERB'])
                # device ID
                & _RAW_DP_TABLE.DeviceID.isin(['ABC', 'XYZ'])
                # time range
                & (_RAW_DP_TABLE.DataPointTime >=
                   ibis.timestamp('2001-02-03 00:00:00+00:00')) &
                (_RAW_DP_TABLE.DataPointTime <
                 ibis.timestamp('2002-03-04 00:00:00+00:00'))),
            None),
        ('Test device type conditions', _DEVICE_TYPE_CONDITION,
         _ANN_TABLE.device_id.re_search(
             conditions.DeviceType.COPPA.device_id_pattern()),
         _RAW_DP_TABLE.DeviceID.re_search(
             conditions.DeviceType.COPPA.device_id_pattern()), None),
        ('Test user namespace tableconditions',
         conditions.UserNamespaceCondition(enums_pb2.UserIdKeyspace.GAIA_ID),
         None, _RAW_DP_TABLE.UserNamespace == 'GAIA_ID', None),
        ('Test user namespace negate conditions',
         conditions.UserNamespaceCondition(enums_pb2.UserIdKeyspace.GAIA_ID,
                                           negate=True),
         _ANN_TABLE.user_namespace != 'GAIA_ID',
         _RAW_DP_TABLE.UserNamespace != 'GAIA_ID', None),
        ('Test negate device ids condition', -_DEVICE_IDS_CONDITION,
         -_ANN_TABLE.device_id.isin(['ABC', 'XYZ']),
         -_RAW_DP_TABLE.DeviceID.isin(['ABC', 'XYZ']), None)
    ])
    def test_conditions(self, test_name, condition_to_test,
                        expected_cond_annotation, expected_cond_data_points,
                        expected_cond_data_points_no_annotations):
        del test_name
        if expected_cond_annotation is not None:
            self._assert_sql_condition(
                expected_cond_annotation,
                condition_to_test.annotations_condition(_ANN_TABLE), _ANN_TABLE)
        if expected_cond_data_points is not None:
            dp_table = _RAW_DP_TABLE
            self._assert_sql_condition(
                expected_cond_data_points,
                condition_to_test.data_points_condition(dp_table), dp_table)
        if expected_cond_data_points_no_annotations is not None:
            self._assert_sql_condition(
                expected_cond_data_points_no_annotations,
                condition_to_test.data_points_condition(_RAW_DP_TABLE, False),
                _RAW_DP_TABLE)

    def _assert_sql_condition(self, want_condition, got_condition, table):
        want_sql = ibis_bigquery.compile(table[want_condition])
        got_sql = ibis_bigquery.compile(table[got_condition])
        self.assertEqual(want_sql, got_sql)

    @parameterized.expand([
        ('Test time range condition.', _TIME_RANGE_CONDITION,
         create_schema_object_and_ds_cache(
             timestamp=pd.Timestamp(year=2001, month=2, day=3, tz='UTC')),
         create_schema_object_and_ds_cache(
             timestamp=pd.Timestamp(year=2003, month=2, day=4, tz='UTC'))),
        ('Test user ids condition', _USER_IDS_CONDITION,
         create_schema_object_and_ds_cache(user_id='USERA'),
         create_schema_object_and_ds_cache(user_id='bad_user')),
        ('Test device ids condition', _DEVICE_IDS_CONDITION,
         create_schema_object_and_ds_cache(device='ABC'),
         create_schema_object_and_ds_cache(device='bad_device')),
        ('Test sensor ids condition', _SENSOR_IDS_CONDITION,
         create_schema_object_and_ds_cache(sensor_id='IJK'),
         create_schema_object_and_ds_cache(sensor_id='bad_sensor')),
        ('Test algorithm condition',
         conditions.AlgorithmCondition(algorithm_name='name',
                                       algorithm_version='version'),
         create_schema_object_and_ds_cache(algorithm_name='name',
                                           algorithm_version='version'),
         create_schema_object_and_ds_cache(algorithm_name='bad_name',
                                           algorithm_version='version')),
        ('Test algorithm condition, only name',
         conditions.AlgorithmCondition(algorithm_name='name'),
         create_schema_object_and_ds_cache(algorithm_name='name'),
         create_schema_object_and_ds_cache(algorithm_name='bad_name')),
        ('Test algorithm condition, only version',
         conditions.AlgorithmCondition(algorithm_version='version'),
         create_schema_object_and_ds_cache(algorithm_version='version'),
         create_schema_object_and_ds_cache(algorithm_version='bad_version')),
        ('Test and conditions',
         (_TIME_RANGE_CONDITION & _USER_IDS_CONDITION & _DEVICE_IDS_CONDITION),
         create_schema_object_and_ds_cache(timestamp=pd.Timestamp(year=2001,
                                                                  month=2,
                                                                  day=3,
                                                                  tz='UTC'),
                                           user_id='USERA',
                                           device='ABC'),
         create_schema_object_and_ds_cache(
             timestamp=pd.Timestamp(year=2003, month=2, day=4, tz='UTC'))),
        ('Test and conditions with annotation filters.',
         (_TIME_RANGE_CONDITION & _ANNOTATION_CONDITION &
          _DEVICE_IDS_CONDITION),
         create_schema_object_and_ds_cache(timestamp=pd.Timestamp(year=2001,
                                                                  month=2,
                                                                  day=3,
                                                                  tz='UTC'),
                                           annotation_labels={'TEST'},
                                           device='ABC'),
         create_schema_object_and_ds_cache(timestamp=pd.Timestamp(year=2003,
                                                                  month=2,
                                                                  day=3,
                                                                  tz='UTC'),
                                           annotation_labels={'FALSE'},
                                           device='ABC')),
        ('Test or conditions',
         (_TIME_RANGE_CONDITION | _USER_IDS_CONDITION | _DEVICE_IDS_CONDITION),
         create_schema_object_and_ds_cache(timestamp=pd.Timestamp(year=2001,
                                                                  month=2,
                                                                  day=3,
                                                                  tz='UTC'),
                                           user_id='bad',
                                           device='bad'),
         create_schema_object_and_ds_cache(timestamp=pd.Timestamp(year=2004,
                                                                  month=2,
                                                                  day=5,
                                                                  tz='UTC'),
                                           user_id='bad',
                                           device='bad')),
        ('Test or conditions with annotation filters',
         (_TIME_RANGE_CONDITION | _ANNOTATION_CONDITION |
          _DEVICE_IDS_CONDITION),
         create_schema_object_and_ds_cache(timestamp=pd.Timestamp(year=2003,
                                                                  month=2,
                                                                  day=3,
                                                                  tz='UTC'),
                                           annotation_labels={'TEST'},
                                           device='bad'),
         create_schema_object_and_ds_cache(timestamp=pd.Timestamp(year=2003,
                                                                  month=2,
                                                                  day=3,
                                                                  tz='UTC'),
                                           annotation_labels={'FALSE'},
                                           device='bad')),
        ('Test complex ordering',
         (_TIME_RANGE_CONDITION &
          (_USER_IDS_CONDITION | _DEVICE_IDS_CONDITION) &
          (conditions.UsersCondition(user_ids=['USER_1', 'USER_2']) |
           _ANNOTATION_CONDITION |
           conditions.AnnotationCondition(annotation_label='FOO'))),
         create_schema_object_and_ds_cache(timestamp=pd.Timestamp(year=2001,
                                                                  month=2,
                                                                  day=3,
                                                                  tz='UTC'),
                                           user_id='USER_1',
                                           device='ABC',
                                           annotation_labels={'FOO'}),
         create_schema_object_and_ds_cache(timestamp=pd.Timestamp(year=2003,
                                                                  month=2,
                                                                  day=3,
                                                                  tz='UTC'),
                                           annotation_labels={'FALSE'},
                                           device='bad')),
        ('Test device type conditions', _DEVICE_TYPE_CONDITION,
         create_schema_object_and_ds_cache(device='C2Q123123'),
         create_schema_object_and_ds_cache(device='bad')),
        ('Test user namespace conditions',
         conditions.UserNamespaceCondition(enums_pb2.UserIdKeyspace.GAIA_ID),
         create_schema_object_and_ds_cache(
             user_namespace=enums_pb2.UserIdKeyspace.GAIA_ID),
         create_schema_object_and_ds_cache(
             user_namespace=enums_pb2.UserIdKeyspace.CSP_ID)),
        ('Test user namespace negate conditions',
         conditions.UserNamespaceCondition(enums_pb2.UserIdKeyspace.GAIA_ID,
                                           negate=True),
         create_schema_object_and_ds_cache(
             user_namespace=enums_pb2.UserIdKeyspace.CSP_ID),
         create_schema_object_and_ds_cache(
             user_namespace=enums_pb2.UserIdKeyspace.GAIA_ID)),
    ])
    def test_schema_conditions(
            self, test_name, condition_to_test: conditions.Condition,
            input_true_schema_with_cache: Tuple[schemas.DataPointType, Dict],
            input_false_schema_with_cache: Tuple[schemas.DataPointType, Dict]):
        del test_name
        input_true_schema, cache_one = input_true_schema_with_cache
        input_false_schema, cache_two = input_false_schema_with_cache
        data_source_cache = cache_one
        data_source_cache.update(cache_two)

        self.assertTrue(
            condition_to_test.data_point_row_condition(
                input_true_schema, DataSourceCache(data_source_cache)))
        self.assertFalse(
            condition_to_test.data_point_row_condition(
                input_false_schema, DataSourceCache(data_source_cache)))

    @parameterized.expand([
        (
            'Test time range condition.',
            # condition_to_test
            _TIME_RANGE_CONDITION,
            # pubsub message. Time = 2/3/2001 1 AM
            PubsubMessage('', attributes={'startMillis': '981162000000'}),
            True),
        (
            'Test time range condition below start time.',
            # condition_to_test
            _TIME_RANGE_CONDITION,
            # pubsub message. Time = 1/3/2001 1 AM
            PubsubMessage('', attributes={'startMillis': '978483600000'}),
            False),
        (
            'Test time range condition above start time.',
            # condition_to_test
            _TIME_RANGE_CONDITION,
            # pubsub message. Time = 1/3/2020 1 AM
            PubsubMessage('', attributes={'startMillis': '1578013200000'}),
            False),
        (
            'Test user ID condition positive.',
            # condition_to_test
            _USER_IDS_CONDITION,
            # pubsub message. Time = 1/3/2020 1 AM
            PubsubMessage('', attributes={'participantId': 'USERA'}),
            True),
        (
            'Test user ID condition negative.',
            # condition_to_test
            _USER_IDS_CONDITION,
            # pubsub message. Time = 1/3/2020 1 AM
            PubsubMessage('', attributes={'participantId': 'false'}),
            False),
        (
            'Test device ID condition positive.',
            # condition_to_test
            _DEVICE_IDS_CONDITION,
            # pubsub message. Time = 1/3/2020 1 AM
            PubsubMessage('', attributes={'deviceId': 'ABC'}),
            True),
        (
            'Test device ID condition negative.',
            # condition_to_test
            _DEVICE_IDS_CONDITION,
            # pubsub message. Time = 1/3/2020 1 AM
            PubsubMessage('', attributes={'deviceId': 'false'}),
            False),
        (
            'Test or condition positive.',
            # condition_to_test
            (_DEVICE_IDS_CONDITION | _USER_IDS_CONDITION),
            # pubsub message. Time = 1/3/2020 1 AM
            PubsubMessage('',
                          attributes={
                              'deviceId': 'false',
                              'participantId': 'USERA'
                          }),
            True),
        (
            'Test or condition negative.',
            # condition_to_test
            (_DEVICE_IDS_CONDITION | _USER_IDS_CONDITION),
            # pubsub message. Time = 1/3/2020 1 AM
            PubsubMessage('',
                          attributes={
                              'deviceId': 'false',
                              'participantId': 'false'
                          }),
            False),
        (
            'Test and condition positive.',
            # condition_to_test
            (_DEVICE_IDS_CONDITION & _USER_IDS_CONDITION),
            # pubsub message. Time = 1/3/2020 1 AM
            PubsubMessage('',
                          attributes={
                              'deviceId': 'ABC',
                              'participantId': 'USERA'
                          }),
            True),
        (
            'Test and condition negative.',
            # condition_to_test
            (_DEVICE_IDS_CONDITION & _USER_IDS_CONDITION),
            # pubsub message. Time = 1/3/2020 1 AM
            PubsubMessage('',
                          attributes={
                              'deviceId': 'ABC',
                              'participantId': 'false'
                          }),
            False),
        (
            'Test device type negative.',
            # condition_to_test
            _DEVICE_TYPE_CONDITION,
            # pubsub message. Time = 1/3/2020 1 AM
            PubsubMessage('',
                          attributes={
                              'deviceId': 'ABC',
                              'participantId': 'false'
                          }),
            False),
        (
            'Test device type negative.',
            # condition_to_test
            _DEVICE_TYPE_CONDITION,
            # pubsub message. Time = 1/3/2020 1 AM
            PubsubMessage('',
                          attributes={
                              'deviceId': 'C2Q123123',
                              'participantId': 'false'
                          }),
            True),
    ])
    def test_pubsub_conditions(self, name: str,
                               condition_to_test: conditions.Condition,
                               pubsub_message: PubsubMessage,
                               expected_value: bool):
        del name
        self.assertEqual(condition_to_test.pubsub_condition(pubsub_message),
                         expected_value)

    def test_and_no_sides_effects(self):
        base_condition = conditions.DevicesCondition(
            ['asdf']) & conditions.DevicesCondition(['fdsa'])
        # Ignore unused expression since we want to verify that anding the
        # condition causes no side effects on base_condition
        base_condition & conditions.DevicesCondition(['unused'])  # pylint: disable=expression-not-assigned

        self.assertEqual(
            base_condition,
            conditions.DevicesCondition(['asdf']) &
            conditions.DevicesCondition(['fdsa']))

    def test_or_no_sides_effects(self):
        base_condition = conditions.DevicesCondition(
            ['asdf']) | conditions.DevicesCondition(['fdsa'])
        # Ignore unused expression since we want to verify that anding the
        # condition causes no side effects on base_condition
        base_condition | conditions.DevicesCondition(['unused'])  # pylint: disable=expression-not-assigned

        self.assertEqual(
            base_condition,
            conditions.DevicesCondition(['asdf']) |
            conditions.DevicesCondition(['fdsa']))


class TimeRangeTests(unittest.TestCase):
    '''Unit tests for time range conditions.'''

    def _assert_sql_condition(self, want_condition, got_condition, table):
        want_sql = ibis_bigquery.compile(table[want_condition])
        got_sql = ibis_bigquery.compile(table[got_condition])
        self.assertEqual(want_sql, got_sql)

    def test_time_range_property(self):

        start_time = pd.Timestamp(year=2001, month=2, day=3)
        end_time = pd.Timestamp(year=2002, month=3, day=4)

        time_range_condition = conditions.TimeRangeCondition(
            time_range=conditions.TimeRange(start_time=start_time,
                                            end_time=end_time))

        start_time = pd.Timestamp(year=2001,
                                  month=2,
                                  day=3,
                                  tzinfo=pytz.timezone('UTC'))
        end_time = pd.Timestamp(year=2002,
                                month=3,
                                day=4,
                                tzinfo=pytz.timezone('UTC'))

        self.assertEqual(time_range_condition.time_range.start_time, start_time)
        self.assertEqual(time_range_condition.time_range.end_time, end_time)

    def test_write_time_data_point_rows_condition(self):
        start_time = pd.Timestamp(year=2001, month=2, day=3)
        end_time = pd.Timestamp(year=2002, month=3, day=4)

        write_time_range_condition = conditions.WriteTimeRangeCondition(
            time_range=conditions.TimeRange(start_time=start_time,
                                            end_time=end_time))

        test_points = [create_schema_object_and_ds_cache(write_time=None)]

        data_points = [test_point[0] for test_point in test_points]
        data_source_cache = {}
        for test_point in test_points:
            data_source_cache.update(test_point[1])

        with self.assertRaises(ValueError):
            for data_point in data_points:
                write_time_range_condition.data_point_row_condition(
                    data_point, data_source_cache)

    def test_write_time_data_points_condition(self):
        start_time = pd.Timestamp('2001-02-03T00:00:00+0000')
        end_time = pd.Timestamp('2002-03-04T00:00:00+0000')

        write_time_range_condition = conditions.WriteTimeRangeCondition(
            time_range=conditions.TimeRange(start_time=start_time,
                                            end_time=end_time))

        dp_table = _RAW_DP_TABLE
        self._assert_sql_condition(
            ((dp_table.DataPointWriteTime >= ibis.timestamp(
                start_time.isoformat())) &
             (dp_table.DataPointWriteTime < ibis.timestamp(
                 end_time.isoformat()))),
            write_time_range_condition.data_points_condition(dp_table),
            dp_table)


if __name__ == '__main__':
    unittest.main()
