"""Unit tests for data_filters.py"""

# pylint: disable=protected-access
from typing import List
import unittest
from unittest import mock

from google.cloud import bigquery
import numpy as np
import pandas as pd

from verily.ds_sdk.core import data_filters

exclusive_filter_1 = pd.DataFrame([{
    'device_id': 'device1',
    'start_time': '2022-01-01T12:00:00.000Z',
    'end_time': '2022-01-01T15:00:00.000Z'
}, {
    'device_id': 'device1',
    'start_time': '2022-01-02T12:00:00.000Z',
    'end_time': '2022-01-02T13:00:00.000Z'
}, {
    'device_id': 'device1',
    'start_time': '2022-01-03T12:00:00.000Z',
    'end_time': '2022-01-04T13:00:00.000Z'
}, {
    'device_id': 'device2',
    'start_time': '2022-01-01T12:00:00.000Z',
    'end_time': '2022-01-01T15:00:00.000Z'
}, {
    'device_id': 'device2',
    'start_time': '2022-01-02T12:00:00.000Z',
    'end_time': '2022-01-04T12:00:00.000Z'
}])

exclusive_filter_2 = pd.DataFrame([{
    'device_id': 'device1',
    'start_time': '2022-01-01T14:00:00.000Z',
    'end_time': '2022-01-01T16:00:00.000Z',
    'DataPoint.supplemental_source_data_spec': 'com.verily.pressure',
    'DataPoint.supplemental_source_sensor_id': '0',
    'DataPoint.supplemental_source_algorithm_name': '',
    'DataPoint.supplemental_source_algorithm_version': '',
}, {
    'device_id': 'device1',
    'start_time': '2022-01-02T08:00:00.000Z',
    'end_time': '2022-01-02T10:00:00.000Z',
    'DataPoint.supplemental_source_data_spec': 'com.verily.pressure',
    'DataPoint.supplemental_source_sensor_id': '1',
    'DataPoint.supplemental_source_algorithm_name': '',
    'DataPoint.supplemental_source_algorithm_version': '',
}, {
    'device_id': 'device2',
    'start_time': '2022-01-01T12:00:00.000Z',
    'end_time': '2022-01-01T15:00:00.000Z',
    'DataPoint.supplemental_source_data_spec': 'com.verily.pressure',
    'DataPoint.supplemental_source_sensor_id': '2',
    'DataPoint.supplemental_source_algorithm_name': 'algoA',
    'DataPoint.supplemental_source_algorithm_version': 'v1',
}, {
    'device_id': 'device2',
    'start_time': '2022-01-01T23:00:00.000Z',
    'end_time': '2022-01-02T01:00:00.000Z',
    'DataPoint.supplemental_source_data_spec': 'com.verily.pressure',
    'DataPoint.supplemental_source_sensor_id': '2',
    'DataPoint.supplemental_source_algorithm_name': 'algoA',
    'DataPoint.supplemental_source_algorithm_version': 'v2',
}, { # This row will have no effect in integration tests because it is for IMU
    'device_id': 'device2',
    'start_time': '2022-01-01T01:00:00.000Z',
    'end_time': '2022-01-05T00:00:00.000Z',
    'DataPoint.supplemental_source_data_spec': 'com.verily.imu',
    'DataPoint.supplemental_source_sensor_id': '2',
    'DataPoint.supplemental_source_algorithm_name': 'algoA',
    'DataPoint.supplemental_source_algorithm_version': 'v2',
}])

inclusive_filter_1 = pd.DataFrame([{
    'device_id': 'device1',
    'start_time': '2022-01-01T06:00:00.000Z',
    'end_time': '2022-01-01T18:00:00.000Z'
}, {
    'device_id': 'device1',
    'start_time': '2022-01-02T11:00:00.000Z',
    'end_time': '2022-01-02T14:00:00.000Z'
}, {
    'device_id': 'device1',
    'start_time': '2022-01-03T13:00:00.000Z',
    'end_time': '2022-01-04T06:00:00.000Z'
}, {
    'device_id': 'device2',
    'start_time': '2022-01-01T00:00:00.000Z',
    'end_time': '2022-01-05T00:00:00.000Z'
}, {
    'device_id': 'device3',
    'start_time': '2022-01-01T00:00:00.000Z',
    'end_time': '2022-01-05T00:00:00.000Z',
}])

inclusive_filter_2 = pd.DataFrame([{
    'device_id':
        'device1',
    'start_time':
        int(pd.Timestamp('2022-01-01T14:00:00.000Z').timestamp() * 1000),
    'end_time':
        '2022-01-01T17:00:00.000Z',
    'DataPoint.supplemental_source_data_spec':
        'com.verily.pressure',
    'DataPoint.supplemental_source_sensor_id':
        '0',
    'DataPoint.supplemental_source_algorithm_name':
        np.nan,
    'DataPoint.supplemental_source_algorithm_version':
        pd.NaT,
}, {
    'device_id':
        'device1',
    'start_time':
        int(pd.Timestamp('2022-01-01T00:00:00.000Z').timestamp() * 1000),
    'end_time':
        '2022-01-05T00:00:00.000Z',
    'DataPoint.supplemental_source_data_spec':
        'com.verily.pressure',
    'DataPoint.supplemental_source_sensor_id':
        '1',
    'DataPoint.supplemental_source_algorithm_name':
        None,
    'DataPoint.supplemental_source_algorithm_version':
        None,
}, { # This row will have no effect in integration tests because it is for a
     # device that does not exist in the input PCollection
    'device_id':
        'device5',
    'start_time':
        int(pd.Timestamp('2022-01-01T00:00:00.000Z').timestamp() * 1000),
    'end_time':
        '2022-01-05T00:00:00.000Z',
    'DataPoint.supplemental_source_data_spec':
        'com.verily.pressure',
    'DataPoint.supplemental_source_sensor_id':
        '0',
    'DataPoint.supplemental_source_algorithm_name':
        None,
    'DataPoint.supplemental_source_algorithm_version':
        'v2',
}, { # This row will have no effect in integration tests because it is for a
     # device that does not exist in the input PCollection
    'device_id':
        'device5',
    'start_time':
        int(pd.Timestamp('2022-01-01T00:00:00.000Z').timestamp() * 1000),
    'end_time':
        '2022-01-05T00:00:00.000Z',
    'DataPoint.supplemental_source_data_spec':
        'com.verily.pressure',
    'DataPoint.supplemental_source_sensor_id':
        '0',
    'DataPoint.supplemental_source_algorithm_name':
        'algoB',
    'DataPoint.supplemental_source_algorithm_version':
        None,
}, { # This row will have no effect in integration tests because it is for a
     # data source that does not exist in the input PCollection
    'device_id':
        'device2',
    'start_time':
        int(pd.Timestamp('2022-01-01T00:00:00.000Z').timestamp() * 1000),
    'end_time':
        '2022-01-01T06:00:00.000Z',
    'DataPoint.supplemental_source_data_spec':
        'com.verily.pressure',
    'DataPoint.supplemental_source_sensor_id':
        '0',
    'DataPoint.supplemental_source_algorithm_name':
        'algo-rhythm',
    'DataPoint.supplemental_source_algorithm_version':
        'v42',
}, {
    'device_id':
        'device2',
    'start_time':
        int(pd.Timestamp('2022-01-01T12:00:00.000Z').timestamp() * 1000),
    'end_time':
        '2022-01-03T12:00:00.000Z',
    'DataPoint.supplemental_source_data_spec':
        'com.verily.pressure',
    'DataPoint.supplemental_source_sensor_id':
        '2',
    'DataPoint.supplemental_source_algorithm_name':
        'algoA',
    'DataPoint.supplemental_source_algorithm_version':
        'v2',
}])

algorithm_version_filter_data = pd.DataFrame([{
    'device_id': 'D1',
    'start_time': '2022-03-01T00:00:00.000Z',
    'end_time': '2022-03-01T01:00:00.000Z',
    'DataSource.algorithm.version': 'v1',
}, {
    'device_id': 'D1',
    'start_time': '2022-03-01T02:00:00.000Z',
    'end_time': '2022-03-01T03:00:00.000Z',
    'DataSource.algorithm.version': 'v2',
}, {
    'device_id': 'D2',
    'start_time': '2022-03-01T04:00:00.000Z',
    'end_time': '2022-03-01T05:00:00.000Z',
    'DataSource.algorithm.version': 'v3',
}])
algorithm_version_filter_data_v2 = (
    algorithm_version_filter_data.iloc[[1]].copy()
)
ALGO_VERSION_FIELD = 'DataSource.algorithm.version'
ALGO_VERSION_TABLE_ID = 'project.dataset.algo_version_filter_table'

# pylint: disable=line-too-long
def mock_read_gbq(query: str, **_) -> pd.DataFrame:
    if query == 'SELECT device_id AS device_id, start_timestamp_utc AS start_time, end_timestamp_utc AS end_time FROM project.dataset.inclusive_1 WHERE TRUE':
        return inclusive_filter_1
    if query == 'SELECT DeviceID AS device_id, DataPointTime AS start_time, DataPoint.end_time_millis AS end_time, DataPoint.supplemental_source_data_spec, DataPoint.supplemental_source_sensor_id, DataPoint.supplemental_source_algorithm_name, DataPoint.supplemental_source_algorithm_version FROM project.dataset.inclusive_2 WHERE TRUE':
        return inclusive_filter_2
    if query == 'SELECT DeviceId AS device_id, start_timestamp_utc AS start_time, end_timestamp_utc AS end_time FROM project.dataset.exclusive_1 WHERE TRUE AND annotation_label IN (\'label1\',\'label2\')':
        return exclusive_filter_1
    if query == 'SELECT DeviceID AS device_id, DataPoint.start_time AS start_time, DataPoint.end_time AS end_time, DataPoint.supplemental_source_data_spec, DataPoint.supplemental_source_sensor_id, DataPoint.supplemental_source_algorithm_name, DataPoint.supplemental_source_algorithm_version FROM project.dataset.exclusive_2 WHERE TRUE':
        return exclusive_filter_2
    if (ALGO_VERSION_TABLE_ID in query and
    f"AND {ALGO_VERSION_FIELD} = 'v2'" in query):
        return algorithm_version_filter_data_v2
    if ALGO_VERSION_TABLE_ID in query:
        return algorithm_version_filter_data

    raise ValueError(f'No match for this query: {query}')


# pylint: enable=line-too-long


class MockClient:
    """Mock BQ Client"""

    def get_table(self, table_id):
        self.table_id = table_id
        return self

    @property
    def schema(self):
        schema = {
            'project.dataset.exclusive_1': [
                bigquery.SchemaField('DeviceId',
                                     'STRING',
                                     'REQUIRED',
                                     None,
                                     fields=(),
                                     policy_tags=None),
                bigquery.SchemaField('start_timestamp_utc',
                                     'TIMESTAMP',
                                     'NULLABLE',
                                     None,
                                     fields=(),
                                     policy_tags=None),
                bigquery.SchemaField('end_timestamp_utc',
                                     'TIMESTAMP',
                                     'NULLABLE',
                                     None,
                                     fields=(),
                                     policy_tags=None)
            ],
            'project.dataset.exclusive_2': [
                bigquery.SchemaField('DeviceID',
                                     'STRING',
                                     'REQUIRED',
                                     None,
                                     fields=(),
                                     policy_tags=None),
                bigquery.SchemaField('DataPointTime',
                                     'TIMESTAMP',
                                     'REQUIRED',
                                     None,
                                     fields=(),
                                     policy_tags=None),
                bigquery.SchemaField(
                    'DataPoint',
                    'RECORD',
                    'REQUIRED',
                    None,
                    fields=(
                        bigquery.SchemaField('end_time',
                                             'INTEGER',
                                             'NULLABLE',
                                             None,
                                             fields=(),
                                             policy_tags=None),
                        bigquery.SchemaField('start_time',
                                             'INTEGER',
                                             'NULLABLE',
                                             None,
                                             fields=(),
                                             policy_tags=None),
                        bigquery.SchemaField('supplemental_source_data_spec',
                                             'STRING',
                                             'NULLABLE',
                                             None,
                                             fields=(),
                                             policy_tags=None),
                        bigquery.SchemaField('supplemental_source_sensor_id',
                                             'STRING',
                                             'NULLABLE',
                                             None,
                                             fields=(),
                                             policy_tags=None),
                        bigquery.SchemaField(
                            'supplemental_source_algorithm_name',
                            'STRING',
                            'NULLABLE',
                            None,
                            fields=(),
                            policy_tags=None),
                        bigquery.SchemaField(
                            'supplemental_source_algorithm_version',
                            'STRING',
                            'NULLABLE',
                            None,
                            fields=(),
                            policy_tags=None),
                    ))
            ],
            'project.dataset.inclusive_1': [
                bigquery.SchemaField('device_id',
                                     'STRING',
                                     'REQUIRED',
                                     None,
                                     fields=(),
                                     policy_tags=None),
                bigquery.SchemaField('start_timestamp_utc',
                                     'TIMESTAMP',
                                     'NULLABLE',
                                     None,
                                     fields=(),
                                     policy_tags=None),
                bigquery.SchemaField('end_timestamp_utc',
                                     'TIMESTAMP',
                                     'NULLABLE',
                                     None,
                                     fields=(),
                                     policy_tags=None)
            ],
            'project.dataset.inclusive_2': [
                bigquery.SchemaField('DeviceID',
                                     'STRING',
                                     'REQUIRED',
                                     None,
                                     fields=(),
                                     policy_tags=None),
                bigquery.SchemaField('DataPointTime',
                                     'TIMESTAMP',
                                     'REQUIRED',
                                     None,
                                     fields=(),
                                     policy_tags=None),
                bigquery.SchemaField(
                    'DataPoint',
                    'RECORD',
                    'REQUIRED',
                    None,
                    fields=(bigquery.SchemaField('end_time_millis',
                                                 'INTEGER',
                                                 'NULLABLE',
                                                 None,
                                                 fields=(),
                                                 policy_tags=None),
                            bigquery.SchemaField(
                                'supplemental_source_data_spec',
                                'STRING',
                                'NULLABLE',
                                None,
                                fields=(),
                                policy_tags=None),
                            bigquery.SchemaField(
                                'supplemental_source_sensor_id',
                                'STRING',
                                'NULLABLE',
                                None,
                                fields=(),
                                policy_tags=None),
                            bigquery.SchemaField(
                                'supplemental_source_algorithm_name',
                                'STRING',
                                'NULLABLE',
                                None,
                                fields=(),
                                policy_tags=None),
                            bigquery.SchemaField(
                                'supplemental_source_algorithm_version',
                                'STRING',
                                'NULLABLE',
                                None,
                                fields=(),
                                policy_tags=None),
                            bigquery.SchemaField('extra_field',
                                                 'STRING',
                                                 'NULLABLE',
                                                 None,
                                                 fields=(),
                                                 policy_tags=None)))
            ],
            ALGO_VERSION_TABLE_ID: [
                bigquery.SchemaField(data_filters.DEVICE_ID_FIELD,
                                     'STRING',
                                     'REQUIRED',
                                     None,
                                     fields=(),
                                     policy_tags=None),
                bigquery.SchemaField(data_filters.DATA_POINT_TIME_FIELD,
                                     'TIMESTAMP',
                                     'NULLABLE',
                                     None,
                                     fields=(),
                                     policy_tags=None),
                bigquery.SchemaField(data_filters.END_TIME_MILLIS_FIELD,
                                     'INTEGER',
                                     'NULLABLE',
                                     None,
                                     fields=(),
                                     policy_tags=None),
                bigquery.SchemaField(ALGO_VERSION_FIELD,
                                     'STRING',
                                     'NULLABLE',
                                     None,
                                     fields=(),
                                     policy_tags=None)
            ],
        }[self.table_id]

        return schema


class DataFiltersTest(unittest.TestCase):
    """Unit tests for the DataFilter class and underlying _DataFilterCache"""

    @mock.patch.object(bigquery, 'Client', autospec=True)
    @mock.patch('pandas.read_gbq', new=mock_read_gbq)
    def test_filter_map_inclusive_1(self, mock_bq_client: mock.MagicMock):

        mock_bq_client.return_value = MockClient()

        expected_time_ranges = {
            data_filters._DataSourceFilterKey('device1', True):
                data_filters._TimeRanges.from_list(
                    [[
                        int(pd.Timestamp('2022-01-01T06:00:00.000Z').asm8),
                        int(pd.Timestamp('2022-01-01T18:00:00.000Z').asm8)
                    ],
                     [
                         int(pd.Timestamp('2022-01-02T11:00:00.000Z').asm8),
                         int(pd.Timestamp('2022-01-02T14:00:00.000Z').asm8)
                     ],
                     [
                         int(pd.Timestamp('2022-01-03T13:00:00.000Z').asm8),
                         int(pd.Timestamp('2022-01-04T06:00:00.000Z').asm8)
                     ]]),
            data_filters._DataSourceFilterKey('device2', True):
                data_filters._TimeRanges.from_list([[
                    int(pd.Timestamp('2022-01-01T00:00:00.000Z').asm8),
                    int(pd.Timestamp('2022-01-05T00:00:00.000Z').asm8)
                ]]),
            data_filters._DataSourceFilterKey('device3', True):
                data_filters._TimeRanges.from_list([[
                    int(pd.Timestamp('2022-01-01T00:00:00.000Z').asm8),
                    int(pd.Timestamp('2022-01-05T00:00:00.000Z').asm8)
                ]])
        }

        # If the table hasn't been queried, then the filter map should be empty
        inclusive_1 = data_filters.DataFilter(
            'project.dataset.inclusive_1',
            is_inclusive=True,
            device_id_field='device_id',
            start_time_field='start_timestamp_utc',
            end_time_field='end_timestamp_utc')
        self.assertEqual(inclusive_1._time_range_map, {})

        time_range_map = inclusive_1.get_filter_table()
        self.assertEqual(time_range_map, expected_time_ranges)

    @mock.patch.object(bigquery, 'Client', autospec=True)
    @mock.patch('pandas.read_gbq', new=mock_read_gbq)
    def test_filter_map_inclusive_2(self, mock_bq_client: mock.MagicMock):

        mock_bq_client.return_value = MockClient()

        # NOTE: the condition is built by iterating on a DataFrameGroupBy. By
        # default, the groupby sorts the group labels. This is why the expected
        # filter dictionary keys are out-of-order compared to the mock table
        # above.
        expected_time_ranges = {
            data_filters._DataSourceFilterKey('device1',
                                              True,
                                              data_spec='com.verily.pressure',
                                              sensor_id='0'):
                data_filters._TimeRanges.from_list([[
                    int(pd.Timestamp('2022-01-01T14:00:00.000Z').asm8),
                    int(pd.Timestamp('2022-01-01T17:00:00.000Z').asm8)
                ]]),
            data_filters._DataSourceFilterKey('device1',
                                              True,
                                              data_spec='com.verily.pressure',
                                              sensor_id='1'):
                data_filters._TimeRanges.from_list([[
                    int(pd.Timestamp('2022-01-01T00:00:00.000Z').asm8),
                    int(pd.Timestamp('2022-01-05T00:00:00.000Z').asm8)
                ]]),
            data_filters._DataSourceFilterKey('device2',
                                              True,
                                              data_spec='com.verily.pressure',
                                              sensor_id='0',
                                              algorithm_name='algo-rhythm',
                                              algorithm_version='v42'):
                data_filters._TimeRanges.from_list([[
                    int(pd.Timestamp('2022-01-01T00:00:00.000Z').asm8),
                    int(pd.Timestamp('2022-01-01T06:00:00.000Z').asm8)
                ]]),
            data_filters._DataSourceFilterKey('device2',
                                              True,
                                              data_spec='com.verily.pressure',
                                              sensor_id='2',
                                              algorithm_name='algoA',
                                              algorithm_version='v2'):
                data_filters._TimeRanges.from_list([[
                    int(pd.Timestamp('2022-01-01T12:00:00.000Z').asm8),
                    int(pd.Timestamp('2022-01-03T12:00:00.000Z').asm8)
                ]]),
            data_filters._DataSourceFilterKey('device5',
                                              True,
                                              data_spec='com.verily.pressure',
                                              sensor_id='0',
                                              algorithm_version='v2'):
                data_filters._TimeRanges.from_list([[
                    int(pd.Timestamp('2022-01-01T00:00:00.000Z').asm8),
                    int(pd.Timestamp('2022-01-05T00:00:00.000Z').asm8)
                ]]),
            data_filters._DataSourceFilterKey('device5',
                                              True,
                                              data_spec='com.verily.pressure',
                                              sensor_id='0',
                                              algorithm_name='algoB'):
                data_filters._TimeRanges.from_list([[
                    int(pd.Timestamp('2022-01-01T00:00:00.000Z').asm8),
                    int(pd.Timestamp('2022-01-05T00:00:00.000Z').asm8)
                ]]),
        }

        inclusive_2 = data_filters.DataFilter('project.dataset.inclusive_2',
                                              is_inclusive=True)
        self.assertEqual(inclusive_2.get_filter_table(), expected_time_ranges)

    @mock.patch.object(bigquery, 'Client', autospec=True)
    @mock.patch('pandas.read_gbq', new=mock_read_gbq)
    def test_filter_map_exclusive_1(self, mock_bq_client: mock.MagicMock):

        mock_bq_client.return_value = MockClient()

        # NOTE: the condition is built by iterating on a DataFrameGroupBy. By
        # default, the groupby sorts the group labels. This is why the expected
        # filter dictionary keys are out-of-order compared to the mock table
        # above.
        expected_time_ranges = {
            data_filters._DataSourceFilterKey('device1', False):
                data_filters._TimeRanges.from_list(
                    [[
                        int(pd.Timestamp('2022-01-01T12:00:00.000Z').asm8),
                        int(pd.Timestamp('2022-01-01T15:00:00.000Z').asm8)
                    ],
                     [
                         int(pd.Timestamp('2022-01-02T12:00:00.000Z').asm8),
                         int(pd.Timestamp('2022-01-02T13:00:00.000Z').asm8)
                     ],
                     [
                         int(pd.Timestamp('2022-01-03T12:00:00.000Z').asm8),
                         int(pd.Timestamp('2022-01-04T13:00:00.000Z').asm8)
                     ]]),
            data_filters._DataSourceFilterKey('device2', False):
                data_filters._TimeRanges.from_list(
                    [[
                        int(pd.Timestamp('2022-01-01T12:00:00.000Z').asm8),
                        int(pd.Timestamp('2022-01-01T15:00:00.000Z').asm8)
                    ],
                     [
                         int(pd.Timestamp('2022-01-02T12:00:00.000Z').asm8),
                         int(pd.Timestamp('2022-01-04T12:00:00.000Z').asm8)
                     ]])
        }

        # If the table hasn't been queried, then the filter map should be empty
        exclusive_1 = data_filters.DataFilter(
            'project.dataset.exclusive_1',
            device_id_field='DeviceId',
            start_time_field='start_timestamp_utc',
            end_time_field='end_timestamp_utc',
            annotation_labels=('label1', 'label2'))

        self.assertEqual(exclusive_1.get_filter_table(), expected_time_ranges)

    @mock.patch.object(bigquery, 'Client', autospec=True)
    @mock.patch('pandas.read_gbq', new=mock_read_gbq)
    def test_filter_map_exclusive_2(self, mock_bq_client: mock.MagicMock):

        mock_bq_client.return_value = MockClient()

        # NOTE: the condition is built by iterating on a DataFrameGroupBy. By
        # default, the groupby sorts the group labels. This is why the expected
        # filter dictionary keys are out-of-order compared to the mock table
        # above.
        expected_time_ranges = {
            data_filters._DataSourceFilterKey('device1',
                                              False,
                                              data_spec='com.verily.pressure',
                                              sensor_id='0',
                                              algorithm_name='',
                                              algorithm_version=''):
                data_filters._TimeRanges.from_list([[
                    int(pd.Timestamp('2022-01-01T14:00:00.000Z').asm8),
                    int(pd.Timestamp('2022-01-01T16:00:00.000Z').asm8)
                ]]),
            data_filters._DataSourceFilterKey('device1',
                                              False,
                                              data_spec='com.verily.pressure',
                                              sensor_id='1',
                                              algorithm_name='',
                                              algorithm_version=''):
                data_filters._TimeRanges.from_list([[
                    int(pd.Timestamp('2022-01-02T08:00:00.000Z').asm8),
                    int(pd.Timestamp('2022-01-02T10:00:00.000Z').asm8)
                ]]),
            data_filters._DataSourceFilterKey('device2',
                                              False,
                                              data_spec='com.verily.imu',
                                              sensor_id='2',
                                              algorithm_name='algoA',
                                              algorithm_version='v2'):
                data_filters._TimeRanges.from_list([[
                    int(pd.Timestamp('2022-01-01T01:00:00.000Z').asm8),
                    int(pd.Timestamp('2022-01-05T00:00:00.000Z').asm8)
                ]]),
            data_filters._DataSourceFilterKey('device2',
                                              False,
                                              data_spec='com.verily.pressure',
                                              sensor_id='2',
                                              algorithm_name='algoA',
                                              algorithm_version='v1'):
                data_filters._TimeRanges.from_list([[
                    int(pd.Timestamp('2022-01-01T12:00:00.000Z').asm8),
                    int(pd.Timestamp('2022-01-01T15:00:00.000Z').asm8)
                ]]),
            data_filters._DataSourceFilterKey('device2',
                                              False,
                                              data_spec='com.verily.pressure',
                                              sensor_id='2',
                                              algorithm_name='algoA',
                                              algorithm_version='v2'):
                data_filters._TimeRanges.from_list([[
                    int(pd.Timestamp('2022-01-01T23:00:00.000Z').asm8),
                    int(pd.Timestamp('2022-01-02T01:00:00.000Z').asm8)
                ]]),
        }

        # If the table hasn't been queried, then the filter map should be empty
        exclusive_2 = data_filters.DataFilter(
            'project.dataset.exclusive_2',
            start_time_field='DataPoint.start_time',
            end_time_field='DataPoint.end_time')

        self.assertEqual(exclusive_2.get_filter_table(), expected_time_ranges)


class TestConvertTimestamps(unittest.TestCase):
    'Unit tests for timestamp conversion transforms.'

    def setUp(self):

        self.test_timestamps: List[str] = [
            '2022-02-01T12:34:24.342+0000',
            '2021-11-15T05:01:59.999+0000',
            '2023-03-31T00:00:00.001+0000',
        ]

        self.expected_series = pd.Series([
            np.datetime64(self.test_timestamps[0], 'ns'),
            np.datetime64(self.test_timestamps[1], 'ns'),
            np.datetime64(self.test_timestamps[2], 'ns'),
        ])

    def test_convert_strings(self):
        str_series = pd.Series(self.test_timestamps)

        self.assertTrue(
            self.expected_series.equals(
                data_filters._convert_timestamp_column(str_series)))

    def test_convert_integers(self):
        timestamp_series = pd.to_datetime(pd.Series(self.test_timestamps))
        float_series = timestamp_series.map(pd.Timestamp.timestamp)
        int_series = (float_series * 1000).astype(int)

        self.assertTrue(
            self.expected_series.equals(
                data_filters._convert_timestamp_column(int_series)))

    def test_convert_timestamps(self):
        timestamp_series = pd.to_datetime(pd.Series(self.test_timestamps))

        self.assertTrue(
            self.expected_series.equals(
                data_filters._convert_timestamp_column(timestamp_series)))

    def test_float_timestamps(self):
        timestamp_series = pd.to_datetime(pd.Series(self.test_timestamps))
        float_series = timestamp_series.map(pd.Timestamp.timestamp)

        with self.assertRaises(ValueError):
            data_filters._convert_timestamp_column(float_series)

    def test_int_out_of_range(self):
        timestamp_series = pd.to_datetime(pd.Series(self.test_timestamps))
        float_series = timestamp_series.map(pd.Timestamp.timestamp)
        int_series = (float_series * 1000).astype(int)

        #min int out of range
        with self.assertRaises(ValueError):
            data_filters._convert_timestamp_column(
                pd.concat((int_series, pd.Series([1000]))))

        #max int out of range
        with self.assertRaises(ValueError):
            data_filters._convert_timestamp_column(
                pd.concat((int_series, pd.Series([1000000000]))))


class FilterSchemaTests(unittest.TestCase):
    """Unit tests for schema logic in DataFilters"""

    @mock.patch.object(bigquery.Client, 'get_table', autospec=True)
    def test_filter_mismatched_names(self, mock_table: mock.MagicMock):

        mock_table.return_value.schema = [
            bigquery.SchemaField('DeviceId',
                                 'STRING',
                                 'REQUIRED',
                                 None,
                                 fields=(),
                                 policy_tags=None),
            bigquery.SchemaField(
                'DataPoint',
                'RECORD',
                'REQUIRED',
                None,
                fields=(bigquery.SchemaField('end_time_milliseconds',
                                             'INTEGER',
                                             'NULLABLE',
                                             None,
                                             fields=(),
                                             policy_tags=None),
                        bigquery.SchemaField('starting_time',
                                             'TIMESTAMP',
                                             'NULLABLE',
                                             None,
                                             fields=(),
                                             policy_tags=None)),
                policy_tags=None)
        ]

        with self.assertRaises(ValueError):
            _ = data_filters.DataFilter(
                table_id=
                'project.dataset.com_verily_annotation_overlapping__data')

    @mock.patch.object(bigquery.Client, 'get_table', autospec=True)
    def test_filter_wrong_types(self, mock_table: mock.MagicMock):

        mock_table.return_value.schema = [
            bigquery.SchemaField('DeviceID',
                                 'INTEGER',
                                 'REQUIRED',
                                 None,
                                 fields=(),
                                 policy_tags=None),
            bigquery.SchemaField('DataPointTime',
                                 'NOTATYPE',
                                 'REQUIRED',
                                 None,
                                 fields=(),
                                 policy_tags=None),
            bigquery.SchemaField('DataPoint',
                                 'RECORD',
                                 'REQUIRED',
                                 None,
                                 fields=(bigquery.SchemaField(
                                     'end_time_millis',
                                     'FLOAT',
                                     'NULLABLE',
                                     None,
                                     fields=(),
                                     policy_tags=None),),
                                 policy_tags=None)
        ]

        with self.assertRaises(ValueError):
            _ = data_filters.DataFilter(
                table_id=
                'project.dataset.com_verily_annotation_overlapping__data')

class AlgorithmVersionFilterTest(unittest.TestCase):

    ALGO_VERSION = 'v2'
    TEST_ALGO_VERSION_FIELD = ALGO_VERSION_FIELD
    TEST_ALGO_VERSION_TABLE_ID = ALGO_VERSION_TABLE_ID

    @mock.patch.object(bigquery, 'Client', autospec=True)
    @mock.patch('pandas.read_gbq', new=mock_read_gbq)
    def test_sql_query_filter(self, mock_bq_client: mock.MagicMock):
        """
        Tests that algo version filter appears in the SQL query (WHERE clause).
        """

        mock_bq_client.return_value = MockClient()

        data_filter = data_filters.DataFilter(
            self.TEST_ALGO_VERSION_TABLE_ID,
            algorithm_version=self.ALGO_VERSION,
            algorithm_version_field=self.TEST_ALGO_VERSION_FIELD,
        )

        generated_query = data_filter._get_table_query_str()

        # Ensure clause is in the generated SQL query
        expected_clause = (
            f" AND {self.TEST_ALGO_VERSION_FIELD} = '{self.ALGO_VERSION}'")
        self.assertIn(
            expected_clause,
            generated_query,
            'SQL query did not include the algorithm_version clause.')

        # Ensure time_range_map only includes v2 entries
        expected_time_ranges = {
            data_filters._DataSourceFilterKey('D1', False):
                data_filters._TimeRanges.from_list(
                    [[
                        int(pd.Timestamp('2022-03-01T02:00:00.000Z').asm8),
                        int(pd.Timestamp('2022-03-01T03:00:00.000Z').asm8)
                    ]]),
        }
        time_range_map = data_filter.get_filter_table()

        self.assertEqual(
            time_range_map,
            expected_time_ranges,
            'The final filter map is incorrect after filtering.')

if __name__ == '__main__':
    unittest.main()
