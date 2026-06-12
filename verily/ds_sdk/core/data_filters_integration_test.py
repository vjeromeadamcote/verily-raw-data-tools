"""Unit tests for data_filters.py when used within the SensorsIO class"""

# pylint: disable=protected-access
import unittest
from unittest import mock

import apache_beam as beam
from apache_beam.testing.test_pipeline import TestPipeline
from google.cloud import bigquery
import pandas as pd

from verily.ds_sdk.core import data_filters
from verily.ds_sdk.core import options
from verily.ds_sdk.core import schemas
from verily.ds_sdk.core import sensors_io
from verily.ds_sdk.core.io.bigquery import data_points_source
from verily.ds_sdk.core.io.data_source_cache import DataSourceCache
from verily.ds_sdk.core.utils import timestamps
from verily.ds_sdk.protos import types_pb2

from .data_filters_test import mock_read_gbq
from .data_filters_test import MockClient

################################################################################
# Used for making mock DataPoints to test the filter with


def mock_metadata(device_id: str, data_source_id: int):
    return schemas.data_point_metadata_for_raw_data(
        data_source_id=data_source_id,
        device_id=device_id,
        participant_id='user',
        participant_namespace=0,
        echo_metadata=None,
        sensor_store_metadata=None,
        annotation_labels=set())


def mock_pressure_point(*,
                        device_id: str,
                        timestamp_delta_hours: int,
                        data_source_id: int = 0) -> schemas.Pressure:
    return schemas.Pressure(
        data_point_metadata=mock_metadata(device_id, data_source_id),
        measurement_timestamp_utc=timestamps.datetime_to_beam_timestamp(
            pd.Timestamp('2022-01-01 00:00:00+0000') +
            pd.Timedelta(hours=timestamp_delta_hours)),
        pressure=100)


# Mock input Pressure DataPoints
num_hours = 4 * 24  # four days, one per hour
mock_input_data = [
    mock_pressure_point(device_id=device_id, timestamp_delta_hours=i)
    for device_id in [f'device{j}'
                      for j in range(1, 11)]
    for i in range(0, num_hours)
]


def datapoint_to_str(dp: schemas.DataPoint):
    return str(
        (dp.data_point_metadata.device_id,
         dp.data_point_metadata.data_source_id,
         dp.measurement_timestamp_utc.to_utc_datetime().strftime('%Y%m%d%H')))


def compare_lists(grouped_pcol, expected_list):
    _, pcol_list = grouped_pcol
    unexpected = [dp for dp in pcol_list if dp not in expected_list]
    missing = [dp for dp in expected_list if dp not in pcol_list]

    if len(unexpected) > 0 or len(missing) > 0:
        raise AssertionError('Lists are not equal:\nmissing:\n' +
                             '\n'.join(missing) + '\nunexpected:\n' +
                             '\n'.join(unexpected))


################################################################################
# Mock data filter DataFrame used in testing individual filters

mock_filter_dataframe = pd.DataFrame([{
    'device_id': 'device1',
    'start_time': '2022-01-01T12:00:00.000Z',
    'end_time': pd.Timestamp('2022-01-01T15:00:00.000Z')
}, {
    'device_id': 'device1',
    'start_time': '2022-01-02T12:00:00.000Z',
    'end_time': pd.Timestamp('2022-01-02T13:00:00.000Z')
}, {
    'device_id': 'device1',
    'start_time': '2022-01-03T12:00:00.000Z',
    'end_time': pd.Timestamp('2022-01-04T13:00:00.000Z')
}, {
    'device_id': 'device5',
    'start_time': '2022-01-02T12:00:00.000Z',
    'end_time': pd.Timestamp('2022-01-02T12:00:00.001Z')
}, {
    'device_id': 'device10',
    'start_time': '2022-01-01T12:00:00.000Z',
    'end_time': pd.Timestamp('2022-01-01T15:00:00.000Z')
}, {
    'device_id': 'device10',
    'start_time': '2022-01-02T12:00:00.000Z',
    'end_time': pd.Timestamp('2022-01-04T11:59:59.999Z')
}])

overlaping_schema = [
    bigquery.SchemaField('DeviceID',
                         'STRING',
                         'REQUIRED',
                         None,
                         fields=(),
                         policy_tags=None),
    bigquery.SchemaField('DataPointWriteTime',
                         'TIMESTAMP',
                         'NULLABLE',
                         None,
                         fields=(),
                         policy_tags=None),
    bigquery.SchemaField('DataPoint',
                         'RECORD',
                         'REQUIRED',
                         None,
                         fields=(bigquery.SchemaField('end_time_millis',
                                                      'INTEGER',
                                                      'NULLABLE',
                                                      None,
                                                      fields=(),
                                                      policy_tags=None),),
                         policy_tags=None),
    bigquery.SchemaField('SnapshotTime',
                         'TIMESTAMP',
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
    bigquery.SchemaField('DeletedTime',
                         'TIMESTAMP',
                         'NULLABLE',
                         None,
                         fields=(),
                         policy_tags=None),
    bigquery.SchemaField('DataSourceID',
                         'INTEGER',
                         'NULLABLE',
                         None,
                         fields=(),
                         policy_tags=None)
]


class MockDataPointRowSource(beam.PTransform):

    def __init__(self, input_data, data_source_cache):
        super().__init__()
        self._input_data = input_data
        self._data_source_cache = data_source_cache

    def expand(self, input_or_inputs: beam.PCollection):

        return (
            input_or_inputs | 'Create input' >> beam.Create(self._input_data),
            input_or_inputs |
            'Create DataSourceCache' >> beam.Create([self._data_source_cache]))


mock_data_source = types_pb2.DataSource()
mock_data_source.data_spec.name = 'com.verily.pressure'
mock_data_source_cache = DataSourceCache({0: mock_data_source})


class DataFilerSensorsIOTest(unittest.TestCase):

    @mock.patch.object(pd, 'read_gbq', autospec=True)
    @mock.patch.object(bigquery.Client, 'get_table', autospec=True)
    @mock.patch.object(data_points_source, 'DataPointRowSource', autospec=True)
    def test_echo_inclusive_filter(self,
                                   mock_data_points_source: mock.MagicMock,
                                   mock_table: mock.MagicMock,
                                   mock_pd_gbq: mock.MagicMock):

        mock_data_points_source.return_value = MockDataPointRowSource(
            mock_input_data, mock_data_source_cache)
        mock_table.return_value.schema = overlaping_schema
        mock_pd_gbq.return_value = mock_filter_dataframe

        inclusive_filter = data_filters.DataFilter(
            table_id='project.dataset.com_verily_annotation_overlapping__data',
            is_inclusive=True)
        # pylint: disable=line-too-long
        expected_output = [
            # start at device1 2022-01-01T12:00:00.000Z, end before device1 2022-01-01T15:00:00.000Z
            *mock_input_data[12:15],
            # start at device1 2022-01-02T12:00:00.000Z, end before device1 2022-01-02T13:00:00.000Z
            *mock_input_data[(1 * 24 + 12):(1 * 24 + 13)],
            # start at device1 2022-01-03T12:00:00.000Z, end before device1 2022-01-04T13:00:00.000Z
            *mock_input_data[(2 * 24 + 12):(3 * 24 + 13)],
            # start at device5 2022-01-02T12:00:00.000Z, end before device5 2022-01-02T13:00:00.000Z
            *mock_input_data[(4 * 96 + 1 * 24 + 12):(4 * 96 + 1 * 24 + 13)],
            # start at device10 2022-01-01T12:00:00.000Z, device10 2022-01-01T15:00:00.000Z
            *mock_input_data[(9 * 96 + 12):(9 * 96 + 15)],
            # start at end before device10 2022-01-02T12:00:00.000Z, end before device10 2022-01-04T12:00:00.000Z
            *mock_input_data[(9 * 96 + 1 * 24 + 12):(9 * 96 + 3 * 24 + 12)]
        ]
        # pylint: enable=line-too-long
        expected_output = [datapoint_to_str(dp) for dp in expected_output]

        test_io = sensors_io.SensorsIO(registry='DevTeam',
                                       runner='DirectRunner',
                                       env='prod')

        with TestPipeline() as p:
            test_io._p = p

            _ = test_io.echo_data_point_rows(
                data_spec_name='com.verily.pressure',
                source_options=options.BatchSourceOptions(),
                condition=None,
                annotation_inner_join_options=None,
                incremental_query_options=None,
                data_filter_list=[
                    inclusive_filter
                ]) | 'map to str' >> beam.Map(datapoint_to_str) | beam.GroupBy(
                    lambda dp: 1) | beam.Map(compare_lists, expected_output)

    @mock.patch.object(pd, 'read_gbq', autospec=True)
    @mock.patch.object(bigquery.Client, 'get_table', autospec=True)
    @mock.patch.object(data_points_source, 'DataPointRowSource', autospec=True)
    def test_echo_exclusive_filter(self,
                                   mock_data_points_source: mock.MagicMock,
                                   mock_table: mock.MagicMock,
                                   mock_pd_gbq: mock.MagicMock):
        """Tests exclusive filter behavior as well as custom start/end field
        names"""

        mock_data_points_source.return_value = MockDataPointRowSource(
            mock_input_data, mock_data_source_cache)
        mock_table.return_value.schema = overlaping_schema
        mock_pd_gbq.return_value = mock_filter_dataframe

        exclusive_filter = data_filters.DataFilter(
            table_id='project.dataset.com_verily_annotation_overlapping__data')
        # pylint: disable=line-too-long
        expected_output = [
            # start at device1 2022-01-01T00:00:00.000Z, end before device1 2022-01-01T12:00:00.000Z
            *mock_input_data[:12],
            # start at device1 2022-01-01T15:00:00.000Z, end before device1 2022-01-02T12:00:00.000Z
            *mock_input_data[15:(1 * 24 + 12)],
            # start at device1 2022-01-02T13:00:00.000Z, end before device1 2022-01-03T12:00:00.000Z
            *mock_input_data[(1 * 24 + 13):(2 * 24 + 12)],
            # start at device1 2022-01-04T13:00:00.000Z, end before device5 2022-01-02T12:00:00.000Z
            *mock_input_data[(3 * 24 + 13):(4 * 96 + 1 * 24 + 12)],
            # start at device5 2022-01-02T13:00:00.000Z, end before device10 2022-01-01T12:00:00.000Z
            *mock_input_data[(4 * 96 + 1 * 24 + 13):(9 * 96 + 12)],
            # start at device10 2022-01-01T15:00:00.000Z, end before device10 2022-01-02T12:00:00.000Z
            *mock_input_data[(9 * 96 + 15):(9 * 96 + 1 * 24 + 12)],
            # start at device10 2022-01-04T12:00:00.000Z, end before device10 2022-01-05T00:00:00.000Z
            *mock_input_data[(9 * 96 + 3 * 24 + 12):]
        ]
        # pylint: enable=line-too-long
        expected_output = [datapoint_to_str(dp) for dp in expected_output]

        test_io = sensors_io.SensorsIO(registry='DevTeam',
                                       runner='DirectRunner',
                                       env='prod')

        with TestPipeline() as p:
            test_io._p = p

            _ = test_io.echo_data_point_rows(
                data_spec_name='com.verily.pressure',
                source_options=options.BatchSourceOptions(),
                condition=None,
                annotation_inner_join_options=None,
                incremental_query_options=None,
                data_filter_list=[
                    exclusive_filter
                ]) | 'map to str' >> beam.Map(datapoint_to_str) | beam.GroupBy(
                    lambda dp: 1) | beam.Map(compare_lists, expected_output)

    @mock.patch.object(bigquery, 'Client', autospec=True)
    @mock.patch.object(data_points_source, 'DataPointRowSource', autospec=True)
    @mock.patch('pandas.read_gbq', new=mock_read_gbq)
    def test_multiple_inclusive(self, mock_data_points_source: mock.MagicMock,
                                mock_bq_client: mock.MagicMock):
        """Test with multiple filters, same as used in
        data_filters_test.DataFiltersTest.test_filter_combining

        Also tests using filters with all three different timestamp types

        The file 'data_filters_integration_test.csv' shows how the time ranges
        described each DataFilter DataFrame are mapped to individual DeviceID
        and DataSourceID level filters and then merged together to determine
        the number of remaining DataPoints
        """

        data_source_0 = types_pb2.DataSource()
        data_source_0.data_spec.name = 'com.verily.pressure'
        data_source_0.sensor.id = '0'
        data_source_1 = types_pb2.DataSource()
        data_source_1.data_spec.name = 'com.verily.pressure'
        data_source_1.sensor.id = '1'
        data_source_2 = types_pb2.DataSource()
        data_source_2.data_spec.name = 'com.verily.pressure'
        data_source_2.sensor.id = '2'
        data_source_2.algorithm.name = 'algoA'
        data_source_2.algorithm.version = 'v1'
        data_source_3 = types_pb2.DataSource()
        data_source_3.data_spec.name = 'com.verily.pressure'
        data_source_3.sensor.id = '2'
        data_source_3.algorithm.name = 'algoA'
        data_source_3.algorithm.version = 'v2'
        data_source_4 = types_pb2.DataSource()
        data_source_4.data_spec.name = 'com.verily.pressure'
        data_source_5 = types_pb2.DataSource()
        data_source_5.data_spec.name = 'com.verily.pressure'
        mock_data_source_cache2 = DataSourceCache({
            0: data_source_0,
            1: data_source_1,
            2: data_source_2,
            3: data_source_3,
            4: data_source_4,
            5: data_source_5
        })

        mock_input_data2 = [
            mock_pressure_point(device_id=device_id,
                                timestamp_delta_hours=i,
                                data_source_id=dsid)
            for device_id, dsid in (('device1', 0), ('device1', 1),
                                    ('device2', 2), ('device2', 3),
                                    ('device3', 4), ('device4', 5))
            for i in range(0, num_hours)
        ]
        # pylint: disable=line-too-long
        expected_output = [
            # start at device1, datasource0 2022-01-01T14:00:00.000Z, end before device1, datasource0 2022-01-01T17:00:00.000Z
            *mock_input_data2[14:17],
            # start at device1, datasource1 2022-01-01T06:00:00.000Z, end before device1, datasource1 2022-01-01T18:00:00.000Z
            *mock_input_data2[(96 + 6):(96 + 18)],
            # start at device1, datasource1 2022-01-02T11:00:00.000Z, end before device1, datasource1 2022-01-02T14:00:00.000Z
            *mock_input_data2[(96 + 24 + 11):(96 + 24 + 14)],
            # start at device1, datasource1 2022-01-03T13:00:00.000Z, end before device1, datasource1 2022-01-04T6:00:00.000Z
            *mock_input_data2[(96 + 2 * 24 + 13):(96 + 3 * 24 + 6)],
            # start at device2, datasource3 2022-01-01T12:00:00.000Z, end before device2, datasource3 2022-01-03T12:00:00.000Z
            *mock_input_data2[(3 * 96 + 12):(3 * 96 + 2 * 24 + 12)],
        ]
        # pylint: enable=line-too-long
        expected_output = [datapoint_to_str(dp) for dp in expected_output]

        mock_data_points_source.return_value = MockDataPointRowSource(
            mock_input_data2, mock_data_source_cache2)
        mock_bq_client.return_value = MockClient()

        data_filter_list = [
            data_filters.DataFilter('project.dataset.inclusive_1',
                                    is_inclusive=True,
                                    device_id_field='device_id',
                                    start_time_field='start_timestamp_utc',
                                    end_time_field='end_timestamp_utc'),
            data_filters.DataFilter('project.dataset.inclusive_2',
                                    is_inclusive=True),
        ]

        test_io = sensors_io.SensorsIO(registry='DevTeam',
                                       runner='DirectRunner',
                                       env='prod')

        with TestPipeline() as p:
            test_io._p = p

            _ = test_io.echo_data_point_rows(
                data_spec_name='com.verily.pressure',
                source_options=options.BatchSourceOptions(),
                condition=None,
                annotation_inner_join_options=None,
                incremental_query_options=None,
                data_filter_list=data_filter_list) | 'map to str' >> beam.Map(
                    datapoint_to_str) | beam.GroupBy(lambda dp: 1) | beam.Map(
                        compare_lists, expected_output)

    @mock.patch.object(bigquery, 'Client', autospec=True)
    @mock.patch.object(data_points_source, 'DataPointRowSource', autospec=True)
    @mock.patch('pandas.read_gbq', new=mock_read_gbq)
    def test_multiple_exclusive(self, mock_data_points_source: mock.MagicMock,
                                mock_bq_client: mock.MagicMock):
        """Test with multiple filters, same as used in
        data_filters_test.DataFiltersTest.test_filter_combining

        Also tests using filters with all three different timestamp types

        The file 'data_filters_integration_test.csv' shows how the time ranges
        described each DataFilter DataFrame are mapped to individual DeviceID
        and DataSourceID level filters and then merged together to determine
        the number of remaining DataPoints
        """

        data_source_0 = types_pb2.DataSource()
        data_source_0.data_spec.name = 'com.verily.pressure'
        data_source_0.sensor.id = '0'
        data_source_1 = types_pb2.DataSource()
        data_source_1.data_spec.name = 'com.verily.pressure'
        data_source_1.sensor.id = '1'
        data_source_2 = types_pb2.DataSource()
        data_source_2.data_spec.name = 'com.verily.pressure'
        data_source_2.sensor.id = '2'
        data_source_2.algorithm.name = 'algoA'
        data_source_2.algorithm.version = 'v1'
        data_source_3 = types_pb2.DataSource()
        data_source_3.data_spec.name = 'com.verily.pressure'
        data_source_3.sensor.id = '2'
        data_source_3.algorithm.name = 'algoA'
        data_source_3.algorithm.version = 'v2'
        data_source_5 = types_pb2.DataSource()
        data_source_5.data_spec.name = 'com.verily.pressure'
        mock_data_source_cache2 = DataSourceCache({
            0: data_source_0,
            1: data_source_1,
            2: data_source_2,
            3: data_source_3,
            5: data_source_5
        })

        mock_input_data2 = [
            mock_pressure_point(device_id=device_id,
                                timestamp_delta_hours=i,
                                data_source_id=dsid)
            for device_id, dsid in (('device1', 0), ('device1', 1),
                                    ('device2', 2), ('device2', 3), ('device4',
                                                                     5))
            for i in range(0, num_hours)
        ]
        # pylint: disable=line-too-long
        expected_output = [
            # start at device1, datasource0 2022-01-01T00:00:00.000Z, end before device1, datasource0 2022-01-01T12:00:00.000Z
            *mock_input_data2[:12],
            # start at device1, datasource0 2022-01-01T16:00:00.000Z, end before device1, datasource0 2022-01-02T12:00:00.000Z
            *mock_input_data2[16:(1 * 24 + 12)],
            # start at device1, datasource0 2022-01-02T13:00:00.000Z, end before device1, datasource0 2022-01-03T12:00:00.000Z
            *mock_input_data2[(1 * 24 + 13):(2 * 24 + 12)],
            # start at device1, datasource0 2022-01-04T13:00:00.000Z, end before device1, datasource1 2022-01-01T12:00:00.000Z
            *mock_input_data2[(3 * 24 + 13):(96 + 12)],
            # start at device1, datasource1 2022-01-01T15:00:00.000Z, end before device1, datasource1 2022-01-02T08:00:00.000Z
            *mock_input_data2[(96 + 15):(96 + 24 + 8)],
            # start at device1, datasource1 2022-01-02T10:00:00.000Z, end before device1, datasource1 2022-01-02T12:00:00.000Z
            *mock_input_data2[(96 + 24 + 10):(96 + 24 + 12)],
            # start at device1, datasource1 2022-01-02T13:00:00.000Z, end before device1, datasource1 2022-01-03T12:00:00.000Z
            *mock_input_data2[(96 + 24 + 13):(96 + 2 * 24 + 12)],
            # start at device1, datasource1 2022-01-04T13:00:00.000Z, end before device2, datasource2 2022-01-01T12:00:00.000Z
            *mock_input_data2[(96 + 3 * 24 + 13):(2 * 96 + 12)],
            # start at device2, datasource2 2022-01-01T15:00:00.000Z, end before device2, datasource2 2022-01-02T12:00:00.000Z
            *mock_input_data2[(2 * 96 + 15):(2 * 96 + 1 * 24 + 12)],
            # start at device2, datasource2 2022-01-04T12:00:00.000Z, end before device2, datasource3 2022-01-01T12:00:00.000Z
            *mock_input_data2[(2 * 96 + 3 * 24 + 12):(3 * 96 + 12)],
            # start at device2, datasource3 2022-01-01T15:00:00.000Z, end before device2, datasource3 2022-01-01T23:00:00.000Z
            *mock_input_data2[(3 * 96 + 15):(3 * 96 + 23)],
            # start at device2, datasource3 2022-01-02T01:00:00.000Z, end before device2, datasource3 2022-01-02T12:00:00.000Z
            *mock_input_data2[(3 * 96 + 24 + 1):(3 * 96 + 24 + 12)],
            # start at device2, datasource3 2022-01-04T12:00:00.000Z, end before device4
            *mock_input_data2[(3 * 96 + 3 * 24 + 12):(4 * 96)],
            # all of device4
            *mock_input_data2[(4 * 96):],
        ]

        # pylint: enable=line-too-long
        expected_output = [datapoint_to_str(dp) for dp in expected_output]

        mock_data_points_source.return_value = MockDataPointRowSource(
            mock_input_data2, mock_data_source_cache2)
        mock_bq_client.return_value = MockClient()

        data_filter_list = [
            data_filters.DataFilter('project.dataset.exclusive_1',
                                    device_id_field='DeviceId',
                                    start_time_field='start_timestamp_utc',
                                    end_time_field='end_timestamp_utc',
                                    annotation_labels=('label1', 'label2')),
            data_filters.DataFilter('project.dataset.exclusive_2',
                                    start_time_field='DataPoint.start_time',
                                    end_time_field='DataPoint.end_time'),
        ]

        test_io = sensors_io.SensorsIO(registry='DevTeam',
                                       runner='DirectRunner',
                                       env='prod')

        with TestPipeline() as p:
            test_io._p = p

            _ = test_io.echo_data_point_rows(
                data_spec_name='com.verily.pressure',
                source_options=options.BatchSourceOptions(),
                condition=None,
                annotation_inner_join_options=None,
                incremental_query_options=None,
                data_filter_list=data_filter_list) | 'map to str' >> beam.Map(
                    datapoint_to_str) | beam.GroupBy(lambda dp: 1) | beam.Map(
                        compare_lists, expected_output)

    @mock.patch.object(bigquery, 'Client', autospec=True)
    @mock.patch.object(data_points_source, 'DataPointRowSource', autospec=True)
    @mock.patch('pandas.read_gbq', new=mock_read_gbq)
    def test_filter_combining(self, mock_data_points_source: mock.MagicMock,
                              mock_bq_client: mock.MagicMock):
        """Test with multiple filters, same as used in
        data_filters_test.DataFiltersTest.test_filter_combining

        Also tests using filters with all three different timestamp types

        The file 'data_filters_integration_test.csv' shows how the time ranges
        described each DataFilter DataFrame are mapped to individual DeviceID
        and DataSourceID level filters and then merged together to determine
        the number of remaining DataPoints
        """

        data_source_0 = types_pb2.DataSource()
        data_source_0.data_spec.name = 'com.verily.pressure'
        data_source_0.sensor.id = '0'
        data_source_1 = types_pb2.DataSource()
        data_source_1.data_spec.name = 'com.verily.pressure'
        data_source_1.sensor.id = '1'
        data_source_2 = types_pb2.DataSource()
        data_source_2.data_spec.name = 'com.verily.pressure'
        data_source_2.sensor.id = '2'
        data_source_2.algorithm.name = 'algoA'
        data_source_2.algorithm.version = 'v1'
        data_source_3 = types_pb2.DataSource()
        data_source_3.data_spec.name = 'com.verily.pressure'
        data_source_3.sensor.id = '2'
        data_source_3.algorithm.name = 'algoA'
        data_source_3.algorithm.version = 'v2'
        data_source_5 = types_pb2.DataSource()
        data_source_5.data_spec.name = 'com.verily.pressure'
        mock_data_source_cache2 = DataSourceCache({
            0: data_source_0,
            1: data_source_1,
            2: data_source_2,
            3: data_source_3,
            5: data_source_5
        })

        mock_input_data2 = [
            mock_pressure_point(device_id=device_id,
                                timestamp_delta_hours=i,
                                data_source_id=dsid)
            for device_id, dsid in (('device1', 0), ('device1', 1),
                                    ('device2', 2), ('device2', 3), ('device4',
                                                                     5))
            for i in range(0, num_hours)
        ]
        # pylint: disable=line-too-long
        expected_output = [
            # start at device1, datasource0 2022-01-01T16:00:00.000Z, end before device1, datasource0 2022-01-01T17:00:00.000Z
            *mock_input_data2[16:17],
            # start at device1, datasource1 2022-01-01T06:00:00.000Z, end before device1, datasource1 2022-01-01T12:00:00.000Z
            *mock_input_data2[(96 + 6):(96 + 12)],
            # start at device1, datasource1 2022-01-01T15:00:00.000Z, end before device1, datasource1 2022-01-01T18:00:00.000Z
            *mock_input_data2[(96 + 15):(96 + 18)],
            # start at device1, datasource1 2022-01-02T11:00:00.000Z, end before device1, datasource1 2022-01-02T12:00:00.000Z
            *mock_input_data2[(96 + 1 * 24 + 11):(96 + 1 * 24 + 12)],
            # start at device1, datasource1 2022-01-02T13:00:00.000Z, end before device1, datasource1 2022-01-02T14:00:00.000Z
            *mock_input_data2[(96 + 1 * 24 + 13):(96 + 1 * 24 + 14)],
            # start at device2, datasource3 2022-01-01T15:00:00.000Z, end before device2, datasource3 2022-01-01T23:00:00.000Z
            *mock_input_data2[(3 * 96 + 15):(3 * 96 + 23)],
            # start at device2, datasource3 2022-01-01T15:00:00.000Z, end before device2, datasource3 2022-01-01T23:00:00.000Z
            *mock_input_data2[(3 * 96 + 24 + 1):(3 * 96 + 24 + 12)],
        ]
        # pylint: enable=line-too-long
        expected_output = [datapoint_to_str(dp) for dp in expected_output]

        mock_data_points_source.return_value = MockDataPointRowSource(
            mock_input_data2, mock_data_source_cache2)
        mock_bq_client.return_value = MockClient()

        data_filter_list = [
            data_filters.DataFilter('project.dataset.exclusive_1',
                                    device_id_field='DeviceId',
                                    start_time_field='start_timestamp_utc',
                                    end_time_field='end_timestamp_utc',
                                    annotation_labels=('label1', 'label2')),
            data_filters.DataFilter('project.dataset.exclusive_2',
                                    start_time_field='DataPoint.start_time',
                                    end_time_field='DataPoint.end_time'),
            data_filters.DataFilter('project.dataset.inclusive_1',
                                    is_inclusive=True,
                                    device_id_field='device_id',
                                    start_time_field='start_timestamp_utc',
                                    end_time_field='end_timestamp_utc'),
            data_filters.DataFilter('project.dataset.inclusive_2',
                                    is_inclusive=True),
        ]

        test_io = sensors_io.SensorsIO(registry='DevTeam',
                                       runner='DirectRunner',
                                       env='prod')

        with TestPipeline() as p:
            test_io._p = p

            _ = test_io.echo_data_point_rows(
                data_spec_name='com.verily.pressure',
                source_options=options.BatchSourceOptions(),
                condition=None,
                annotation_inner_join_options=None,
                incremental_query_options=None,
                data_filter_list=data_filter_list) | 'map to str' >> beam.Map(
                    datapoint_to_str) | beam.GroupBy(lambda dp: 1) | beam.Map(
                        compare_lists, expected_output)


if __name__ == '__main__':
    unittest.main()
