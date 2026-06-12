"""Tests for group_into_data_frames."""

from typing import Any, Dict
import unittest

import apache_beam as beam
from apache_beam.testing.test_pipeline import TestPipeline
from apache_beam.testing.util import assert_that
from apache_beam.testing.util import equal_to
from apache_beam.utils.timestamp import Timestamp
import pandas as pd

from verily.ds_sdk.contrib import rialto_timezone_fix
from verily.ds_sdk.core import schemas
from verily.ds_sdk.core.io.data_source_cache import DataSourceCache
from verily.ds_sdk.core.transforms import BuildAnnotationDataFrames
from verily.ds_sdk.core.transforms import BuildDataPointDataFrames
from verily.ds_sdk.protos import types_pb2


def dataframes_equal(left: pd.DataFrame, right: pd.DataFrame):
    return left.equals(right)


def dataframe_dicts_equal(left: Dict[str, pd.DataFrame],
                          right: Dict[str, pd.DataFrame]):
    if left.keys() != right.keys():
        return False
    for key in left.keys():
        if not left[key].equals(right[key]):
            return False
    return True


def build_pandas_timestamp(timestamp_str: str) -> pd.Timestamp:
    return pd.Timestamp(timestamp_str, tz='UTC')


def build_beam_timestamp(timestamp_str: str) -> Timestamp:
    return Timestamp.from_utc_datetime(build_pandas_timestamp(timestamp_str))


def build_annotation(device_id: str,
                     start_timestamp_str: str,
                     end_timestamp_str: str,
                     label='annotation_label') -> schemas.Annotation:

    annotation_metadata = schemas.AnnotationMetadata(device_id=device_id,
                                                     participant_id='123',
                                                     participant_namespace=1,
                                                     version_name=None,
                                                     version_number=None,
                                                     input_data_info=[])

    return schemas.Annotation(
        annotation_label=label,
        start_timestamp_utc=build_beam_timestamp(start_timestamp_str),
        end_timestamp_utc=build_beam_timestamp(end_timestamp_str),
        annotation_metadata=annotation_metadata)


def build_annotation_dict(device_id: str,
                          start_timestamp_str: str,
                          end_timestamp_str: str,
                          label='annotation_label') -> Dict[Any, Any]:
    return {
        'annotation_label': label,
        'start_timestamp_utc': build_pandas_timestamp(start_timestamp_str),
        'end_timestamp_utc': build_pandas_timestamp(end_timestamp_str),
        'annotation_metadata': {
            'device_id': device_id,
            'participant_id': '123',
            'participant_namespace': 1,
            'version_name': None,
            'version_number': None,
            'input_data_info': [],
        },
    }


def build_pressure_data_point(device_id: str,
                              timestamp_str: str,
                              data_source_id: int = 12345) -> schemas.Pressure:

    data_point_metadata = schemas.data_point_metadata_for_raw_data(
        data_source_id=data_source_id,
        device_id=device_id,
        participant_id='123',
        participant_namespace=1,
        echo_metadata=None,
        sensor_store_metadata=None,
        annotation_labels=set())

    return schemas.Pressure(
        data_point_metadata=data_point_metadata,
        measurement_timestamp_utc=build_beam_timestamp(timestamp_str),
        pressure=1)


def build_pressure_data_point_dict(
        device_id: str,
        timestamp_str: str,
        data_source_id: int = 12345) -> Dict[Any, Any]:
    return {
        'data_point_metadata': {
            'data_source_id': data_source_id,
            'device_id': device_id,
            'participant_id': '123',
            'participant_namespace': 1,
            'echo_metadata': None,
            'sensor_store_metadata': None,
            'annotation_labels': set(),
        },
        'measurement_timestamp_utc': build_pandas_timestamp(timestamp_str),
        'pressure': 1,
    }


def build_humidity_data_point(device_id: str,
                              timestamp_str: str,
                              data_source_id: int = 12345) -> schemas.Humidity:

    data_point_metadata = schemas.data_point_metadata_for_raw_data(
        data_source_id=data_source_id,
        device_id=device_id,
        participant_id='123',
        participant_namespace=1,
        echo_metadata=None,
        sensor_store_metadata=None,
        annotation_labels=set())

    return schemas.Humidity(
        data_point_metadata=data_point_metadata,
        measurement_timestamp_utc=build_beam_timestamp(timestamp_str),
        humidity=1)


def build_humidity_data_point_dict(
        device_id: str,
        timestamp_str: str,
        data_source_id: int = 12345) -> Dict[Any, Any]:
    return {
        'data_point_metadata': {
            'data_source_id': data_source_id,
            'device_id': device_id,
            'participant_id': '123',
            'participant_namespace': 1,
            'echo_metadata': None,
            'sensor_store_metadata': None,
            'annotation_labels': set(),
        },
        'measurement_timestamp_utc': build_pandas_timestamp(timestamp_str),
        'humidity': 1,
    }


class GroupIntoDataFramesTest(unittest.TestCase):

    def test_group_data_points_multiple_keys(self):

        data_points = [
            build_pressure_data_point('device_1', '2020-01-01 12:00:00 UTC'),
            build_pressure_data_point('device_1', '2020-01-01 13:00:00 UTC'),
            build_pressure_data_point('device_2', '2020-01-01 13:00:00 UTC'),
        ]

        group_1_dicts = [
            build_pressure_data_point_dict('device_1',
                                           '2020-01-01 12:00:00 UTC'),
            build_pressure_data_point_dict('device_1',
                                           '2020-01-01 13:00:00 UTC'),
        ]
        group_2_dicts = [
            build_pressure_data_point_dict('device_2',
                                           '2020-01-01 13:00:00 UTC'),
        ]
        expected = [pd.DataFrame(group_1_dicts), pd.DataFrame(group_2_dicts)]

        with TestPipeline() as p:
            output = (p | beam.Create(data_points) |
                      BuildDataPointDataFrames.PerParticipantDevice())

            assert_that(output, equal_to(expected, equals_fn=dataframes_equal))

    def test_group_data_points_multiple_keys_with_timestamp(self):

        data_points = [
            build_pressure_data_point('device_1', '2020-01-01 12:00:00 UTC'),
            build_pressure_data_point('device_1', '2020-01-01 12:00:00 UTC'),
            build_pressure_data_point('device_1', '2020-01-01 13:00:00 UTC'),
            build_pressure_data_point('device_2', '2020-01-01 13:00:00 UTC'),
        ]
        group_1_dicts = [
            build_pressure_data_point_dict('device_1',
                                           '2020-01-01 12:00:00 UTC'),
            build_pressure_data_point_dict('device_1',
                                           '2020-01-01 12:00:00 UTC'),
        ]
        group_2_dicts = [
            build_pressure_data_point_dict('device_1',
                                           '2020-01-01 13:00:00 UTC'),
        ]
        group_3_dicts = [
            build_pressure_data_point_dict('device_2',
                                           '2020-01-01 13:00:00 UTC'),
        ]
        expected = [
            pd.DataFrame(group_1_dicts),
            pd.DataFrame(group_2_dicts),
            pd.DataFrame(group_3_dicts)
        ]

        with TestPipeline() as p:
            output = (p | beam.Create(data_points) |
                      BuildDataPointDataFrames.PerParticipantDeviceTimestamp())

            assert_that(output, equal_to(expected, equals_fn=dataframes_equal))

    def test_group_annotations_multiple_keys(self):

        annotations = [
            build_annotation('device_1', '2020-01-01 12:00:00 UTC',
                             '2020-01-01 13:00:00 UTC'),
            build_annotation('device_1', '2020-01-01 13:00:00 UTC',
                             '2020-01-01 14:00:00 UTC'),
            build_annotation('device_2', '2020-01-01 13:00:00 UTC',
                             '2020-01-01 14:00:00 UTC'),
        ]

        group_1_dicts = [
            build_annotation_dict('device_1', '2020-01-01 12:00:00 UTC',
                                  '2020-01-01 13:00:00 UTC'),
            build_annotation_dict('device_1', '2020-01-01 13:00:00 UTC',
                                  '2020-01-01 14:00:00 UTC'),
        ]
        group_2_dicts = [
            build_annotation_dict('device_2', '2020-01-01 13:00:00 UTC',
                                  '2020-01-01 14:00:00 UTC'),
        ]
        expected = [pd.DataFrame(group_1_dicts), pd.DataFrame(group_2_dicts)]

        with TestPipeline() as p:
            output = (p | beam.Create(annotations) |
                      BuildAnnotationDataFrames.PerParticipantDevice())

            assert_that(output, equal_to(expected, equals_fn=dataframes_equal))

    def test_co_group_data_points_multiple_keys_concat(self):

        pressure_data_points = [
            build_pressure_data_point('device_1', '2020-01-01 12:00:00 UTC'),
            build_pressure_data_point('device_1', '2020-01-01 13:00:00 UTC'),
            build_pressure_data_point('device_2', '2020-01-01 13:00:00 UTC'),
        ]
        humidity_data_points = [
            build_humidity_data_point('device_1', '2020-01-01 12:00:00 UTC'),
            build_humidity_data_point('device_2', '2020-01-01 13:00:00 UTC'),
        ]

        group_1_pressure_dataframe = pd.DataFrame([
            build_pressure_data_point_dict('device_1',
                                           '2020-01-01 12:00:00 UTC'),
            build_pressure_data_point_dict('device_1',
                                           '2020-01-01 13:00:00 UTC'),
        ])
        group_1_humidity_dataframe = pd.DataFrame([
            build_humidity_data_point_dict('device_1',
                                           '2020-01-01 12:00:00 UTC'),
        ])
        group_1_combined = pd.concat(
            [group_1_pressure_dataframe, group_1_humidity_dataframe])

        group_2_pressure_dataframe = pd.DataFrame([
            build_pressure_data_point_dict('device_2',
                                           '2020-01-01 13:00:00 UTC'),
        ])
        group_2_humidity_dataframe = pd.DataFrame([
            build_humidity_data_point_dict('device_2',
                                           '2020-01-01 13:00:00 UTC'),
        ])
        group_2_combined = pd.concat(
            [group_2_pressure_dataframe, group_2_humidity_dataframe])
        expected = [group_1_combined, group_2_combined]

        with TestPipeline() as p:
            pressure_rows = p | 'Pressure' >> beam.Create(pressure_data_points)
            humidity_rows = p | 'Humidity' >> beam.Create(humidity_data_points)
            output = ({
                'pressure': pressure_rows,
                'humidity': humidity_rows
            } | BuildDataPointDataFrames.PerParticipantDevice(
                combine_method='concat'))

            assert_that(output, equal_to(expected, equals_fn=dataframes_equal))

    def test_co_group_data_points_multiple_keys_no_combine_method(self):

        pressure_data_points = [
            build_pressure_data_point('device_1', '2020-01-01 12:00:00 UTC'),
            build_pressure_data_point('device_1', '2020-01-01 13:00:00 UTC'),
            build_pressure_data_point('device_2', '2020-01-01 13:00:00 UTC'),
        ]
        humidity_data_points = [
            build_humidity_data_point('device_1', '2020-01-01 12:00:00 UTC'),
            build_humidity_data_point('device_2', '2020-01-01 13:00:00 UTC'),
        ]

        group_1_pressure_dataframe = pd.DataFrame([
            build_pressure_data_point_dict('device_1',
                                           '2020-01-01 12:00:00 UTC'),
            build_pressure_data_point_dict('device_1',
                                           '2020-01-01 13:00:00 UTC'),
        ])
        group_1_humidity_dataframe = pd.DataFrame([
            build_humidity_data_point_dict('device_1',
                                           '2020-01-01 12:00:00 UTC'),
        ])
        group_1_combined = {
            'pressure': group_1_pressure_dataframe,
            'humidity': group_1_humidity_dataframe
        }

        group_2_pressure_dataframe = pd.DataFrame([
            build_pressure_data_point_dict('device_2',
                                           '2020-01-01 13:00:00 UTC'),
        ])
        group_2_humidity_dataframe = pd.DataFrame([
            build_humidity_data_point_dict('device_2',
                                           '2020-01-01 13:00:00 UTC'),
        ])
        group_2_combined = {
            'pressure': group_2_pressure_dataframe,
            'humidity': group_2_humidity_dataframe
        }
        expected = [group_1_combined, group_2_combined]

        with TestPipeline() as p:
            pressure_rows = p | 'Pressure' >> beam.Create(pressure_data_points)
            humidity_rows = p | 'Humidity' >> beam.Create(humidity_data_points)
            output = ({
                'pressure': pressure_rows,
                'humidity': humidity_rows
            } | BuildDataPointDataFrames.PerParticipantDevice(
                combine_method=None))

            assert_that(output,
                        equal_to(expected, equals_fn=dataframe_dicts_equal))

    def test_co_group_annotations_multiple_keys_concat(self):

        annotations_1 = [
            build_annotation('device_1',
                             '2020-01-01 12:00:00 UTC',
                             '2020-01-01 13:00:00 UTC',
                             label='one'),
            build_annotation('device_1',
                             '2020-01-01 13:00:00 UTC',
                             '2020-01-01 14:00:00 UTC',
                             label='one'),
            build_annotation('device_2',
                             '2020-01-01 13:00:00 UTC',
                             '2020-01-01 14:00:00 UTC',
                             label='one'),
        ]

        annotations_2 = [
            build_annotation('device_1',
                             '2020-01-01 12:00:00 UTC',
                             '2020-01-01 13:00:00 UTC',
                             label='two'),
            build_annotation('device_2',
                             '2020-01-01 13:00:00 UTC',
                             '2020-01-01 14:00:00 UTC',
                             label='two'),
        ]

        group_1_ann1_dataframe = pd.DataFrame([
            build_annotation_dict('device_1',
                                  '2020-01-01 12:00:00 UTC',
                                  '2020-01-01 13:00:00 UTC',
                                  label='one'),
            build_annotation_dict('device_1',
                                  '2020-01-01 13:00:00 UTC',
                                  '2020-01-01 14:00:00 UTC',
                                  label='one'),
        ])
        group_1_ann2_dataframe = pd.DataFrame([
            build_annotation_dict('device_1',
                                  '2020-01-01 12:00:00 UTC',
                                  '2020-01-01 13:00:00 UTC',
                                  label='two'),
        ])
        group_1_combined = pd.concat(
            [group_1_ann1_dataframe, group_1_ann2_dataframe])

        group_2_ann1_dataframe = pd.DataFrame([
            build_annotation_dict('device_2',
                                  '2020-01-01 13:00:00 UTC',
                                  '2020-01-01 14:00:00 UTC',
                                  label='one'),
        ])
        group_2_ann2_dataframe = pd.DataFrame([
            build_annotation_dict('device_2',
                                  '2020-01-01 13:00:00 UTC',
                                  '2020-01-01 14:00:00 UTC',
                                  label='two'),
        ])
        group_2_combined = pd.concat(
            [group_2_ann1_dataframe, group_2_ann2_dataframe])

        expected = [group_1_combined, group_2_combined]

        with TestPipeline() as p:
            ann1_rows = p | 'Ann1' >> beam.Create(annotations_1)
            ann2_rows = p | 'Ann2' >> beam.Create(annotations_2)
            output = ({
                'ann1': ann1_rows,
                'ann2': ann2_rows
            } | BuildAnnotationDataFrames.PerParticipantDevice(
                combine_method='concat'))

            assert_that(output, equal_to(expected, equals_fn=dataframes_equal))

    def test_co_group_annotations_multiple_keys_no_combine_method(self):

        annotations_1 = [
            build_annotation('device_1',
                             '2020-01-01 12:00:00 UTC',
                             '2020-01-01 13:00:00 UTC',
                             label='one'),
            build_annotation('device_1',
                             '2020-01-01 13:00:00 UTC',
                             '2020-01-01 14:00:00 UTC',
                             label='one'),
            build_annotation('device_2',
                             '2020-01-01 13:00:00 UTC',
                             '2020-01-01 14:00:00 UTC',
                             label='one'),
        ]

        annotations_2 = [
            build_annotation('device_1',
                             '2020-01-01 12:00:00 UTC',
                             '2020-01-01 13:00:00 UTC',
                             label='two'),
            build_annotation('device_2',
                             '2020-01-01 13:00:00 UTC',
                             '2020-01-01 14:00:00 UTC',
                             label='two'),
        ]

        group_1_ann1_dataframe = pd.DataFrame([
            build_annotation_dict('device_1',
                                  '2020-01-01 12:00:00 UTC',
                                  '2020-01-01 13:00:00 UTC',
                                  label='one'),
            build_annotation_dict('device_1',
                                  '2020-01-01 13:00:00 UTC',
                                  '2020-01-01 14:00:00 UTC',
                                  label='one'),
        ])
        group_1_ann2_dataframe = pd.DataFrame([
            build_annotation_dict('device_1',
                                  '2020-01-01 12:00:00 UTC',
                                  '2020-01-01 13:00:00 UTC',
                                  label='two'),
        ])
        group_1_combined = {
            'ann1': group_1_ann1_dataframe,
            'ann2': group_1_ann2_dataframe
        }
        group_2_ann1_dataframe = pd.DataFrame([
            build_annotation_dict('device_2',
                                  '2020-01-01 13:00:00 UTC',
                                  '2020-01-01 14:00:00 UTC',
                                  label='one'),
        ])
        group_2_ann2_dataframe = pd.DataFrame([
            build_annotation_dict('device_2',
                                  '2020-01-01 13:00:00 UTC',
                                  '2020-01-01 14:00:00 UTC',
                                  label='two'),
        ])
        group_2_combined = {
            'ann1': group_2_ann1_dataframe,
            'ann2': group_2_ann2_dataframe
        }

        expected = [group_1_combined, group_2_combined]

        with TestPipeline() as p:
            ann1_rows = p | 'Ann1' >> beam.Create(annotations_1)
            ann2_rows = p | 'Ann2' >> beam.Create(annotations_2)
            output = ({
                'ann1': ann1_rows,
                'ann2': ann2_rows
            } | BuildAnnotationDataFrames.PerParticipantDevice())

            assert_that(output,
                        equal_to(expected, equals_fn=dataframe_dicts_equal))

    def test_co_group_data_points_multiple_keys_merge(self):

        pressure_data_points = [
            build_pressure_data_point('device_1', '2020-01-01 12:00:00 UTC'),
            build_pressure_data_point('device_1', '2020-01-01 13:00:00 UTC'),
            build_pressure_data_point('device_2', '2020-01-01 13:00:00 UTC'),
        ]
        humidity_data_points = [
            build_humidity_data_point('device_1', '2020-01-01 12:00:00 UTC'),
            build_humidity_data_point('device_2', '2020-01-01 13:00:00 UTC'),
        ]

        group_1_pressure_dataframe = pd.DataFrame([
            build_pressure_data_point_dict('device_1',
                                           '2020-01-01 12:00:00 UTC'),
            build_pressure_data_point_dict('device_1',
                                           '2020-01-01 13:00:00 UTC'),
        ])
        group_1_humidity_dataframe = pd.DataFrame([
            build_humidity_data_point_dict('device_1',
                                           '2020-01-01 12:00:00 UTC'),
        ])
        group_1_combined = group_1_pressure_dataframe.merge(
            group_1_humidity_dataframe,
            on='measurement_timestamp_utc',
            how='outer',
            suffixes=('_pressure', '_humidity'))

        group_2_pressure_dataframe = pd.DataFrame([
            build_pressure_data_point_dict('device_2',
                                           '2020-01-01 13:00:00 UTC'),
        ])
        group_2_humidity_dataframe = pd.DataFrame([
            build_humidity_data_point_dict('device_2',
                                           '2020-01-01 13:00:00 UTC'),
        ])
        group_2_combined = group_2_pressure_dataframe.merge(
            group_2_humidity_dataframe,
            on='measurement_timestamp_utc',
            how='outer',
            suffixes=('_pressure', '_humidity'))

        expected = [group_1_combined, group_2_combined]

        with TestPipeline() as p:
            pressure_rows = p | 'Pressure' >> beam.Create(pressure_data_points)
            humidity_rows = p | 'Humidity' >> beam.Create(humidity_data_points)
            output = ({
                'pressure': pressure_rows,
                'humidity': humidity_rows
            } | BuildDataPointDataFrames.PerParticipantDevice(
                combine_method='merge'))

            assert_that(output, equal_to(expected, equals_fn=dataframes_equal))

    def test_co_group_annotations_multiple_keys_merge(self):

        annotations_1 = [
            build_annotation('device_1',
                             '2020-01-01 12:00:00 UTC',
                             '2020-01-01 13:00:00 UTC',
                             label='one'),
            build_annotation('device_1',
                             '2020-01-01 13:00:00 UTC',
                             '2020-01-01 14:00:00 UTC',
                             label='one'),
            build_annotation('device_2',
                             '2020-01-01 13:00:00 UTC',
                             '2020-01-01 14:00:00 UTC',
                             label='one'),
        ]

        annotations_2 = [
            build_annotation('device_1',
                             '2020-01-01 12:00:00 UTC',
                             '2020-01-01 13:00:00 UTC',
                             label='two'),
            build_annotation('device_2',
                             '2020-01-01 13:00:00 UTC',
                             '2020-01-01 14:00:00 UTC',
                             label='two'),
        ]

        group_1_ann1_dataframe = pd.DataFrame([
            build_annotation_dict('device_1',
                                  '2020-01-01 12:00:00 UTC',
                                  '2020-01-01 13:00:00 UTC',
                                  label='one'),
            build_annotation_dict('device_1',
                                  '2020-01-01 13:00:00 UTC',
                                  '2020-01-01 14:00:00 UTC',
                                  label='one'),
        ])
        group_1_ann2_dataframe = pd.DataFrame([
            build_annotation_dict('device_1',
                                  '2020-01-01 12:00:00 UTC',
                                  '2020-01-01 13:00:00 UTC',
                                  label='two'),
        ])
        group_1_combined = group_1_ann1_dataframe.merge(
            group_1_ann2_dataframe,
            on=['start_timestamp_utc', 'end_timestamp_utc'],
            how='outer',
            suffixes=('_ann1', '_ann2'))

        group_2_ann1_dataframe = pd.DataFrame([
            build_annotation_dict('device_2',
                                  '2020-01-01 13:00:00 UTC',
                                  '2020-01-01 14:00:00 UTC',
                                  label='one'),
        ])
        group_2_ann2_dataframe = pd.DataFrame([
            build_annotation_dict('device_2',
                                  '2020-01-01 13:00:00 UTC',
                                  '2020-01-01 14:00:00 UTC',
                                  label='two'),
        ])
        group_2_combined = group_2_ann1_dataframe.merge(
            group_2_ann2_dataframe,
            on=['start_timestamp_utc', 'end_timestamp_utc'],
            how='outer',
            suffixes=('_ann1', '_ann2'))

        expected = [group_1_combined, group_2_combined]

        with TestPipeline() as p:
            ann1_rows = p | 'Ann1' >> beam.Create(annotations_1)
            ann2_rows = p | 'Ann2' >> beam.Create(annotations_2)
            output = ({
                'ann1': ann1_rows,
                'ann2': ann2_rows
            } | BuildAnnotationDataFrames.PerParticipantDevice(
                combine_method='merge'))

            assert_that(output, equal_to(expected, equals_fn=dataframes_equal))

    def test_co_group_data_points_multiple_keys_merge_with_window(self):

        pressure_data_points = [
            build_pressure_data_point('device_1', '2020-01-01 12:00:00 UTC'),
            build_pressure_data_point('device_1', '2020-01-01 13:00:00 UTC'),
            build_pressure_data_point('device_2', '2020-01-01 13:00:00 UTC'),
        ]
        humidity_data_points = [
            build_humidity_data_point('device_1', '2020-01-01 12:00:00 UTC'),
            build_humidity_data_point('device_2', '2020-01-01 13:00:00 UTC'),
        ]

        group_1_window_1_pressure_dataframe = pd.DataFrame([
            build_pressure_data_point_dict('device_1',
                                           '2020-01-01 12:00:00 UTC'),
        ])
        group_1_window_1_humidity_dataframe = pd.DataFrame([
            build_humidity_data_point_dict('device_1',
                                           '2020-01-01 12:00:00 UTC'),
        ])
        group_1_window_1_combined = group_1_window_1_pressure_dataframe.merge(
            group_1_window_1_humidity_dataframe,
            on='measurement_timestamp_utc',
            how='outer',
            suffixes=('_pressure', '_humidity'))
        group_1_window_2_combined = pd.DataFrame([
            build_pressure_data_point_dict('device_1',
                                           '2020-01-01 13:00:00 UTC'),
        ])

        group_2_window_1_pressure_dataframe = pd.DataFrame([
            build_pressure_data_point_dict('device_2',
                                           '2020-01-01 13:00:00 UTC'),
        ])
        group_2_window_1_humidity_dataframe = pd.DataFrame([
            build_humidity_data_point_dict('device_2',
                                           '2020-01-01 13:00:00 UTC'),
        ])
        group_2_window_1_combined = group_2_window_1_pressure_dataframe.merge(
            group_2_window_1_humidity_dataframe,
            on='measurement_timestamp_utc',
            how='outer',
            suffixes=('_pressure', '_humidity'))

        expected = [
            group_1_window_1_combined, group_1_window_2_combined,
            group_2_window_1_combined
        ]

        with TestPipeline() as p:
            pressure_rows = p | 'Pressure' >> beam.Create(pressure_data_points)
            humidity_rows = p | 'Humidity' >> beam.Create(humidity_data_points)
            output = ({
                'pressure': pressure_rows,
                'humidity': humidity_rows
            } | BuildDataPointDataFrames.PerParticipantDeviceWindow(
                beam_window_fn=beam.window.FixedWindows(60 * 60),
                combine_method='merge'))

            assert_that(output, equal_to(expected, equals_fn=dataframes_equal))

    def test_co_group_annotations_multiple_keys_merge_with_window(self):

        annotations_1 = [
            build_annotation('device_1',
                             '2020-01-01 12:00:00 UTC',
                             '2020-01-01 13:00:00 UTC',
                             label='one'),
            build_annotation('device_1',
                             '2020-01-01 13:00:00 UTC',
                             '2020-01-01 14:00:00 UTC',
                             label='one'),
            build_annotation('device_2',
                             '2020-01-01 13:00:00 UTC',
                             '2020-01-01 14:00:00 UTC',
                             label='one'),
        ]

        annotations_2 = [
            build_annotation('device_1',
                             '2020-01-01 12:00:00 UTC',
                             '2020-01-01 13:00:00 UTC',
                             label='two'),
            build_annotation('device_2',
                             '2020-01-01 13:00:00 UTC',
                             '2020-01-01 14:00:00 UTC',
                             label='two'),
        ]

        group_1_window_1_ann1_dataframe = pd.DataFrame([
            build_annotation_dict('device_1',
                                  '2020-01-01 12:00:00 UTC',
                                  '2020-01-01 13:00:00 UTC',
                                  label='one'),
        ])
        group_1_window_1_ann2_dataframe = pd.DataFrame([
            build_annotation_dict('device_1',
                                  '2020-01-01 12:00:00 UTC',
                                  '2020-01-01 13:00:00 UTC',
                                  label='two'),
        ])
        group_1_window_1_combined = group_1_window_1_ann1_dataframe.merge(
            group_1_window_1_ann2_dataframe,
            on=['start_timestamp_utc', 'end_timestamp_utc'],
            how='outer',
            suffixes=('_ann1', '_ann2'))
        group_1_window_2_combined = pd.DataFrame([
            build_annotation_dict('device_1',
                                  '2020-01-01 13:00:00 UTC',
                                  '2020-01-01 14:00:00 UTC',
                                  label='one'),
        ])

        group_2_window_1_ann1_dataframe = pd.DataFrame([
            build_annotation_dict('device_2',
                                  '2020-01-01 13:00:00 UTC',
                                  '2020-01-01 14:00:00 UTC',
                                  label='one'),
        ])
        group_2_window_1_ann2_dataframe = pd.DataFrame([
            build_annotation_dict('device_2',
                                  '2020-01-01 13:00:00 UTC',
                                  '2020-01-01 14:00:00 UTC',
                                  label='two'),
        ])
        group_2_window_1_combined = group_2_window_1_ann1_dataframe.merge(
            group_2_window_1_ann2_dataframe,
            on=['start_timestamp_utc', 'end_timestamp_utc'],
            how='outer',
            suffixes=('_ann1', '_ann2'))

        expected = [
            group_1_window_1_combined, group_1_window_2_combined,
            group_2_window_1_combined
        ]

        with TestPipeline() as p:
            ann1_rows = p | 'Ann1' >> beam.Create(annotations_1)
            ann2_rows = p | 'Ann2' >> beam.Create(annotations_2)
            output = ({
                'ann1': ann1_rows,
                'ann2': ann2_rows
            } | BuildAnnotationDataFrames.PerParticipantDeviceWindow(
                beam_window_fn=beam.window.FixedWindows(60 * 60),
                window_by_start_time=True,
                combine_method='merge'))

            assert_that(output, equal_to(expected, equals_fn=dataframes_equal))

    def test_group_data_points_local_time_zone(self):
        data_source_id1 = 12345
        data_source_id2 = 54321
        data_source_cache = DataSourceCache({
            data_source_id1:
                types_pb2.DataSource(device=types_pb2.Device(
                    time_zone_name='US/Eastern')),
            data_source_id2:
                types_pb2.DataSource(device=types_pb2.Device(
                    time_zone_name='US/Pacific'))
        })
        data_points = [
            # Falls on 2019-12-31 in eastern timezone.
            build_pressure_data_point(device_id='123',
                                      timestamp_str='2020-01-01 04:00:00',
                                      data_source_id=data_source_id1),
            # Falls on 2019-12-31 in pacific timezone.
            build_pressure_data_point(device_id='123',
                                      timestamp_str='2020-01-01 07:00:00',
                                      data_source_id=data_source_id2),
            # Falls on 2020-01-01 in eastern timezone.
            build_pressure_data_point(device_id='123',
                                      timestamp_str='2020-01-01 05:00:00',
                                      data_source_id=data_source_id1),
            # Falls on 2020-01-01 in pacific timezone.
            build_pressure_data_point(device_id='123',
                                      timestamp_str='2020-01-01 08:00:00',
                                      data_source_id=data_source_id1),
            # Falls on 2020-01-02 in eastern timezone.
            build_pressure_data_point(device_id='123',
                                      timestamp_str='2020-01-02 06:00:00',
                                      data_source_id=data_source_id1),
            # Falls on 2020-01-02 in pacific timezone.
            build_pressure_data_point(device_id='123',
                                      timestamp_str='2020-01-02 09:00:00',
                                      data_source_id=data_source_id2),
            # Falls on 2020-01-02 in eastern timezone.
            build_pressure_data_point(device_id='123',
                                      timestamp_str='2020-01-02 07:00:00',
                                      data_source_id=data_source_id1),
            # Falls on 2020-01-02 in pacific timezone.
            build_pressure_data_point(device_id='123',
                                      timestamp_str='2020-01-02 10:00:00',
                                      data_source_id=data_source_id2),
            # Falls on 2020-01-02 in eastern timezone.
            build_pressure_data_point(device_id='123',
                                      timestamp_str='2020-01-03 00:00:00',
                                      data_source_id=data_source_id1),
            # Falls on 2020-01-02 in pacific timezone.
            build_pressure_data_point(device_id='123',
                                      timestamp_str='2020-01-03 03:00:00',
                                      data_source_id=data_source_id2),
            # Different Device ID
            build_pressure_data_point(device_id='321',
                                      timestamp_str='2020-01-03 03:00:00',
                                      data_source_id=data_source_id2),
        ]

        # 2019-12-31 in local time
        group_1_dicts = [
            build_pressure_data_point_dict(device_id='123',
                                           timestamp_str='2020-01-01 04:00:00',
                                           data_source_id=data_source_id1),
            build_pressure_data_point_dict(device_id='123',
                                           timestamp_str='2020-01-01 07:00:00',
                                           data_source_id=data_source_id2)
        ]
        # 2020-01-01 in local time
        group_2_dicts = [
            build_pressure_data_point_dict(device_id='123',
                                           timestamp_str='2020-01-01 05:00:00',
                                           data_source_id=data_source_id1),
            build_pressure_data_point_dict(device_id='123',
                                           timestamp_str='2020-01-01 08:00:00',
                                           data_source_id=data_source_id1)
        ]
        # 2020-01-02 in local time
        group_3_dicts = [
            build_pressure_data_point_dict(device_id='123',
                                           timestamp_str='2020-01-02 06:00:00',
                                           data_source_id=data_source_id1),
            build_pressure_data_point_dict(device_id='123',
                                           timestamp_str='2020-01-02 09:00:00',
                                           data_source_id=data_source_id2),
            build_pressure_data_point_dict(device_id='123',
                                           timestamp_str='2020-01-02 07:00:00',
                                           data_source_id=data_source_id1),
            build_pressure_data_point_dict(device_id='123',
                                           timestamp_str='2020-01-02 10:00:00',
                                           data_source_id=data_source_id2),
            build_pressure_data_point_dict(device_id='123',
                                           timestamp_str='2020-01-03 00:00:00',
                                           data_source_id=data_source_id1),
            build_pressure_data_point_dict(device_id='123',
                                           timestamp_str='2020-01-03 03:00:00',
                                           data_source_id=data_source_id2),
        ]
        # Different device
        group_4_dicts = [
            build_pressure_data_point_dict(device_id='321',
                                           timestamp_str='2020-01-03 03:00:00',
                                           data_source_id=data_source_id2),
        ]

        expected = [
            pd.DataFrame(group_1_dicts),
            pd.DataFrame(group_2_dicts),
            pd.DataFrame(group_3_dicts),
            pd.DataFrame(group_4_dicts)
        ]

        with TestPipeline() as p:
            output = (p | beam.Create(data_points) | BuildDataPointDataFrames.
                      PerParticipantDeviceWindowLocalTimezone(
                          beam_window_fn=beam.window.FixedWindows(60 * 60 * 24),
                          data_source_cache=data_source_cache))

            assert_that(output, equal_to(expected, equals_fn=dataframes_equal))

    def test_group_data_points_local_time_zone_utc_offset_map(self):
        data_source_id1 = 12345
        data_source_id2 = 54321

        utc_offset_map = rialto_timezone_fix.UtcOffsetMap()
        # Regardless of each DataPoint's local timezone, all data for device 123
        # will be in the Pacific timezone, and 321 will all be in Eastern.
        utc_offset_map.add_utc_offset('123', 'US/Pacific')
        utc_offset_map.add_utc_offset('321', 'US/Eastern')

        data_points = [
            # Falls on 2019-12-31 in eastern timezone.
            build_pressure_data_point(device_id='123',
                                      timestamp_str='2020-01-01 04:00:00',
                                      data_source_id=data_source_id1),
            # Falls on 2020-01-01 in eastern timezone.
            build_pressure_data_point(device_id='123',
                                      timestamp_str='2020-01-01 05:00:00',
                                      data_source_id=data_source_id1),
            # Falls on 2020-01-01 in pacific timezone.
            build_pressure_data_point(device_id='123',
                                      timestamp_str='2020-01-01 08:00:00',
                                      data_source_id=data_source_id1),
            # Falls on 2020-01-02 in eastern timezone.
            build_pressure_data_point(device_id='123',
                                      timestamp_str='2020-01-02 06:00:00',
                                      data_source_id=data_source_id1),
            # Falls on 2020-01-02 in pacific timezone.
            build_pressure_data_point(device_id='123',
                                      timestamp_str='2020-01-02 09:00:00',
                                      data_source_id=data_source_id2),
            # Falls on 2020-01-02 in pacific timezone.
            build_pressure_data_point(device_id='123',
                                      timestamp_str='2020-01-02 10:00:00',
                                      data_source_id=data_source_id2),
            # Falls on 2020-01-02 in eastern timezone.
            build_pressure_data_point(device_id='123',
                                      timestamp_str='2020-01-03 00:00:00',
                                      data_source_id=data_source_id1),
            # Falls on 2020-01-02 in pacific timezone.
            build_pressure_data_point(device_id='123',
                                      timestamp_str='2020-01-03 03:00:00',
                                      data_source_id=data_source_id2),
            # Different Device ID
            build_pressure_data_point(device_id='321',
                                      timestamp_str='2020-01-03 03:00:00',
                                      data_source_id=data_source_id2),
        ]

        # 2019-12-31 in Pacific time
        group_1_dicts = [
            build_pressure_data_point_dict(device_id='123',
                                           timestamp_str='2020-01-01 04:00:00',
                                           data_source_id=data_source_id1),
            build_pressure_data_point_dict(device_id='123',
                                           timestamp_str='2020-01-01 05:00:00',
                                           data_source_id=data_source_id1)
        ]
        # 2020-01-01 in Pacific time
        group_2_dicts = [
            build_pressure_data_point_dict(device_id='123',
                                           timestamp_str='2020-01-01 08:00:00',
                                           data_source_id=data_source_id1),
            build_pressure_data_point_dict(device_id='123',
                                           timestamp_str='2020-01-02 06:00:00',
                                           data_source_id=data_source_id1)
        ]
        # 2020-01-02 in Pacific time
        group_3_dicts = [
            build_pressure_data_point_dict(device_id='123',
                                           timestamp_str='2020-01-02 09:00:00',
                                           data_source_id=data_source_id2),
            build_pressure_data_point_dict(device_id='123',
                                           timestamp_str='2020-01-02 10:00:00',
                                           data_source_id=data_source_id2),
            build_pressure_data_point_dict(device_id='123',
                                           timestamp_str='2020-01-03 00:00:00',
                                           data_source_id=data_source_id1),
            build_pressure_data_point_dict(device_id='123',
                                           timestamp_str='2020-01-03 03:00:00',
                                           data_source_id=data_source_id2)
        ]
        # Different device (Eastern Time)
        group_4_dicts = [
            build_pressure_data_point_dict(device_id='321',
                                           timestamp_str='2020-01-03 03:00:00',
                                           data_source_id=data_source_id2),
        ]

        expected = [
            pd.DataFrame(group_1_dicts),
            pd.DataFrame(group_2_dicts),
            pd.DataFrame(group_3_dicts),
            pd.DataFrame(group_4_dicts)
        ]

        with TestPipeline() as p:
            output = (p | beam.Create(data_points) | BuildDataPointDataFrames.
                      PerParticipantDeviceWindowLocalTimezone(
                          beam_window_fn=beam.window.FixedWindows(60 * 60 * 24),
                          utc_offset_map=utc_offset_map))

            assert_that(output, equal_to(expected, equals_fn=dataframes_equal))

    def test_group_data_points_in_timezone(self):
        data_source_id1 = 12345
        data_source_id2 = 54321
        timezone = 'US/Eastern'

        data_points = [
            # Falls on 2019-12-31 in eastern timezone.
            build_pressure_data_point(device_id='123',
                                      timestamp_str='2020-01-01 04:00:00',
                                      data_source_id=data_source_id1),
            # Falls on 2020-01-01 in eastern timezone.
            build_pressure_data_point(device_id='123',
                                      timestamp_str='2020-01-01 05:00:00',
                                      data_source_id=data_source_id1),
            # Falls on 2020-01-01 in pacific timezone.
            build_pressure_data_point(device_id='123',
                                      timestamp_str='2020-01-01 08:00:00',
                                      data_source_id=data_source_id1),
            # Falls on 2020-01-02 in eastern timezone.
            build_pressure_data_point(device_id='123',
                                      timestamp_str='2020-01-02 06:00:00',
                                      data_source_id=data_source_id1),
            # Falls on 2020-01-02 in pacific timezone.
            build_pressure_data_point(device_id='123',
                                      timestamp_str='2020-01-02 09:00:00',
                                      data_source_id=data_source_id2),
            # Falls on 2020-01-02 in pacific timezone.
            build_pressure_data_point(device_id='123',
                                      timestamp_str='2020-01-02 10:00:00',
                                      data_source_id=data_source_id2),
            # Falls on 2020-01-02 in eastern timezone.
            build_pressure_data_point(device_id='123',
                                      timestamp_str='2020-01-03 00:00:00',
                                      data_source_id=data_source_id1),
            # Falls on 2020-01-02 in pacific timezone.
            build_pressure_data_point(device_id='123',
                                      timestamp_str='2020-01-03 03:00:00',
                                      data_source_id=data_source_id2),
            # Different Device ID
            build_pressure_data_point(device_id='321',
                                      timestamp_str='2020-01-03 03:00:00',
                                      data_source_id=data_source_id2),
        ]

        # 2019-12-31 in Eastern time
        group_1_dicts = [
            build_pressure_data_point_dict(device_id='123',
                                           timestamp_str='2020-01-01 04:00:00',
                                           data_source_id=data_source_id1)
        ]
        # 2020-01-01 in Eastern time
        group_2_dicts = [
            build_pressure_data_point_dict(device_id='123',
                                           timestamp_str='2020-01-01 05:00:00',
                                           data_source_id=data_source_id1),
            build_pressure_data_point_dict(device_id='123',
                                           timestamp_str='2020-01-01 08:00:00',
                                           data_source_id=data_source_id1)
        ]
        # 2020-01-02 in Eastern time
        group_3_dicts = [
            build_pressure_data_point_dict(device_id='123',
                                           timestamp_str='2020-01-02 06:00:00',
                                           data_source_id=data_source_id1),
            build_pressure_data_point_dict(device_id='123',
                                           timestamp_str='2020-01-02 09:00:00',
                                           data_source_id=data_source_id2),
            build_pressure_data_point_dict(device_id='123',
                                           timestamp_str='2020-01-02 10:00:00',
                                           data_source_id=data_source_id2),
            build_pressure_data_point_dict(device_id='123',
                                           timestamp_str='2020-01-03 00:00:00',
                                           data_source_id=data_source_id1),
            build_pressure_data_point_dict(device_id='123',
                                           timestamp_str='2020-01-03 03:00:00',
                                           data_source_id=data_source_id2)
        ]
        # Different device (2020-01-02 in Eastern Time)
        group_4_dicts = [
            build_pressure_data_point_dict(device_id='321',
                                           timestamp_str='2020-01-03 03:00:00',
                                           data_source_id=data_source_id2),
        ]

        expected = [
            pd.DataFrame(group_1_dicts),
            pd.DataFrame(group_2_dicts),
            pd.DataFrame(group_3_dicts),
            pd.DataFrame(group_4_dicts)
        ]

        with TestPipeline() as p:
            output = (
                p | beam.Create(data_points) |
                BuildDataPointDataFrames.PerParticipantDeviceWindowInTimezone(
                    beam_window_fn=beam.window.FixedWindows(60 * 60 * 24),
                    timezone=timezone))

            assert_that(output, equal_to(expected, equals_fn=dataframes_equal))

    def test_co_group_data_points_local_time_zone(self):
        data_source_id1 = 12345
        data_source_id2 = 54321
        data_source_cache = DataSourceCache({
            data_source_id1:
                types_pb2.DataSource(device=types_pb2.Device(
                    time_zone_name='US/Eastern')),
            data_source_id2:
                types_pb2.DataSource(device=types_pb2.Device(
                    time_zone_name='US/Pacific'))
        })

        pressure_data_points = [
            # Falls on 2019-12-31 in eastern timezone.
            build_pressure_data_point(device_id='123',
                                      timestamp_str='2020-01-01 04:00:00',
                                      data_source_id=data_source_id1),
            # Falls on 2020-01-01 in eastern timezone.
            build_pressure_data_point(device_id='123',
                                      timestamp_str='2020-01-01 05:00:00',
                                      data_source_id=data_source_id1),
        ]

        humidity_data_points = [
            # Falls on 2019-12-31 in eastern timezone.
            build_humidity_data_point(device_id='123',
                                      timestamp_str='2020-01-01 04:00:00',
                                      data_source_id=data_source_id1),
            # Falls on 2020-01-01 in eastern timezone.
            build_humidity_data_point(device_id='123',
                                      timestamp_str='2020-01-01 05:00:00',
                                      data_source_id=data_source_id1),
        ]

        group1 = {
            'pressure':
                pd.DataFrame([
                    build_pressure_data_point_dict(
                        device_id='123',
                        timestamp_str='2020-01-01 04:00:00',
                        data_source_id=data_source_id1)
                ]),
            'humidity':
                pd.DataFrame([
                    build_humidity_data_point_dict(
                        device_id='123',
                        timestamp_str='2020-01-01 04:00:00',
                        data_source_id=data_source_id1)
                ])
        }
        group2 = {
            'pressure':
                pd.DataFrame([
                    build_pressure_data_point_dict(
                        device_id='123',
                        timestamp_str='2020-01-01 05:00:00',
                        data_source_id=data_source_id1)
                ]),
            'humidity':
                pd.DataFrame([
                    build_humidity_data_point_dict(
                        device_id='123',
                        timestamp_str='2020-01-01 05:00:00',
                        data_source_id=data_source_id1),
                ])
        }

        with TestPipeline() as p:
            pressure_rows = p | 'Pressure' >> beam.Create(pressure_data_points)
            humidity_rows = p | 'Humidity' >> beam.Create(humidity_data_points)
            output = ({
                'pressure': pressure_rows,
                'humidity': humidity_rows
            } | BuildDataPointDataFrames.
                      PerParticipantDeviceWindowLocalTimezone(
                          beam_window_fn=beam.window.FixedWindows(60 * 60 * 24),
                          data_source_cache=data_source_cache))

            assert_that(
                output,
                equal_to([group1, group2], equals_fn=dataframe_dicts_equal))


if __name__ == '__main__':
    unittest.main()
