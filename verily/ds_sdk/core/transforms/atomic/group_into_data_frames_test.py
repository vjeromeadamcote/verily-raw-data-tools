"""Tests for group_into_data_frames."""

from typing import Any, Dict
import unittest

import apache_beam as beam
from apache_beam.testing.test_pipeline import TestPipeline
from apache_beam.testing.util import assert_that
from apache_beam.testing.util import equal_to
from apache_beam.utils.timestamp import Timestamp
import frozendict
import pandas as pd

from verily.ds_sdk.core import schemas
from verily.ds_sdk.core.transforms import group_into_data_frames
from verily.ds_sdk.core.transforms.atomic import key_by


def dataframes_metadata_equal(left: Dict[Any, Any], right: pd.DataFrame):
    return left == right.attrs


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


def dataframe_metadata_dicts_equal(left: Dict[str, pd.DataFrame],
                                   right: Dict[str, pd.DataFrame]):
    if left.keys() != right.keys():
        return False
    for key in left.keys():
        if not left[key].attrs == right[key].attrs:
            return False
    return True


def build_pandas_timestamp(timestamp_str: str) -> pd.Timestamp:
    return pd.Timestamp(timestamp_str, tz='UTC')


def build_beam_timestamp(timestamp_str: str) -> Timestamp:
    return Timestamp.from_utc_datetime(build_pandas_timestamp(timestamp_str))


def build_annotation(start_timestamp_str: str,
                     end_timestamp_str: str,
                     label='annotation_label') -> schemas.Annotation:

    annotation_metadata = schemas.AnnotationMetadata(device_id='C2Q123',
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


def build_annotation_dict(start_timestamp_str: str,
                          end_timestamp_str: str,
                          label='annotation_label') -> Dict[Any, Any]:
    return {
        'annotation_label': label,
        'start_timestamp_utc': build_pandas_timestamp(start_timestamp_str),
        'end_timestamp_utc': build_pandas_timestamp(end_timestamp_str),
        'annotation_metadata': {
            'device_id': 'C2Q123',
            'participant_id': '123',
            'participant_namespace': 1,
            'version_name': None,
            'version_number': None,
            'input_data_info': [],
        },
    }


def build_data_point_metadata():
    return schemas.data_point_metadata_for_raw_data(data_source_id=12345,
                                                    device_id='C2Q123',
                                                    participant_id='123',
                                                    participant_namespace=1,
                                                    echo_metadata=None,
                                                    sensor_store_metadata=None,
                                                    annotation_labels=set())


def build_pressure_data_point(timestamp_str: str) -> schemas.Pressure:

    return schemas.Pressure(
        data_point_metadata=build_data_point_metadata(),
        measurement_timestamp_utc=build_beam_timestamp(timestamp_str),
        pressure=1)


def build_pressure_data_point_dict(timestamp_str: str) -> Dict[Any, Any]:
    return {
        'data_point_metadata': {
            'data_source_id': 12345,
            'device_id': 'C2Q123',
            'participant_id': '123',
            'participant_namespace': 1,
            'echo_metadata': None,
            'sensor_store_metadata': None,
            'annotation_labels': set(),
        },
        'measurement_timestamp_utc': build_pandas_timestamp(timestamp_str),
        'pressure': 1,
    }


def build_humidity_data_point(timestamp_str: str) -> schemas.Humidity:

    return schemas.Humidity(
        data_point_metadata=build_data_point_metadata(),
        measurement_timestamp_utc=build_beam_timestamp(timestamp_str),
        humidity=1)


def build_humidity_data_point_dict(timestamp_str: str) -> Dict[Any, Any]:
    return {
        'data_point_metadata': {
            'data_source_id': 12345,
            'device_id': 'C2Q123',
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

    def test_group_data_points_single_key(self):

        keyed_data_points = [
            (key_by.Key(device_id='1',
                        participant_id='1',
                        participant_namespace=1,
                        additional_keys=frozendict.FrozenOrderedDict({})),
             build_pressure_data_point('2020-01-01 12:00:00 UTC')),
            (key_by.Key(device_id='1',
                        participant_id='1',
                        participant_namespace=1,
                        additional_keys=frozendict.FrozenOrderedDict({})),
             build_pressure_data_point('2020-01-01 13:00:00 UTC')),
        ]

        data_point_dicts = [
            build_pressure_data_point_dict('2020-01-01 12:00:00 UTC'),
            build_pressure_data_point_dict('2020-01-01 13:00:00 UTC'),
        ]
        expected = [pd.DataFrame(data_point_dicts)]

        with TestPipeline() as p:
            output = (p | beam.Create(keyed_data_points) |
                      group_into_data_frames.GroupIntoDataFrames())

            assert_that(output, equal_to(expected, equals_fn=dataframes_equal))

    def test_group_data_points_multiple_keys(self):

        keyed_data_points = [
            (key_by.Key(device_id='1',
                        participant_id='1',
                        participant_namespace=1,
                        additional_keys=frozendict.FrozenOrderedDict({})),
             build_pressure_data_point('2020-01-01 12:00:00 UTC')),
            (key_by.Key(device_id='1',
                        participant_id='1',
                        participant_namespace=1,
                        additional_keys=frozendict.FrozenOrderedDict({})),
             build_pressure_data_point('2020-01-01 13:00:00 UTC')),
            (key_by.Key(device_id='2',
                        participant_id='1',
                        participant_namespace=1,
                        additional_keys=frozendict.FrozenOrderedDict({})),
             build_pressure_data_point('2020-01-01 13:00:00 UTC')),
        ]

        group_1_dicts = [
            build_pressure_data_point_dict('2020-01-01 12:00:00 UTC'),
            build_pressure_data_point_dict('2020-01-01 13:00:00 UTC'),
        ]
        group_2_dicts = [
            build_pressure_data_point_dict('2020-01-01 13:00:00 UTC'),
        ]
        expected = [pd.DataFrame(group_1_dicts), pd.DataFrame(group_2_dicts)]

        with TestPipeline() as p:
            output = (p | beam.Create(keyed_data_points) |
                      group_into_data_frames.GroupIntoDataFrames())

            assert_that(output, equal_to(expected, equals_fn=dataframes_equal))

    def test_group_annotations_single_key(self):

        keyed_annotations = [
            (key_by.Key(device_id='1',
                        participant_id='1',
                        participant_namespace=1,
                        additional_keys=frozendict.FrozenOrderedDict({})),
             build_annotation('2020-01-01 12:00:00 UTC',
                              '2020-01-01 13:00:00 UTC')),
            (key_by.Key(device_id='1',
                        participant_id='1',
                        participant_namespace=1,
                        additional_keys=frozendict.FrozenOrderedDict({})),
             build_annotation('2020-01-01 13:00:00 UTC',
                              '2020-01-01 14:00:00 UTC')),
        ]

        annotation_dicts = [
            build_annotation_dict('2020-01-01 12:00:00 UTC',
                                  '2020-01-01 13:00:00 UTC'),
            build_annotation_dict('2020-01-01 13:00:00 UTC',
                                  '2020-01-01 14:00:00 UTC'),
        ]
        expected = [pd.DataFrame(annotation_dicts)]

        with TestPipeline() as p:
            output = (p | beam.Create(keyed_annotations) |
                      group_into_data_frames.GroupIntoDataFrames())

            assert_that(output, equal_to(expected, equals_fn=dataframes_equal))

    def test_group_annotations_multiple_keys(self):

        keyed_annotations = [
            (key_by.Key(device_id='1',
                        participant_id='1',
                        participant_namespace=1,
                        additional_keys=frozendict.FrozenOrderedDict({})),
             build_annotation('2020-01-01 12:00:00 UTC',
                              '2020-01-01 13:00:00 UTC')),
            (key_by.Key(device_id='1',
                        participant_id='1',
                        participant_namespace=1,
                        additional_keys=frozendict.FrozenOrderedDict({})),
             build_annotation('2020-01-01 13:00:00 UTC',
                              '2020-01-01 14:00:00 UTC')),
            (key_by.Key(device_id='2',
                        participant_id='1',
                        participant_namespace=1,
                        additional_keys=frozendict.FrozenOrderedDict({})),
             build_annotation('2020-01-01 13:00:00 UTC',
                              '2020-01-01 14:00:00 UTC')),
        ]

        group_1_dicts = [
            build_annotation_dict('2020-01-01 12:00:00 UTC',
                                  '2020-01-01 13:00:00 UTC'),
            build_annotation_dict('2020-01-01 13:00:00 UTC',
                                  '2020-01-01 14:00:00 UTC'),
        ]
        group_2_dicts = [
            build_annotation_dict('2020-01-01 13:00:00 UTC',
                                  '2020-01-01 14:00:00 UTC'),
        ]
        expected = [pd.DataFrame(group_1_dicts), pd.DataFrame(group_2_dicts)]

        with TestPipeline() as p:
            output = (p | beam.Create(keyed_annotations) |
                      group_into_data_frames.GroupIntoDataFrames())

            assert_that(output, equal_to(expected, equals_fn=dataframes_equal))

    def test_group_data_points_metadata_attached(self):

        keyed_data_points = [
            (key_by.Key(device_id='123',
                        participant_id='321',
                        participant_namespace=1,
                        additional_keys=frozendict.FrozenOrderedDict({'a': 1})),
             build_pressure_data_point('2020-01-01 12:00:00 UTC')),
        ]

        expected_metadata = [
            {
                'data_point_metadata': build_data_point_metadata(),
                'device_id': '123',
                'participant_id': '321',
                'participant_namespace': 1,
                'additional_keys': {
                    'a': 1
                }
            },
        ]

        with TestPipeline() as p:
            output = (p | beam.Create(keyed_data_points) |
                      group_into_data_frames.GroupIntoDataFrames())

            assert_that(
                output,
                equal_to(expected_metadata,
                         equals_fn=dataframes_metadata_equal))

    def test_group_data_points_time_window_metadata_attached(self):

        data_points = [
            build_pressure_data_point('2020-01-01 12:00:00 UTC'),
            build_pressure_data_point('2020-01-01 13:00:00 UTC'),
            build_pressure_data_point('2020-01-01 14:00:00 UTC'),
        ]

        expected_metadata = [
            {
                'data_point_metadata': build_data_point_metadata(),
                'device_id': 'C2Q123',
                'participant_id': '123',
                'participant_namespace': 1,
                'additional_keys': {
                    'start_time_range_micros':
                        build_beam_timestamp('2020-01-01 12:00:00 UTC').micros,
                    'end_time_range_micros':
                        build_beam_timestamp('2020-01-01 13:00:00 UTC').micros,
                },
            },
            {
                'data_point_metadata': build_data_point_metadata(),
                'device_id': 'C2Q123',
                'participant_id': '123',
                'participant_namespace': 1,
                'additional_keys': {
                    'start_time_range_micros':
                        build_beam_timestamp('2020-01-01 13:00:00 UTC').micros,
                    'end_time_range_micros':
                        build_beam_timestamp('2020-01-01 14:00:00 UTC').micros,
                },
            },
            {
                'data_point_metadata': build_data_point_metadata(),
                'device_id': 'C2Q123',
                'participant_id': '123',
                'participant_namespace': 1,
                'additional_keys': {
                    'start_time_range_micros':
                        build_beam_timestamp('2020-01-01 14:00:00 UTC').micros,
                    'end_time_range_micros':
                        build_beam_timestamp('2020-01-01 15:00:00 UTC').micros
                },
            },
        ]

        with TestPipeline() as p:
            output = (p | beam.Create(data_points) |
                      key_by.KeyDataPointsByParticipantDeviceTimeRange(
                          beam_window_fn=beam.window.FixedWindows(60 * 60)) |
                      group_into_data_frames.GroupIntoDataFrames())

            assert_that(
                output,
                equal_to(expected_metadata,
                         equals_fn=dataframes_metadata_equal))


class CoGroupIntoDataFramesTest(unittest.TestCase):

    def test_co_group_data_points_single_key_concat(self):

        pressure_data_points = [
            (key_by.Key(device_id='1',
                        participant_id='1',
                        participant_namespace=1,
                        additional_keys=frozendict.FrozenOrderedDict({})),
             build_pressure_data_point('2020-01-01 12:00:00 UTC')),
            (key_by.Key(device_id='1',
                        participant_id='1',
                        participant_namespace=1,
                        additional_keys=frozendict.FrozenOrderedDict({})),
             build_pressure_data_point('2020-01-01 13:00:00 UTC')),
        ]
        humidity_data_points = [
            (key_by.Key(device_id='1',
                        participant_id='1',
                        participant_namespace=1,
                        additional_keys=frozendict.FrozenOrderedDict({})),
             build_humidity_data_point('2020-01-01 12:00:00 UTC')),
            (key_by.Key(device_id='1',
                        participant_id='1',
                        participant_namespace=1,
                        additional_keys=frozendict.FrozenOrderedDict({})),
             build_humidity_data_point('2020-01-01 13:00:00 UTC')),
        ]

        pressure_dataframe = pd.DataFrame([
            build_pressure_data_point_dict('2020-01-01 12:00:00 UTC'),
            build_pressure_data_point_dict('2020-01-01 13:00:00 UTC'),
        ])
        humidity_dataframe = pd.DataFrame([
            build_humidity_data_point_dict('2020-01-01 12:00:00 UTC'),
            build_humidity_data_point_dict('2020-01-01 13:00:00 UTC'),
        ])
        expected = [pd.concat([pressure_dataframe, humidity_dataframe])]

        with TestPipeline() as p:
            keyed_pressure_rows = p | 'Pressure' >> beam.Create(
                pressure_data_points)
            keyed_humidity_rows = p | 'Humidity' >> beam.Create(
                humidity_data_points)
            output = ({
                'pressure': keyed_pressure_rows,
                'humidity': keyed_humidity_rows
            } | group_into_data_frames.CoGroupIntoDataFrames(
                combine_method='concat'))

            assert_that(output, equal_to(expected, equals_fn=dataframes_equal))

    def test_co_group_data_points_multiple_keys_concat(self):

        pressure_data_points = [
            (key_by.Key(device_id='1',
                        participant_id='1',
                        participant_namespace=1,
                        additional_keys=frozendict.FrozenOrderedDict({})),
             build_pressure_data_point('2020-01-01 12:00:00 UTC')),
            (key_by.Key(device_id='1',
                        participant_id='1',
                        participant_namespace=1,
                        additional_keys=frozendict.FrozenOrderedDict({})),
             build_pressure_data_point('2020-01-01 13:00:00 UTC')),
            (key_by.Key(device_id='2',
                        participant_id='1',
                        participant_namespace=1,
                        additional_keys=frozendict.FrozenOrderedDict({})),
             build_pressure_data_point('2020-01-01 13:00:00 UTC')),
        ]
        humidity_data_points = [
            (key_by.Key(device_id='1',
                        participant_id='1',
                        participant_namespace=1,
                        additional_keys=frozendict.FrozenOrderedDict({})),
             build_humidity_data_point('2020-01-01 12:00:00 UTC')),
            (key_by.Key(device_id='2',
                        participant_id='1',
                        participant_namespace=1,
                        additional_keys=frozendict.FrozenOrderedDict({})),
             build_humidity_data_point('2020-01-01 13:00:00 UTC')),
        ]

        group_1_pressure_dataframe = pd.DataFrame([
            build_pressure_data_point_dict('2020-01-01 12:00:00 UTC'),
            build_pressure_data_point_dict('2020-01-01 13:00:00 UTC'),
        ])
        group_1_humidity_dataframe = pd.DataFrame([
            build_humidity_data_point_dict('2020-01-01 12:00:00 UTC'),
        ])
        group_1_combined = pd.concat(
            [group_1_pressure_dataframe, group_1_humidity_dataframe])

        group_2_pressure_dataframe = pd.DataFrame([
            build_pressure_data_point_dict('2020-01-01 13:00:00 UTC'),
        ])
        group_2_humidity_dataframe = pd.DataFrame([
            build_humidity_data_point_dict('2020-01-01 13:00:00 UTC'),
        ])
        group_2_combined = pd.concat(
            [group_2_pressure_dataframe, group_2_humidity_dataframe])
        expected = [group_1_combined, group_2_combined]

        with TestPipeline() as p:
            keyed_pressure_rows = p | 'Pressure' >> beam.Create(
                pressure_data_points)
            keyed_humidity_rows = p | 'Humidity' >> beam.Create(
                humidity_data_points)
            output = ({
                'pressure': keyed_pressure_rows,
                'humidity': keyed_humidity_rows
            } | group_into_data_frames.CoGroupIntoDataFrames(
                combine_method='concat'))

            assert_that(output, equal_to(expected, equals_fn=dataframes_equal))

    def test_co_group_annotations_single_key_concat(self):

        annotations_1 = [
            (key_by.Key(device_id='1',
                        participant_id='1',
                        participant_namespace=1,
                        additional_keys=frozendict.FrozenOrderedDict({})),
             build_annotation('2020-01-01 12:00:00 UTC',
                              '2020-01-01 13:00:00 UTC',
                              label='one')),
            (key_by.Key(device_id='1',
                        participant_id='1',
                        participant_namespace=1,
                        additional_keys=frozendict.FrozenOrderedDict({})),
             build_annotation('2020-01-01 13:00:00 UTC',
                              '2020-01-01 14:00:00 UTC',
                              label='one')),
        ]

        annotations_2 = [
            (key_by.Key(device_id='1',
                        participant_id='1',
                        participant_namespace=1,
                        additional_keys=frozendict.FrozenOrderedDict({})),
             build_annotation('2020-01-01 12:00:00 UTC',
                              '2020-01-01 13:00:00 UTC',
                              label='two')),
            (key_by.Key(device_id='1',
                        participant_id='1',
                        participant_namespace=1,
                        additional_keys=frozendict.FrozenOrderedDict({})),
             build_annotation('2020-01-01 13:00:00 UTC',
                              '2020-01-01 14:00:00 UTC',
                              label='two')),
        ]

        annotations_1_dataframe = pd.DataFrame([
            build_annotation_dict('2020-01-01 12:00:00 UTC',
                                  '2020-01-01 13:00:00 UTC',
                                  label='one'),
            build_annotation_dict('2020-01-01 13:00:00 UTC',
                                  '2020-01-01 14:00:00 UTC',
                                  label='one'),
        ])
        annotations_2_dataframe = pd.DataFrame([
            build_annotation_dict('2020-01-01 12:00:00 UTC',
                                  '2020-01-01 13:00:00 UTC',
                                  label='two'),
            build_annotation_dict('2020-01-01 13:00:00 UTC',
                                  '2020-01-01 14:00:00 UTC',
                                  label='two'),
        ])
        expected = [
            pd.concat([annotations_1_dataframe, annotations_2_dataframe])
        ]

        with TestPipeline() as p:
            keyed_ann1_rows = p | 'Ann1' >> beam.Create(annotations_1)
            keyed_ann2_rows = p | 'Ann2' >> beam.Create(annotations_2)
            output = ({
                'ann1': keyed_ann1_rows,
                'ann2': keyed_ann2_rows
            } | group_into_data_frames.CoGroupIntoDataFrames(
                combine_method='concat'))

            assert_that(output, equal_to(expected, equals_fn=dataframes_equal))

    def test_co_group_annotations_multiple_keys_concat(self):

        annotations_1 = [
            (key_by.Key(device_id='1',
                        participant_id='1',
                        participant_namespace=1,
                        additional_keys=frozendict.FrozenOrderedDict({})),
             build_annotation('2020-01-01 12:00:00 UTC',
                              '2020-01-01 13:00:00 UTC',
                              label='one')),
            (key_by.Key(device_id='1',
                        participant_id='1',
                        participant_namespace=1,
                        additional_keys=frozendict.FrozenOrderedDict({})),
             build_annotation('2020-01-01 13:00:00 UTC',
                              '2020-01-01 14:00:00 UTC',
                              label='one')),
            (key_by.Key(device_id='2',
                        participant_id='1',
                        participant_namespace=1,
                        additional_keys=frozendict.FrozenOrderedDict({})),
             build_annotation('2020-01-01 13:00:00 UTC',
                              '2020-01-01 14:00:00 UTC',
                              label='one')),
        ]

        annotations_2 = [
            (key_by.Key(device_id='1',
                        participant_id='1',
                        participant_namespace=1,
                        additional_keys=frozendict.FrozenOrderedDict({})),
             build_annotation('2020-01-01 12:00:00 UTC',
                              '2020-01-01 13:00:00 UTC',
                              label='two')),
            (key_by.Key(device_id='2',
                        participant_id='1',
                        participant_namespace=1,
                        additional_keys=frozendict.FrozenOrderedDict({})),
             build_annotation('2020-01-01 13:00:00 UTC',
                              '2020-01-01 14:00:00 UTC',
                              label='two')),
        ]

        group_1_ann1_dataframe = pd.DataFrame([
            build_annotation_dict('2020-01-01 12:00:00 UTC',
                                  '2020-01-01 13:00:00 UTC',
                                  label='one'),
            build_annotation_dict('2020-01-01 13:00:00 UTC',
                                  '2020-01-01 14:00:00 UTC',
                                  label='one'),
        ])
        group_1_ann2_dataframe = pd.DataFrame([
            build_annotation_dict('2020-01-01 12:00:00 UTC',
                                  '2020-01-01 13:00:00 UTC',
                                  label='two'),
        ])
        group_1_combined = pd.concat(
            [group_1_ann1_dataframe, group_1_ann2_dataframe])

        group_2_ann1_dataframe = pd.DataFrame([
            build_annotation_dict('2020-01-01 13:00:00 UTC',
                                  '2020-01-01 14:00:00 UTC',
                                  label='one'),
        ])
        group_2_ann2_dataframe = pd.DataFrame([
            build_annotation_dict('2020-01-01 13:00:00 UTC',
                                  '2020-01-01 14:00:00 UTC',
                                  label='two'),
        ])
        group_2_combined = pd.concat(
            [group_2_ann1_dataframe, group_2_ann2_dataframe])

        expected = [group_1_combined, group_2_combined]

        with TestPipeline() as p:
            keyed_ann1_rows = p | 'Ann1' >> beam.Create(annotations_1)
            keyed_ann2_rows = p | 'Ann2' >> beam.Create(annotations_2)
            output = ({
                'ann1': keyed_ann1_rows,
                'ann2': keyed_ann2_rows
            } | group_into_data_frames.CoGroupIntoDataFrames(
                combine_method='concat'))

            assert_that(output, equal_to(expected, equals_fn=dataframes_equal))

    def test_co_group_data_points_single_key_merge(self):

        pressure_data_points = [
            (key_by.Key(device_id='1',
                        participant_id='1',
                        participant_namespace=1,
                        additional_keys=frozendict.FrozenOrderedDict({})),
             build_pressure_data_point('2020-01-01 12:00:00 UTC')),
            (key_by.Key(device_id='1',
                        participant_id='1',
                        participant_namespace=1,
                        additional_keys=frozendict.FrozenOrderedDict({})),
             build_pressure_data_point('2020-01-01 13:00:00 UTC')),
        ]
        humidity_data_points = [
            (key_by.Key(device_id='1',
                        participant_id='1',
                        participant_namespace=1,
                        additional_keys=frozendict.FrozenOrderedDict({})),
             build_humidity_data_point('2020-01-01 12:00:00 UTC')),
            (key_by.Key(device_id='1',
                        participant_id='1',
                        participant_namespace=1,
                        additional_keys=frozendict.FrozenOrderedDict({})),
             build_humidity_data_point('2020-01-01 13:00:00 UTC')),
        ]

        pressure_dataframe = pd.DataFrame([
            build_pressure_data_point_dict('2020-01-01 12:00:00 UTC'),
            build_pressure_data_point_dict('2020-01-01 13:00:00 UTC'),
        ])
        humidity_dataframe = pd.DataFrame([
            build_humidity_data_point_dict('2020-01-01 12:00:00 UTC'),
            build_humidity_data_point_dict('2020-01-01 13:00:00 UTC'),
        ])
        expected = [
            pressure_dataframe.merge(humidity_dataframe,
                                     on='measurement_timestamp_utc',
                                     suffixes=('_pressure', '_humidity'),
                                     how='outer')
        ]

        with TestPipeline() as p:
            keyed_pressure_rows = p | 'Pressure' >> beam.Create(
                pressure_data_points)
            keyed_humidity_rows = p | 'Humidity' >> beam.Create(
                humidity_data_points)
            output = ({
                'pressure': keyed_pressure_rows,
                'humidity': keyed_humidity_rows
            } | group_into_data_frames.CoGroupIntoDataFrames(
                combine_method='merge'))

            assert_that(output, equal_to(expected, equals_fn=dataframes_equal))

    def test_co_group_data_points_multiple_keys_merge(self):

        pressure_data_points = [
            (key_by.Key(device_id='1',
                        participant_id='1',
                        participant_namespace=1,
                        additional_keys=frozendict.FrozenOrderedDict({})),
             build_pressure_data_point('2020-01-01 12:00:00 UTC')),
            (key_by.Key(device_id='1',
                        participant_id='1',
                        participant_namespace=1,
                        additional_keys=frozendict.FrozenOrderedDict({})),
             build_pressure_data_point('2020-01-01 13:00:00 UTC')),
            (key_by.Key(device_id='2',
                        participant_id='1',
                        participant_namespace=1,
                        additional_keys=frozendict.FrozenOrderedDict({})),
             build_pressure_data_point('2020-01-01 13:00:00 UTC')),
        ]
        humidity_data_points = [
            (key_by.Key(device_id='1',
                        participant_id='1',
                        participant_namespace=1,
                        additional_keys=frozendict.FrozenOrderedDict({})),
             build_humidity_data_point('2020-01-01 12:00:00 UTC')),
            (key_by.Key(device_id='2',
                        participant_id='1',
                        participant_namespace=1,
                        additional_keys=frozendict.FrozenOrderedDict({})),
             build_humidity_data_point('2020-01-01 13:00:00 UTC')),
        ]

        group_1_pressure_dataframe = pd.DataFrame([
            build_pressure_data_point_dict('2020-01-01 12:00:00 UTC'),
            build_pressure_data_point_dict('2020-01-01 13:00:00 UTC'),
        ])
        group_1_humidity_dataframe = pd.DataFrame([
            build_humidity_data_point_dict('2020-01-01 12:00:00 UTC'),
        ])
        group_1_combined = group_1_pressure_dataframe.merge(
            group_1_humidity_dataframe,
            on='measurement_timestamp_utc',
            suffixes=('_pressure', '_humidity'),
            how='outer')

        group_2_pressure_dataframe = pd.DataFrame([
            build_pressure_data_point_dict('2020-01-01 13:00:00 UTC'),
        ])
        group_2_humidity_dataframe = pd.DataFrame([
            build_humidity_data_point_dict('2020-01-01 13:00:00 UTC'),
        ])
        group_2_combined = group_2_pressure_dataframe.merge(
            group_2_humidity_dataframe,
            on='measurement_timestamp_utc',
            suffixes=('_pressure', '_humidity'),
            how='outer')

        expected = [group_1_combined, group_2_combined]

        with TestPipeline() as p:
            keyed_pressure_rows = p | 'Pressure' >> beam.Create(
                pressure_data_points)
            keyed_humidity_rows = p | 'Humidity' >> beam.Create(
                humidity_data_points)
            output = ({
                'pressure': keyed_pressure_rows,
                'humidity': keyed_humidity_rows
            } | group_into_data_frames.CoGroupIntoDataFrames(
                combine_method='merge'))

            assert_that(output, equal_to(expected, equals_fn=dataframes_equal))

    def test_co_group_annotations_single_key_merge(self):

        annotations_1 = [
            (key_by.Key(device_id='1',
                        participant_id='1',
                        participant_namespace=1,
                        additional_keys=frozendict.FrozenOrderedDict({})),
             build_annotation('2020-01-01 12:00:00 UTC',
                              '2020-01-01 13:00:00 UTC',
                              label='one')),
            (key_by.Key(device_id='1',
                        participant_id='1',
                        participant_namespace=1,
                        additional_keys=frozendict.FrozenOrderedDict({})),
             build_annotation('2020-01-01 13:00:00 UTC',
                              '2020-01-01 14:00:00 UTC',
                              label='one')),
        ]

        annotations_2 = [
            (key_by.Key(device_id='1',
                        participant_id='1',
                        participant_namespace=1,
                        additional_keys=frozendict.FrozenOrderedDict({})),
             build_annotation('2020-01-01 12:00:00 UTC',
                              '2020-01-01 13:00:00 UTC',
                              label='two')),
            (key_by.Key(device_id='1',
                        participant_id='1',
                        participant_namespace=1,
                        additional_keys=frozendict.FrozenOrderedDict({})),
             build_annotation('2020-01-01 13:00:00 UTC',
                              '2020-01-01 14:00:00 UTC',
                              label='two')),
        ]

        annotations_1_dataframe = pd.DataFrame([
            build_annotation_dict('2020-01-01 12:00:00 UTC',
                                  '2020-01-01 13:00:00 UTC',
                                  label='one'),
            build_annotation_dict('2020-01-01 13:00:00 UTC',
                                  '2020-01-01 14:00:00 UTC',
                                  label='one'),
        ])
        annotations_2_dataframe = pd.DataFrame([
            build_annotation_dict('2020-01-01 12:00:00 UTC',
                                  '2020-01-01 13:00:00 UTC',
                                  label='two'),
            build_annotation_dict('2020-01-01 13:00:00 UTC',
                                  '2020-01-01 14:00:00 UTC',
                                  label='two'),
        ])
        expected = [
            annotations_1_dataframe.merge(
                annotations_2_dataframe,
                on=['start_timestamp_utc', 'end_timestamp_utc'],
                suffixes=('_ann1', '_ann2'),
                how='outer')
        ]

        with TestPipeline() as p:
            keyed_ann1_rows = p | 'Ann1' >> beam.Create(annotations_1)
            keyed_ann2_rows = p | 'Ann2' >> beam.Create(annotations_2)
            output = ({
                'ann1': keyed_ann1_rows,
                'ann2': keyed_ann2_rows
            } | group_into_data_frames.CoGroupIntoDataFrames(
                combine_method='merge'))

            assert_that(output, equal_to(expected, equals_fn=dataframes_equal))

    def test_co_group_annotations_multiple_keys_merge(self):

        annotations_1 = [
            (key_by.Key(device_id='1',
                        participant_id='1',
                        participant_namespace=1,
                        additional_keys=frozendict.FrozenOrderedDict({})),
             build_annotation('2020-01-01 12:00:00 UTC',
                              '2020-01-01 13:00:00 UTC',
                              label='one')),
            (key_by.Key(device_id='1',
                        participant_id='1',
                        participant_namespace=1,
                        additional_keys=frozendict.FrozenOrderedDict({})),
             build_annotation('2020-01-01 13:00:00 UTC',
                              '2020-01-01 14:00:00 UTC',
                              label='one')),
            (key_by.Key(device_id='2',
                        participant_id='1',
                        participant_namespace=1,
                        additional_keys=frozendict.FrozenOrderedDict({})),
             build_annotation('2020-01-01 13:00:00 UTC',
                              '2020-01-01 14:00:00 UTC',
                              label='one')),
        ]

        annotations_2 = [
            (key_by.Key(device_id='1',
                        participant_id='1',
                        participant_namespace=1,
                        additional_keys=frozendict.FrozenOrderedDict({})),
             build_annotation('2020-01-01 12:00:00 UTC',
                              '2020-01-01 13:00:00 UTC',
                              label='two')),
            (key_by.Key(device_id='2',
                        participant_id='1',
                        participant_namespace=1,
                        additional_keys=frozendict.FrozenOrderedDict({})),
             build_annotation('2020-01-01 13:00:00 UTC',
                              '2020-01-01 14:00:00 UTC',
                              label='two')),
        ]

        group_1_ann1_dataframe = pd.DataFrame([
            build_annotation_dict('2020-01-01 12:00:00 UTC',
                                  '2020-01-01 13:00:00 UTC',
                                  label='one'),
            build_annotation_dict('2020-01-01 13:00:00 UTC',
                                  '2020-01-01 14:00:00 UTC',
                                  label='one'),
        ])
        group_1_ann2_dataframe = pd.DataFrame([
            build_annotation_dict('2020-01-01 12:00:00 UTC',
                                  '2020-01-01 13:00:00 UTC',
                                  label='two'),
        ])
        group_1_combined = group_1_ann1_dataframe.merge(
            group_1_ann2_dataframe,
            on=['start_timestamp_utc', 'end_timestamp_utc'],
            suffixes=('_ann1', '_ann2'),
            how='outer')

        group_2_ann1_dataframe = pd.DataFrame([
            build_annotation_dict('2020-01-01 13:00:00 UTC',
                                  '2020-01-01 14:00:00 UTC',
                                  label='one'),
        ])
        group_2_ann2_dataframe = pd.DataFrame([
            build_annotation_dict('2020-01-01 13:00:00 UTC',
                                  '2020-01-01 14:00:00 UTC',
                                  label='two'),
        ])
        group_2_combined = group_2_ann1_dataframe.merge(
            group_2_ann2_dataframe,
            on=['start_timestamp_utc', 'end_timestamp_utc'],
            suffixes=('_ann1', '_ann2'),
            how='outer')

        expected = [group_1_combined, group_2_combined]

        with TestPipeline() as p:
            keyed_ann1_rows = p | 'Ann1' >> beam.Create(annotations_1)
            keyed_ann2_rows = p | 'Ann2' >> beam.Create(annotations_2)
            output = ({
                'ann1': keyed_ann1_rows,
                'ann2': keyed_ann2_rows
            } | group_into_data_frames.CoGroupIntoDataFrames(
                combine_method='merge'))

            assert_that(output, equal_to(expected, equals_fn=dataframes_equal))

    def test_bad_combine_method_throws_error(self):

        with self.assertRaises(ValueError):
            with TestPipeline() as p:
                one = p | 'One' >> beam.Create([])
                two = p | 'Two' >> beam.Create([])
                _ = ({
                    'one': one,
                    'two': two
                } | group_into_data_frames.CoGroupIntoDataFrames(
                    combine_method='bad'))

    def test_co_group_data_points_metadata_attached(self):

        pressure_data_points = [
            (key_by.Key(device_id='123',
                        participant_id='321',
                        participant_namespace=1,
                        additional_keys=frozendict.FrozenOrderedDict({'a': 1})),
             build_pressure_data_point('2020-01-01 12:00:00 UTC')),
        ]
        humidity_data_points = [
            (key_by.Key(device_id='123',
                        participant_id='321',
                        participant_namespace=1,
                        additional_keys=frozendict.FrozenOrderedDict({'a': 1})),
             build_humidity_data_point('2020-01-01 12:00:00 UTC')),
        ]

        expected_metadata = [
            {
                'data_point_metadata': build_data_point_metadata(),
                'device_id': '123',
                'participant_id': '321',
                'participant_namespace': 1,
                'additional_keys': frozendict.FrozenOrderedDict({'a': 1}),
            },
        ]

        with TestPipeline() as p:
            keyed_pressure_rows = p | 'Pressure' >> beam.Create(
                pressure_data_points)
            keyed_humidity_rows = p | 'Humidity' >> beam.Create(
                humidity_data_points)
            output = ({
                'pressure': keyed_pressure_rows,
                'humidity': keyed_humidity_rows
            } | group_into_data_frames.CoGroupIntoDataFrames(
                combine_method='merge'))

            assert_that(
                output,
                equal_to(expected_metadata,
                         equals_fn=dataframes_metadata_equal))

    def test_group_data_points_time_window_metadata_attached(self):

        pressure_data_points = [
            build_pressure_data_point('2020-01-01 12:00:00 UTC'),
            build_pressure_data_point('2020-01-01 13:00:00 UTC'),
            build_pressure_data_point('2020-01-01 14:00:00 UTC'),
        ]

        humidity_data_points = [
            build_humidity_data_point('2020-01-01 12:00:00 UTC'),
            build_humidity_data_point('2020-01-01 13:00:00 UTC'),
            build_humidity_data_point('2020-01-01 14:00:00 UTC'),
        ]

        expected_metadata = [
            {
                'data_point_metadata': build_data_point_metadata(),
                'device_id': 'C2Q123',
                'participant_id': '123',
                'participant_namespace': 1,
                'additional_keys': {
                    'start_time_range_micros':
                        build_beam_timestamp('2020-01-01 12:00:00 UTC').micros,
                    'end_time_range_micros':
                        build_beam_timestamp('2020-01-01 13:00:00 UTC').micros
                },
            },
            {
                'data_point_metadata': build_data_point_metadata(),
                'device_id': 'C2Q123',
                'participant_id': '123',
                'participant_namespace': 1,
                'additional_keys': {
                    'start_time_range_micros':
                        build_beam_timestamp('2020-01-01 13:00:00 UTC').micros,
                    'end_time_range_micros':
                        build_beam_timestamp('2020-01-01 14:00:00 UTC').micros
                },
            },
            {
                'data_point_metadata': build_data_point_metadata(),
                'device_id': 'C2Q123',
                'participant_id': '123',
                'participant_namespace': 1,
                'additional_keys': {
                    'start_time_range_micros':
                        build_beam_timestamp('2020-01-01 14:00:00 UTC').micros,
                    'end_time_range_micros':
                        build_beam_timestamp('2020-01-01 15:00:00 UTC').micros,
                }
            },
        ]

        with TestPipeline() as p:
            pressure_rows = (
                p | 'create pressure' >> beam.Create(pressure_data_points) |
                'key pressure' >>
                key_by.KeyDataPointsByParticipantDeviceTimeRange(
                    beam_window_fn=beam.window.FixedWindows(60 * 60)))
            humidity_rows = (
                p | 'create humidity' >> beam.Create(humidity_data_points) |
                'key humidity' >>
                key_by.KeyDataPointsByParticipantDeviceTimeRange(
                    beam_window_fn=beam.window.FixedWindows(60 * 60)))
            combined_rows = {
                'pressure': pressure_rows,
                'humidity': humidity_rows
            }
            output = (combined_rows |
                      group_into_data_frames.CoGroupIntoDataFrames(
                          combine_method='merge'))

            assert_that(
                output,
                equal_to(expected_metadata,
                         equals_fn=dataframes_metadata_equal))

    def test_co_group_data_points_single_key_no_combine(self):

        pressure_data_points = [
            build_pressure_data_point('2020-01-01 12:00:00 UTC'),
            build_pressure_data_point('2020-01-01 13:00:00 UTC'),
        ]
        humidity_data_points = [
            build_humidity_data_point('2020-01-01 12:00:00 UTC'),
            build_humidity_data_point('2020-01-01 13:00:00 UTC'),
        ]

        pressure_dataframe_noon = pd.DataFrame([
            build_pressure_data_point_dict('2020-01-01 12:00:00 UTC'),
        ])
        pressure_dataframe_noon.attrs[
            'data_point_metadata'] = build_data_point_metadata()
        pressure_dataframe_noon.attrs['device_id'] = 'C2Q123'
        pressure_dataframe_noon.attrs['participant_id'] = '123'
        pressure_dataframe_noon.attrs['participant_namespace'] = 1
        pressure_dataframe_noon.attrs['additional_keys'] = {
            'start_time_range_micros':
                build_beam_timestamp('2020-01-01 12:00:00 UTC').micros,
            'end_time_range_micros':
                build_beam_timestamp('2020-01-01 13:00:00 UTC').micros
        }

        pressure_dataframe_one = pd.DataFrame([
            build_pressure_data_point_dict('2020-01-01 13:00:00 UTC'),
        ])
        pressure_dataframe_one.attrs[
            'data_point_metadata'] = build_data_point_metadata()
        pressure_dataframe_one.attrs['device_id'] = 'C2Q123'
        pressure_dataframe_one.attrs['participant_id'] = '123'
        pressure_dataframe_one.attrs['participant_namespace'] = 1
        pressure_dataframe_one.attrs['additional_keys'] = {
            'start_time_range_micros':
                build_beam_timestamp('2020-01-01 13:00:00 UTC').micros,
            'end_time_range_micros':
                build_beam_timestamp('2020-01-01 14:00:00 UTC').micros
        }

        humidity_dataframe_noon = pd.DataFrame([
            build_humidity_data_point_dict('2020-01-01 12:00:00 UTC'),
        ])
        humidity_dataframe_noon.attrs[
            'data_point_metadata'] = build_data_point_metadata()
        humidity_dataframe_noon.attrs['device_id'] = 'C2Q123'
        humidity_dataframe_noon.attrs['participant_id'] = '123'
        humidity_dataframe_noon.attrs['participant_namespace'] = 1
        humidity_dataframe_noon.attrs['additional_keys'] = {
            'start_time_range_micros':
                build_beam_timestamp('2020-01-01 12:00:00 UTC').micros,
            'end_time_range_micros':
                build_beam_timestamp('2020-01-01 13:00:00 UTC').micros
        }

        humidity_dataframe_one = pd.DataFrame([
            build_humidity_data_point_dict('2020-01-01 13:00:00 UTC'),
        ])
        humidity_dataframe_one.attrs[
            'data_point_metadata'] = build_data_point_metadata()
        humidity_dataframe_one.attrs['device_id'] = 'C2Q123'
        humidity_dataframe_one.attrs['participant_id'] = '123'
        humidity_dataframe_one.attrs['participant_namespace'] = 1
        humidity_dataframe_one.attrs['additional_keys'] = {
            'start_time_range_micros':
                build_beam_timestamp('2020-01-01 13:00:00 UTC').micros,
            'end_time_range_micros':
                build_beam_timestamp('2020-01-01 14:00:00 UTC').micros
        }

        expected = [{
            'pressure': pressure_dataframe_noon,
            'humidity': humidity_dataframe_noon
        }, {
            'pressure': pressure_dataframe_one,
            'humidity': humidity_dataframe_one
        }]

        with TestPipeline() as p:
            pressure_rows = (
                p | 'create pressure' >> beam.Create(pressure_data_points) |
                'key pressure' >>
                key_by.KeyDataPointsByParticipantDeviceTimeRange(
                    beam_window_fn=beam.window.FixedWindows(60 * 60)))
            humidity_rows = (
                p | 'create humidity' >> beam.Create(humidity_data_points) |
                'key humidity' >>
                key_by.KeyDataPointsByParticipantDeviceTimeRange(
                    beam_window_fn=beam.window.FixedWindows(60 * 60)))
            output = ({
                'pressure': pressure_rows,
                'humidity': humidity_rows
            } | group_into_data_frames.CoGroupIntoDataFrames(
                combine_method=None))

            assert_that(output,
                        equal_to(expected, equals_fn=dataframe_dicts_equal),
                        label='compare dataframes')
            assert_that(output,
                        equal_to(expected,
                                 equals_fn=dataframe_metadata_dicts_equal),
                        label='compare dataframes attrs')


if __name__ == '__main__':
    unittest.main()
