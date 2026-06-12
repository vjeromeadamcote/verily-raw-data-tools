"""Tests for key_by."""

import copy
from typing import List, NamedTuple, Optional
import unittest

import apache_beam as beam
from apache_beam.testing.test_pipeline import TestPipeline
from apache_beam.testing.util import assert_that
from apache_beam.testing.util import equal_to
from apache_beam.utils.timestamp import Timestamp
import frozendict
import pandas as pd
import pytz

from verily.ds_sdk.contrib import rialto_timezone_fix
from verily.ds_sdk.core import schemas
from verily.ds_sdk.core.io.data_source_cache import DataSourceCache
from verily.ds_sdk.core.transforms import key_by
from verily.ds_sdk.core.utils import timestamps
from verily.ds_sdk.protos import types_pb2


class FakeFlumeRunner(beam.runners.DirectRunner):
    """Fake runner for testing that just inherits from the direct runner."""


def build_timestamp(timestamp_str: str) -> Timestamp:
    return Timestamp.from_utc_datetime(pd.Timestamp(timestamp_str, tz='UTC'))


def build_annotation(device_id: str, participant_id: str,
                     start_timestamp_str: str,
                     end_timestamp_str: str) -> schemas.Annotation:

    annotation_metadata = schemas.AnnotationMetadata(
        device_id=device_id,
        participant_id=participant_id,
        participant_namespace=1,
        version_name=None,
        version_number=None,
        input_data_info=[])

    return schemas.Annotation(
        annotation_label='annotation_label',
        start_timestamp_utc=build_timestamp(start_timestamp_str),
        end_timestamp_utc=build_timestamp(end_timestamp_str),
        annotation_metadata=annotation_metadata)


def build_data_point(device_id: str,
                     participant_id: str,
                     timestamp_str: str,
                     data_source_id: Optional[int] = None) -> schemas.Pressure:
    if data_source_id is None:
        data_source_id = hash(device_id + participant_id)

    data_point_metadata = schemas.data_point_metadata_for_raw_data(
        data_source_id=data_source_id,
        device_id=device_id,
        participant_id=participant_id,
        participant_namespace=1,
        echo_metadata=None,
        sensor_store_metadata=None,
        annotation_labels=set())

    return schemas.Pressure(
        data_point_metadata=data_point_metadata,
        measurement_timestamp_utc=build_timestamp(timestamp_str),
        pressure=1)


def add_annotations_to_data_point(data_point: NamedTuple,
                                  annotations: List[schemas.Annotation]):
    data_point_copy = copy.deepcopy(data_point)
    for ann in annotations:
        data_point_copy.data_point_metadata.annotation_labels.add(
            ann.annotation_label)
    return data_point_copy


def key_data_point_by_measurement_timestamp(data_point):
    return timestamps.beam_timestamp_to_pandas_timestamp(
        data_point.measurement_timestamp_utc).isoformat()


def key_annotations_by_start_timestamp(annotation):
    return timestamps.beam_timestamp_to_pandas_timestamp(
        annotation.start_timestamp_utc).isoformat()


class KeyByTest(unittest.TestCase):

    def test_key_data_points(self):
        data_points = [
            build_data_point(device_id='123',
                             participant_id='321',
                             timestamp_str='2020-01-01 12:00:00'),
            build_data_point(device_id='456',
                             participant_id='654',
                             timestamp_str='2020-01-01 15:00:00'),
        ]
        expected_keys = [
            key_by.Key(device_id='123',
                       participant_id='321',
                       participant_namespace=1,
                       additional_keys=frozendict.FrozenOrderedDict(
                           {'ts': '2020-01-01T12:00:00+00:00'})),
            key_by.Key(device_id='456',
                       participant_id='654',
                       participant_namespace=1,
                       additional_keys=frozendict.FrozenOrderedDict(
                           {'ts': '2020-01-01T15:00:00+00:00'})),
        ]

        expected = list(zip(expected_keys, data_points))

        with TestPipeline() as p:
            output = p | beam.Create(data_points) | key_by.KeyDataPointsBy(
                by_device=True,
                by_participant=True,
                additional_key_fns={
                    'ts': key_data_point_by_measurement_timestamp
                })

            assert_that(output, equal_to(expected))

    def test_key_annotations(self):
        annotations = [
            build_annotation(device_id='123',
                             participant_id='321',
                             start_timestamp_str='2020-01-01 12:00:00',
                             end_timestamp_str='2020-01-01 13:00:00'),
            build_annotation(device_id='456',
                             participant_id='654',
                             start_timestamp_str='2020-01-01 14:00:00',
                             end_timestamp_str='2020-01-01 14:15:00'),
        ]
        expected_keys = [
            key_by.Key(device_id='123',
                       participant_id='321',
                       participant_namespace=1,
                       additional_keys=frozendict.FrozenOrderedDict(
                           {'ts': '2020-01-01T12:00:00+00:00'})),
            key_by.Key(device_id='456',
                       participant_id='654',
                       participant_namespace=1,
                       additional_keys=frozendict.FrozenOrderedDict(
                           {'ts': '2020-01-01T14:00:00+00:00'})),
        ]

        expected = list(zip(expected_keys, annotations))

        with TestPipeline() as p:
            output = p | beam.Create(annotations) | key_by.KeyAnnotationsBy(
                by_device=True,
                by_participant=True,
                additional_key_fns={'ts': key_annotations_by_start_timestamp})

            assert_that(output, equal_to(expected))

    def test_key_annotations_time_range(self):
        annotations = [
            build_annotation(device_id='123',
                             participant_id='321',
                             start_timestamp_str='2020-01-01 12:00:00',
                             end_timestamp_str='2020-01-01 13:00:00'),
            build_annotation(device_id='456',
                             participant_id='654',
                             start_timestamp_str='2020-01-01 15:00:00',
                             end_timestamp_str='2020-01-01 15:15:00'),
        ]
        expected_keys = [
            key_by.Key(
                device_id='123',
                participant_id='321',
                participant_namespace=1,
                additional_keys=frozendict.FrozenOrderedDict({
                    'start_time_range_micros': Timestamp(1577880000).micros,
                    'end_time_range_micros': Timestamp(1577880060).micros
                })),
            key_by.Key(
                device_id='456',
                participant_id='654',
                participant_namespace=1,
                additional_keys=frozendict.FrozenOrderedDict({
                    'start_time_range_micros': Timestamp(1577890800).micros,
                    'end_time_range_micros': Timestamp(1577890860).micros
                })),
        ]

        expected = list(zip(expected_keys, annotations))

        with TestPipeline() as p:
            output = p | beam.Create(
                annotations
            ) | key_by.KeyAnnotationsByParticipantDeviceTimeRange(
                beam_window_fn=beam.window.FixedWindows(60),
                by_start_timestamp=True,
                by_end_timestamp=False)

            assert_that(output, equal_to(expected))

    def test_key_annotations_time_range_most_common_tz(self):
        annotations = [
            build_annotation(device_id='123',
                             participant_id='321',
                             start_timestamp_str='2020-01-01 12:00:00',
                             end_timestamp_str='2020-01-01 13:00:00'),
            build_annotation(device_id='456',
                             participant_id='654',
                             start_timestamp_str='2020-01-01 15:00:00',
                             end_timestamp_str='2020-01-01 15:15:00'),
        ]
        expected_keys = [
            key_by.Key(device_id='123',
                       participant_id='321',
                       participant_namespace=1,
                       additional_keys=frozendict.FrozenOrderedDict({
                           'start_time_range_micros':
                               Timestamp(1577880000 - 28800).micros,
                           'end_time_range_micros':
                               Timestamp(1577880060 - 28800).micros
                       })),
            key_by.Key(device_id='456',
                       participant_id='654',
                       participant_namespace=1,
                       additional_keys=frozendict.FrozenOrderedDict({
                           'start_time_range_micros':
                               Timestamp(1577890800 - 18000).micros,
                           'end_time_range_micros':
                               Timestamp(1577890860 - 18000).micros
                       })),
        ]

        utc_offset_map = rialto_timezone_fix.UtcOffsetMap()
        utc_offset_map.add_utc_offset('123', -28800.0)
        utc_offset_map.add_utc_offset('456', -18000.0)

        expected = list(zip(expected_keys, annotations))

        with TestPipeline() as p:
            output = (p | beam.Create(annotations) | key_by.
                      KeyAnnotationsByParticipantDeviceTimeRangeInLocalTimezone(
                          beam_window_fn=beam.window.FixedWindows(60),
                          by_start_timestamp=True,
                          by_end_timestamp=False,
                          utc_offset_map=utc_offset_map))

            assert_that(output, equal_to(expected))

    def test_key_annotations_time_range_in_timezone(self):
        annotations = [
            build_annotation(device_id='123',
                             participant_id='321',
                             start_timestamp_str='2020-01-01 01:00:00',
                             end_timestamp_str='2020-01-01 02:00:00'),
            build_annotation(device_id='123',
                             participant_id='321',
                             start_timestamp_str='2020-01-01 15:00:00',
                             end_timestamp_str='2020-01-01 15:15:00'),
        ]
        expected_keys = [
            key_by.Key(
                device_id='123',
                participant_id='321',
                participant_namespace=1,
                additional_keys=frozendict.FrozenOrderedDict({
                    'start_time_range_micros': Timestamp(1577750400).micros,
                    'end_time_range_micros': Timestamp(1577836800).micros
                })),
            key_by.Key(
                device_id='123',
                participant_id='321',
                participant_namespace=1,
                additional_keys=frozendict.FrozenOrderedDict({
                    'start_time_range_micros': Timestamp(1577836800).micros,
                    'end_time_range_micros': Timestamp(1577923200).micros
                })),
        ]

        expected = list(zip(expected_keys, annotations))

        with TestPipeline() as p:
            output = p | beam.Create(
                annotations
            ) | key_by.KeyAnnotationsByParticipantDeviceTimeRangeInTimezone(
                beam_window_fn=beam.window.FixedWindows(60 * 60 * 24),
                by_start_timestamp=True,
                by_end_timestamp=False,
                timezone=pytz.timezone('US/Eastern'))

            assert_that(output, equal_to(expected))

    def test_key_by_with_fixed_time_range(self):
        data_points = [
            build_data_point(device_id='123',
                             participant_id='321',
                             timestamp_str='2020-01-01 12:00:00'),
            build_data_point(device_id='456',
                             participant_id='654',
                             timestamp_str='2020-01-01 15:00:00'),
        ]
        expected_keys = [
            key_by.Key(
                device_id='123',
                participant_id='321',
                participant_namespace=1,
                additional_keys=frozendict.FrozenOrderedDict({
                    'start_time_range_micros': Timestamp(1577880000).micros,
                    'end_time_range_micros': Timestamp(1577880060).micros
                })),
            key_by.Key(
                device_id='456',
                participant_id='654',
                participant_namespace=1,
                additional_keys=frozendict.FrozenOrderedDict({
                    'start_time_range_micros': Timestamp(1577890800).micros,
                    'end_time_range_micros': Timestamp(1577890860).micros
                })),
        ]

        expected = list(zip(expected_keys, data_points))

        with TestPipeline() as p:
            output = p | beam.Create(
                data_points) | key_by.KeyDataPointsByParticipantDeviceTimeRange(
                    beam_window_fn=beam.window.FixedWindows(60))

            assert_that(output, equal_to(expected))

    def test_key_by_with_sliding_time_range(self):
        data_points = [
            build_data_point(device_id='123',
                             participant_id='321',
                             timestamp_str='2020-01-01 12:00:00'),
            build_data_point(device_id='456',
                             participant_id='654',
                             timestamp_str='2020-01-01 12:01:00'),
        ]
        expected = [
            (key_by.Key(
                device_id='123',
                participant_id='321',
                participant_namespace=1,
                additional_keys=frozendict.FrozenOrderedDict({
                    'start_time_range_micros': Timestamp(1577880000).micros,
                    'end_time_range_micros': Timestamp(1577880120).micros
                })), data_points[0]),
            (key_by.Key(
                device_id='123',
                participant_id='321',
                participant_namespace=1,
                additional_keys=frozendict.FrozenOrderedDict({
                    'start_time_range_micros': Timestamp(1577879940).micros,
                    'end_time_range_micros': Timestamp(1577880060).micros
                })), data_points[0]),
            (key_by.Key(
                device_id='456',
                participant_id='654',
                participant_namespace=1,
                additional_keys=frozendict.FrozenOrderedDict({
                    'start_time_range_micros': Timestamp(1577880060).micros,
                    'end_time_range_micros': Timestamp(1577880180).micros
                })), data_points[1]),
            (key_by.Key(
                device_id='456',
                participant_id='654',
                participant_namespace=1,
                additional_keys=frozendict.FrozenOrderedDict({
                    'start_time_range_micros': Timestamp(1577880000).micros,
                    'end_time_range_micros': Timestamp(1577880120).micros
                })), data_points[1]),
        ]

        with TestPipeline() as p:
            output = p | beam.Create(
                data_points) | key_by.KeyDataPointsByParticipantDeviceTimeRange(
                    beam_window_fn=beam.window.SlidingWindows(120, 60))

            assert_that(output, equal_to(expected))

    def test_key_by_with_fixed_time_range_in_local(self):
        data_points = [
            # Falls on 2019-12-31 in eastern timezone.
            build_data_point(device_id='123',
                             participant_id='321',
                             timestamp_str='2020-01-01 04:00:00',
                             data_source_id=12345),
            # Falls on 2019-12-31 in pacific timezone.
            build_data_point(device_id='123',
                             participant_id='321',
                             timestamp_str='2020-01-01 07:00:00',
                             data_source_id=54321),
            # Falls on 2020-01-01 in eastern timezone.
            build_data_point(device_id='123',
                             participant_id='321',
                             timestamp_str='2020-01-01 05:00:00',
                             data_source_id=12345),
            # Falls on 2020-01-01 in pacific timezone.
            build_data_point(device_id='123',
                             participant_id='321',
                             timestamp_str='2020-01-01 08:00:00',
                             data_source_id=54321),
            # Falls on 2020-01-02 in eastern timezone.
            build_data_point(device_id='123',
                             participant_id='321',
                             timestamp_str='2020-01-02 06:00:00',
                             data_source_id=12345),
            # Falls on 2020-01-02 in pacific timezone.
            build_data_point(device_id='123',
                             participant_id='321',
                             timestamp_str='2020-01-02 09:00:00',
                             data_source_id=54321),
            # Falls on 2020-01-02 in eastern timezone.
            build_data_point(device_id='123',
                             participant_id='321',
                             timestamp_str='2020-01-02 07:00:00',
                             data_source_id=12345),
            # Falls on 2020-01-02 in pacific timezone.
            build_data_point(device_id='123',
                             participant_id='321',
                             timestamp_str='2020-01-02 10:00:00',
                             data_source_id=54321),
            # Falls on 2020-01-02 in eastern timezone.
            build_data_point(device_id='123',
                             participant_id='321',
                             timestamp_str='2020-01-03 00:00:00',
                             data_source_id=12345),
            # Falls on 2020-01-02 in pacific timezone.
            build_data_point(device_id='123',
                             participant_id='321',
                             timestamp_str='2020-01-03 03:00:00',
                             data_source_id=54321),
            # Different device
            build_data_point(device_id='new_device',
                             participant_id='321',
                             timestamp_str='2020-01-03 03:00:00',
                             data_source_id=54321),
        ]

        data_source_cache = DataSourceCache({
            12345:
                types_pb2.DataSource(device=types_pb2.Device(
                    time_zone_name='US/Eastern')),
            54321:
                types_pb2.DataSource(device=types_pb2.Device(
                    time_zone_name='US/Pacific'))
        })
        expected_counts = [2, 2, 6, 1]

        with TestPipeline() as p:
            output = (
                p | beam.Create(data_points) |
                key_by.KeyDataPointsByParticipantDeviceTimeRangeInLocalTimezone(
                    beam_window_fn=beam.window.FixedWindows(60 * 60 * 24),
                    data_source_cache=data_source_cache) |
                beam.combiners.Count.PerKey() | beam.Values()  # pylint: disable=no-value-for-parameter
            )

            assert_that(output, equal_to(expected_counts))

    def test_key_by_with_fixed_time_range_in_most_common_local(self):
        data_points = [
            # Falls on 2019-12-31 in eastern timezone.
            build_data_point(device_id='456',
                             participant_id='321',
                             timestamp_str='2020-01-01 04:00:00',
                             data_source_id=12345),
            # Falls on 2019-12-31 in pacific timezone.
            build_data_point(device_id='123',
                             participant_id='321',
                             timestamp_str='2020-01-01 07:00:00',
                             data_source_id=54321),
            # Falls on 2020-01-01 in eastern timezone.
            build_data_point(device_id='456',
                             participant_id='321',
                             timestamp_str='2020-01-01 05:00:00',
                             data_source_id=12345),
            # Falls on 2020-01-01 in eastern timezone.
            build_data_point(device_id='456',
                             participant_id='321',
                             timestamp_str='2020-01-01 08:00:00',
                             data_source_id=54321),
            # Falls on 2020-01-02 in eastern timezone.
            build_data_point(device_id='456',
                             participant_id='321',
                             timestamp_str='2020-01-02 06:00:00',
                             data_source_id=12345),
            # Falls on 2020-01-02 in pacific timezone.
            build_data_point(device_id='123',
                             participant_id='321',
                             timestamp_str='2020-01-02 09:00:00',
                             data_source_id=54321),
            # Falls on 2020-01-02 in eastern timezone.
            build_data_point(device_id='456',
                             participant_id='321',
                             timestamp_str='2020-01-02 07:00:00',
                             data_source_id=12345),
            # Falls on 2020-01-02 in pacific timezone.
            build_data_point(device_id='123',
                             participant_id='321',
                             timestamp_str='2020-01-02 10:00:00',
                             data_source_id=54321),
            # Falls on 2020-01-02 in eastern timezone.
            build_data_point(device_id='456',
                             participant_id='321',
                             timestamp_str='2020-01-03 00:00:00',
                             data_source_id=12345),
            # Falls on 2020-01-02 in eastern timezone.
            build_data_point(device_id='456',
                             participant_id='321',
                             timestamp_str='2020-01-03 03:00:00',
                             data_source_id=54321),
        ]

        utc_offset_map = rialto_timezone_fix.UtcOffsetMap()
        utc_offset_map.add_utc_offset('123', -28800.0)
        utc_offset_map.add_utc_offset('456', -18000.0)

        expected_counts = [1, 1, 2, 2, 4]

        with TestPipeline() as p:
            output = (
                p | beam.Create(data_points) |
                key_by.KeyDataPointsByParticipantDeviceTimeRangeInLocalTimezone(
                    beam_window_fn=beam.window.FixedWindows(60 * 60 * 24),
                    utc_offset_map=utc_offset_map) |
                beam.combiners.Count.PerKey() | beam.Values()  # pylint: disable=no-value-for-parameter
            )

            assert_that(output, equal_to(expected_counts))

    def test_key_by_with_fixed_time_range_in_most_common_local_readable(self):
        data_points = [
            # Falls on 2019-12-31 in eastern timezone.
            build_data_point(device_id='456',
                             participant_id='321',
                             timestamp_str='2020-01-01 04:00:00',
                             data_source_id=12345),
            # Falls on 2019-12-31 in pacific timezone.
            build_data_point(device_id='123',
                             participant_id='321',
                             timestamp_str='2020-01-01 07:00:00',
                             data_source_id=54321),
            # Falls on 2020-01-01 in eastern timezone.
            build_data_point(device_id='456',
                             participant_id='321',
                             timestamp_str='2020-01-01 05:00:00',
                             data_source_id=12345),
            # Falls on 2020-01-01 in eastern timezone.
            build_data_point(device_id='456',
                             participant_id='321',
                             timestamp_str='2020-01-01 08:00:00',
                             data_source_id=54321),
            # Falls on 2020-01-02 in eastern timezone.
            build_data_point(device_id='456',
                             participant_id='321',
                             timestamp_str='2020-01-02 06:00:00',
                             data_source_id=12345),
            # Falls on 2020-01-02 in pacific timezone.
            build_data_point(device_id='123',
                             participant_id='321',
                             timestamp_str='2020-01-02 09:00:00',
                             data_source_id=54321),
            # Falls on 2020-01-02 in eastern timezone.
            build_data_point(device_id='456',
                             participant_id='321',
                             timestamp_str='2020-01-02 07:00:00',
                             data_source_id=12345),
            # Falls on 2020-01-02 in pacific timezone.
            build_data_point(device_id='123',
                             participant_id='321',
                             timestamp_str='2020-01-02 10:00:00',
                             data_source_id=54321),
            # Falls on 2020-01-02 in eastern timezone.
            build_data_point(device_id='456',
                             participant_id='321',
                             timestamp_str='2020-01-03 00:00:00',
                             data_source_id=12345),
            # Falls on 2020-01-02 in eastern timezone.
            build_data_point(device_id='456',
                             participant_id='321',
                             timestamp_str='2020-01-03 03:00:00',
                             data_source_id=54321),
        ]

        utc_offset_map = rialto_timezone_fix.UtcOffsetMap()
        utc_offset_map.add_utc_offset('123', 'America/Los_Angeles')
        utc_offset_map.add_utc_offset('456', 'America/New_York')

        expected_counts = [1, 1, 2, 2, 4]

        with TestPipeline() as p:
            output = (
                p | beam.Create(data_points) |
                key_by.KeyDataPointsByParticipantDeviceTimeRangeInLocalTimezone(
                    beam_window_fn=beam.window.FixedWindows(60 * 60 * 24),
                    utc_offset_map=utc_offset_map) |
                beam.combiners.Count.PerKey() | beam.Values()  # pylint: disable=no-value-for-parameter
            )

            assert_that(output, equal_to(expected_counts))

    def test_key_by_with_fixed_time_range_in_fixed_timezone(self):
        data_points = [
            # Falls on 2019-12-31 in eastern timezone.
            build_data_point(device_id='123',
                             participant_id='321',
                             timestamp_str='2020-01-01 04:00:00'),
            # Falls on 2020-01-01 in eastern timezone.
            build_data_point(device_id='123',
                             participant_id='321',
                             timestamp_str='2020-01-01 05:00:00'),
            # Falls on 2020-01-02 in eastern timezone.
            build_data_point(device_id='123',
                             participant_id='321',
                             timestamp_str='2020-01-02 06:00:00'),
            # Falls on 2020-01-02 in eastern timezone.
            build_data_point(device_id='123',
                             participant_id='321',
                             timestamp_str='2020-01-02 07:00:00'),
            # Falls on 2020-01-02 in eastern timezone.
            build_data_point(device_id='123',
                             participant_id='321',
                             timestamp_str='2020-01-03 00:00:00'),
            build_data_point(device_id='new_device',
                             participant_id='321',
                             timestamp_str='2020-01-03 00:00:00'),
        ]

        # The expected number of points per day window
        expected_counts = [1, 1, 3, 1]

        with TestPipeline() as p:
            output = (
                p | beam.Create(data_points) |
                key_by.KeyDataPointsByParticipantDeviceTimeRangeInTimezone(
                    beam_window_fn=beam.window.FixedWindows(60 * 60 * 24),
                    timezone=pytz.timezone('US/Eastern')) |
                beam.combiners.Count.PerKey() | beam.Values()  # pylint: disable=no-value-for-parameter
            )

            assert_that(output, equal_to(expected_counts))


class KeyByParticipantDeviceSensorTest(unittest.TestCase):

    def test_key_data_points_by_sensor(self):
        data_points = [
            build_data_point(device_id='123',
                             participant_id='321',
                             timestamp_str='2020-01-01 12:00:00'),
            build_data_point(device_id='456',
                             participant_id='654',
                             timestamp_str='2020-01-01 15:00:00'),
        ]
        data_source_cache = DataSourceCache({
            hash('123' + '321'):
                types_pb2.DataSource(sensor=types_pb2.Sensor(id='1')),
            hash('456' + '654'):
                types_pb2.DataSource(sensor=types_pb2.Sensor(id='2')),
        })
        expected_keys = [
            key_by.Key(device_id='123',
                       participant_id='321',
                       participant_namespace=1,
                       additional_keys=frozendict.FrozenOrderedDict(
                           {'sensor_id': '1'})),
            key_by.Key(device_id='456',
                       participant_id='654',
                       participant_namespace=1,
                       additional_keys=frozendict.FrozenOrderedDict(
                           {'sensor_id': '2'})),
        ]

        expected = list(zip(expected_keys, data_points))

        with TestPipeline() as p:
            output = (
                p | beam.Create(data_points) |
                key_by.KeyDataPointsByParticipantDeviceSensor(data_source_cache)
            )

            assert_that(output, equal_to(expected))

    def test_key_data_points_by_sensor_with_fixed_time_range(self):
        data_points = [
            build_data_point(device_id='123',
                             participant_id='321',
                             timestamp_str='2020-01-01 12:00:00'),
            build_data_point(device_id='456',
                             participant_id='654',
                             timestamp_str='2020-01-01 15:00:00'),
        ]
        data_source_cache = DataSourceCache({
            hash('123' + '321'):
                types_pb2.DataSource(sensor=types_pb2.Sensor(id='1')),
            hash('456' + '654'):
                types_pb2.DataSource(sensor=types_pb2.Sensor(id='2')),
        })
        expected_keys = [
            key_by.Key(
                device_id='123',
                participant_id='321',
                participant_namespace=1,
                additional_keys=frozendict.FrozenOrderedDict({
                    'sensor_id': '1',
                    'start_time_range_micros': Timestamp(1577880000).micros,
                    'end_time_range_micros': Timestamp(1577880060).micros
                })),
            key_by.Key(
                device_id='456',
                participant_id='654',
                participant_namespace=1,
                additional_keys=frozendict.FrozenOrderedDict({
                    'sensor_id': '2',
                    'start_time_range_micros': Timestamp(1577890800).micros,
                    'end_time_range_micros': Timestamp(1577890860).micros
                })),
        ]

        expected = list(zip(expected_keys, data_points))

        with TestPipeline() as p:
            output = p | beam.Create(
                data_points
            ) | key_by.KeyDataPointsByParticipantDeviceSensorTimeRange(
                beam_window_fn=beam.window.FixedWindows(60),
                data_source_cache=data_source_cache)

            assert_that(output, equal_to(expected))

    def test_key_by_with_fixed_time_range_no_device(self):
        data_group_1 = [
            # Same participant/time - different device
            build_data_point(device_id='123',
                             participant_id='321',
                             timestamp_str='2020-01-01 12:00:00'),
            build_data_point(device_id='321',
                             participant_id='321',
                             timestamp_str='2020-01-01 12:00:00')
        ]
        data_group_2 = [
            # Same participant/time - different device
            build_data_point(device_id='456',
                             participant_id='654',
                             timestamp_str='2020-01-01 15:00:00'),
            build_data_point(device_id='555',
                             participant_id='654',
                             timestamp_str='2020-01-01 15:00:00'),
        ]
        data_points = data_group_1 + data_group_2
        key_1 = (key_by.Key(
            device_id=None,
            participant_id='321',
            participant_namespace=1,
            additional_keys=frozendict.FrozenOrderedDict({
                'start_time_range_micros': Timestamp(1577880000).micros,
                'end_time_range_micros': Timestamp(1577880060).micros
            })))
        key_2 = (key_by.Key(
            device_id=None,
            participant_id='654',
            participant_namespace=1,
            additional_keys=frozendict.FrozenOrderedDict({
                'start_time_range_micros': Timestamp(1577890800).micros,
                'end_time_range_micros': Timestamp(1577890860).micros
            })))

        expected = [(key_1, data_group_1[0]), (key_1, data_group_1[1]),
                    (key_2, data_group_2[0]), (key_2, data_group_2[1])]

        with TestPipeline() as p:
            output = p | beam.Create(
                data_points) | key_by.KeyDataPointsByParticipantTimeRange(
                    beam_window_fn=beam.window.FixedWindows(60))

            assert_that(output, equal_to(expected))


if __name__ == '__main__':
    unittest.main()
