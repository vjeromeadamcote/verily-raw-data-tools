"""Tests for schema_utils."""

import unittest

import apache_beam as beam
from apache_beam.testing.test_pipeline import TestPipeline
from apache_beam.testing.util import assert_that
from apache_beam.testing.util import equal_to
from apache_beam.utils.timestamp import Timestamp
import pandas as pd

from verily.raw_data_tools.schemas import schemas
from verily.raw_data_tools.schemas.schemas import schema_utils
from verily.raw_data_tools.schemas.schemas import shared_schemas
from verily.raw_data_tools.transforms import group_into_data_frames
from verily.raw_data_tools.transforms import key_by


def build_timestamp(timestamp_str: str) -> Timestamp:
    return Timestamp.from_utc_datetime(pd.Timestamp(timestamp_str, tz='UTC'))


def build_data_point(data_source_id: int,
                     timestamp_str: str,
                     device_id='device_id',
                     participant_id='participant_id',
                     sensor_store_metadata=None) -> schemas.Pressure:
    return schemas.Pressure(
        data_point_metadata=schemas.DataPointMetadata(
            data_source_id=data_source_id,
            device_id=device_id,
            participant_id=participant_id,
            participant_namespace=None if participant_id is None else 1,
            echo_metadata=None,
            sensor_store_metadata=sensor_store_metadata,
            annotation_labels=set(),
            _state_key=shared_schemas._STATE_KEY.CREATED_USING_BUILDER),  # pylint: disable=protected-access
        measurement_timestamp_utc=build_timestamp(timestamp_str),
        pressure=1)


class SchemaUtilsTest(unittest.TestCase):

    def test_dp_metadata_set_using_init_throws_error(self):

        with self.assertRaisesRegex(
                ValueError,
                'DataPointMetadata cannot be created using __init__().*'):
            schemas.DataPointMetadata(data_source_id=123,
                                      device_id='device_id',
                                      participant_id='participant_id',
                                      participant_namespace=1,
                                      echo_metadata=None,
                                      sensor_store_metadata=None,
                                      annotation_labels=set())

    def test_dp_metadata_for_raw(self):

        want = schemas.DataPointMetadata(
            data_source_id=123,
            device_id='device_id',
            participant_id='participant_id',
            participant_namespace=1,
            echo_metadata=None,
            sensor_store_metadata=None,
            annotation_labels=set(),
            _state_key=shared_schemas._STATE_KEY.CREATED_USING_BUILDER)  # pylint: disable=protected-access

        got = schema_utils.data_point_metadata_for_raw_data(
            123, 'device_id', 'participant_id', 1, None, None, set())

        self.assertEqual(want, got)

    def test_dp_metadata_for_derived_one_data_source_id(self):

        input_data_points = [
            build_data_point(12345, '2020-01-01 12:00:00 UTC'),
            build_data_point(12345, '2020-01-01 13:00:00 UTC'),
            build_data_point(12345, '2020-01-01 14:00:00 UTC'),
        ]

        want = schemas.DataPointMetadata(
            data_source_id=12345,
            device_id='device_id',
            participant_id='participant_id',
            participant_namespace=1,
            echo_metadata=None,
            sensor_store_metadata=None,
            annotation_labels=set(),
            _state_key=shared_schemas._STATE_KEY.CREATED_USING_BUILDER)  # pylint: disable=protected-access

        got = schema_utils.data_point_metadata_for_derived_data(
            input_data_points)

        self.assertEqual(want, got)

    def test_dp_metadata_for_derived_multiple_data_source_id(self):

        input_data_points = [
            build_data_point(12345,
                             '2020-01-01 12:00:00 UTC',
                             participant_id=None),
            # Can use any DP's participant, as long as there's only 1 distinct
            build_data_point(12345,
                             '2020-01-01 13:00:00 UTC',
                             participant_id='participant_id'),
            # min timestamp should be chosen.
            build_data_point(54321,
                             '2020-01-01 11:00:00 UTC',
                             participant_id=None),
        ]

        want = schemas.DataPointMetadata(
            data_source_id=54321,
            device_id='device_id',
            participant_id='participant_id',
            participant_namespace=1,
            echo_metadata=None,
            sensor_store_metadata=None,
            annotation_labels=set(),
            _state_key=shared_schemas._STATE_KEY.CREATED_USING_BUILDER)  # pylint: disable=protected-access

        got = schema_utils.data_point_metadata_for_derived_data(
            input_data_points)

        self.assertEqual(want, got)

    def test_dp_metadata_for_derived_throw_multiple_device_error(self):

        input_data_points = [
            build_data_point(12345, '2020-01-01 12:00:00 UTC'),
            build_data_point(12345, '2020-01-01 13:00:00 UTC'),
            build_data_point(12345,
                             '2020-01-01 14:00:00 UTC',
                             device_id='other'),
        ]

        with self.assertRaisesRegex(
                RuntimeError,
                'data_point_metadata_for_derived_data encountered multiple '
                'DeviceIDs while parsing the source data points.*'):
            schema_utils.data_point_metadata_for_derived_data(input_data_points)

    def test_dp_metadata_for_derived_throw_multiple_participant_error(self):

        input_data_points = [
            build_data_point(12345, '2020-01-01 12:00:00 UTC'),
            build_data_point(12345, '2020-01-01 13:00:00 UTC'),
            build_data_point(12345,
                             '2020-01-01 14:00:00 UTC',
                             participant_id='other'),
        ]

        with self.assertRaisesRegex(
                RuntimeError,
                'data_point_metadata_for_derived_data encountered multiple '
                'Participants while parsing the source data points.*'):
            schema_utils.data_point_metadata_for_derived_data(input_data_points)

    def test_dp_metadata_for_derived_with_ss_write_time(self):

        input_data_points = [
            build_data_point(12345,
                             '2020-01-01 12:00:00 UTC',
                             sensor_store_metadata=None),
            build_data_point(
                12345,
                '2020-01-01 13:00:00 UTC',
                # Max Timestamp should be used.
                sensor_store_metadata=schemas.SensorStoreMetadata(
                    Timestamp(200))),
            build_data_point(12345,
                             '2020-01-01 14:00:00 UTC',
                             sensor_store_metadata=schemas.SensorStoreMetadata(
                                 Timestamp(100))),
        ]

        want = schemas.DataPointMetadata(
            data_source_id=12345,
            device_id='device_id',
            participant_id='participant_id',
            participant_namespace=1,
            echo_metadata=None,
            sensor_store_metadata=schemas.SensorStoreMetadata(Timestamp(200)),
            annotation_labels=set(),
            _state_key=shared_schemas._STATE_KEY.CREATED_USING_BUILDER)  # pylint: disable=protected-access

        got = schema_utils.data_point_metadata_for_derived_data(
            input_data_points)

        self.assertEqual(want, got)

    def test_dp_metadata_from_df(self):

        input_data_points = [
            build_data_point(12345, '2020-01-01 12:00:00 UTC'),
            build_data_point(12345, '2020-01-01 12:01:00 UTC'),
            build_data_point(12345, '2020-01-01 12:02:00 UTC'),
        ]

        want = schemas.DataPointMetadata(
            data_source_id=12345,
            device_id='device_id',
            participant_id='participant_id',
            participant_namespace=1,
            echo_metadata=None,
            sensor_store_metadata=None,
            annotation_labels=set(),
            _state_key=shared_schemas._STATE_KEY.CREATED_USING_BUILDER)  # pylint: disable=protected-access

        with TestPipeline() as p:
            output = (
                p | beam.Create(input_data_points) |
                key_by.KeyDataPointsByParticipantDeviceTimeRange(
                    beam_window_fn=beam.window.FixedWindows(60 * 60)) |
                group_into_data_frames.GroupIntoDataFrames() | beam.Map(
                    schema_utils.data_point_metadata_for_derived_data_from_df))

            assert_that(output, equal_to([want]))

    def test_dp_metadata_from_df_throws_error(self):

        with self.assertRaisesRegex(
                RuntimeError,
                'No value for `data_point_metadata` in DataFrame.attrs.*'):
            schema_utils.data_point_metadata_for_derived_data_from_df(
                pd.DataFrame())


if __name__ == '__main__':
    unittest.main()
