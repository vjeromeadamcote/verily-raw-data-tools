"""Tests for decorators_test."""

import dataclasses
from typing import List, Optional
import unittest

from apache_beam.utils.timestamp import Timestamp

from verily.ds_sdk.core import schemas
from verily.ds_sdk.core.schemas import decorators
from verily.ds_sdk.core.schemas import shared_schemas


class DecoratorsTest(unittest.TestCase):

    def test_compatible_data_spec(self):

        @decorators.dataspec('com.verily.imu')  # pylint: disable=unused-variable,missing-class-docstring
        @dataclasses.dataclass
        class TestImuDataSpec(schemas.DataPoint):  # pylint: disable=unused-variable,missing-class-docstring
            acceleration_x: Optional[List[int]]
            acceleration_y: Optional[List[int]]
            acceleration_z: Optional[List[int]]
            gyro_x: Optional[List[int]]
            gyro_y: Optional[List[int]]
            gyro_z: Optional[List[int]]
            sampling_rate: Optional[int]
            sensor_id: Optional[int]
            true_timestamp_millis: Optional[Timestamp]
            true_timestamp_sample_index: Optional[Timestamp]

    def test_missing_input_fields(self):
        with self.assertRaisesRegex(
                ValueError,
                'Input schema was missing fields that are present on the data '
                'spec'):

            @decorators.dataspec('com.verily.imu')  # pylint: disable=unused-variable,missing-class-docstring
            @dataclasses.dataclass
            class TestImuDataSpec(schemas.DataPoint):  # pylint: disable=unused-variable,missing-class-docstring
                acceleration_x: Optional[List[int]]
                acceleration_y: Optional[List[int]]
                acceleration_z: Optional[List[int]]
                gyro_x: Optional[List[int]]
                gyro_y: Optional[List[int]]
                gyro_z: Optional[List[int]]
                sampling_rate: Optional[int]
                true_timestamp_millis: Optional[Timestamp]
                true_timestamp_sample_index: Optional[Timestamp]

    def test_extra_input_fields(self):
        with self.assertRaisesRegex(
                ValueError,
                'Sensors data spec was missing fields that are present on the '
                'input schema'):

            @decorators.dataspec('com.verily.imu')  # pylint: disable=unused-variable,missing-class-docstring
            @dataclasses.dataclass
            class TestImuDataSpec(schemas.DataPoint):  # pylint: disable=unused-variable,missing-class-docstring
                acceleration_x: Optional[List[int]]
                acceleration_y: Optional[List[int]]
                acceleration_z: Optional[List[int]]
                gyro_x: Optional[List[int]]
                gyro_y: Optional[List[int]]
                gyro_z: Optional[List[int]]
                sampling_rate: Optional[int]
                sensor_id: Optional[int]
                extra_field: Optional[int]
                true_timestamp_millis: Optional[Timestamp]
                true_timestamp_sample_index: Optional[Timestamp]

    def test_data_spec_missing_timestamp(self):
        with self.assertRaisesRegex(
                ValueError, 'A measurement_timestamp_utc field of type '
                'apache_beam.utils.timestamp.Timestamp is required to write '
                'data to SensorStore.'):

            @decorators.dataspec('com.verily.imu')  # pylint: disable=unused-variable,missing-class-docstring
            @dataclasses.dataclass
            class TestImuDataSpec:  # pylint: disable=unused-variable,missing-class-docstring
                acceleration_x: Optional[List[int]]
                acceleration_y: Optional[List[int]]
                acceleration_z: Optional[List[int]]
                gyro_x: Optional[List[int]]
                gyro_y: Optional[List[int]]
                gyro_z: Optional[List[int]]
                sampling_rate: Optional[int]
                sensor_id: Optional[int]
                data_point_metadata: shared_schemas.DataPointMetadata
                true_timestamp_millis: Optional[Timestamp]
                true_timestamp_sample_index: Optional[Timestamp]

    def test_data_spec_missing_metadata(self):
        with self.assertRaisesRegex(
                ValueError, 'A data_point_metadata field of type '
                'verily.ds_sdk.core.schemas.shared_schemas.DataPointMetadata is'
                ' required to write data to SensorStore.'):

            @decorators.dataspec('com.verily.imu')  # pylint: disable=unused-variable,missing-class-docstring
            @dataclasses.dataclass
            class TestImuDataSpec:  # pylint: disable=unused-variable,missing-class-docstring
                measurement_timestamp_utc: Timestamp
                acceleration_x: Optional[List[int]]
                acceleration_y: Optional[List[int]]
                acceleration_z: Optional[List[int]]
                gyro_x: Optional[List[int]]
                gyro_y: Optional[List[int]]
                gyro_z: Optional[List[int]]
                sampling_rate: Optional[int]
                sensor_id: Optional[int]
                true_timestamp_millis: Optional[Timestamp]
                true_timestamp_sample_index: Optional[Timestamp]


if __name__ == '__main__':
    unittest.main()
