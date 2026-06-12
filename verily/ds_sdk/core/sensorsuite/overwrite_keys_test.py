"""Tests for overwrite_keys.py"""

from dataclasses import dataclass
import unittest

from apache_beam.utils.timestamp import Timestamp
import pandas as pd

from verily.ds_sdk.core import schemas
from verily.ds_sdk.core.schemas import dataspec
from verily.ds_sdk.core.sensorsuite import overwrite_keys
from verily.ds_sdk.core.utils import timestamps
from verily.ds_sdk.protos import types_pb2


@dataspec('com.verily.pressure')
@dataclass
class CustomPressure(schemas.DataPoint):
    pressure: int


def create_pressure_data_point(sensor_store_metadata=None) -> schemas.Pressure:
    return CustomPressure(
        data_point_metadata=schemas.data_point_metadata_for_raw_data(
            data_source_id='123',
            device_id='device',
            participant_id='part',
            participant_namespace=1,
            echo_metadata=None,
            sensor_store_metadata=sensor_store_metadata,
            annotation_labels=set()),
        measurement_timestamp_utc=timestamps.datetime_to_beam_timestamp(
            pd.Timestamp('2020-01-01')),
        pressure=1)


class OverwriteKeysTest(unittest.TestCase):

    def test_time_window_key(self):
        generator = overwrite_keys.OverWriteKeyGenerators.TimeWindow('1h')
        data_point = create_pressure_data_point()

        want = overwrite_keys.OverwriteKey(
            'com.verily.pressure:my_algo:v1:1577836800.0-1577840400.0')
        got = generator.generate_overwrite_key(data_point, 'my_algo', 'v1',
                                               None)

        self.assertEqual(want, got)

    def test_sensor_and_time_window_key(self):
        generator = overwrite_keys.OverWriteKeyGenerators.SensorTimeWindow('1h')
        data_point = create_pressure_data_point()

        want = overwrite_keys.OverwriteKey(
            'com.verily.pressure:my_algo:v1:1577836800.0-1577840400.0:sensor_id'
        )
        got = generator.generate_overwrite_key(
            data_point, 'my_algo', 'v1',
            types_pb2.DataSource(sensor=types_pb2.Sensor(id='sensor_id')))

        self.assertEqual(want, got)

    def test_sensor_and_time_window_key_no_sensor(self):
        generator = overwrite_keys.OverWriteKeyGenerators.SensorTimeWindow('1h')
        data_point = create_pressure_data_point()

        with self.assertRaisesRegex(
                ValueError,
                'Attempting to generate a sensor time range overwrite key with '
                'no sensor'):
            generator.generate_overwrite_key(data_point, 'my_algo', 'v1',
                                             types_pb2.DataSource())

    def test_sensor_and_time_window_key_no_data_source(self):
        generator = overwrite_keys.OverWriteKeyGenerators.SensorTimeWindow('1h')
        data_point = create_pressure_data_point()

        with self.assertRaisesRegex(
                ValueError,
                'No data source found when trying to generate overwrite key '
                'with sensor'):
            generator.generate_overwrite_key(data_point, 'my_algo', 'v1', None)

    def test_sensor_and_time_window_key_and_ss_write_version(self):
        generator = overwrite_keys.OverWriteKeyGenerators.SensorTimeWindowWithWriteTimeVersion(  # pylint: disable=line-too-long
            '1h')
        data_point = create_pressure_data_point(
            sensor_store_metadata=schemas.SensorStoreMetadata(
                sensor_store_write_time=Timestamp(micros=1000)))

        want = overwrite_keys.OverwriteKey(
            key=
            'com.verily.pressure:my_algo:v1:1577836800.0-1577840400.0:sensor_id',  # pylint: disable=line-too-long
            version=1000)
        got = generator.generate_overwrite_key(
            data_point, 'my_algo', 'v1',
            types_pb2.DataSource(sensor=types_pb2.Sensor(id='sensor_id')))

        self.assertEqual(want, got)

    def test_sensor_and_time_window_key_and_ss_write_version_no_metadata(self):
        generator = overwrite_keys.OverWriteKeyGenerators.SensorTimeWindowWithWriteTimeVersion(  # pylint: disable=line-too-long
            '1h')
        data_point = create_pressure_data_point(sensor_store_metadata=None)

        with self.assertRaisesRegex(
                ValueError,
                'Attempting to generate a sensor store write time overwrite'
                ' version from None.'):
            generator.generate_overwrite_key(
                data_point, 'my_algo', 'v1',
                types_pb2.DataSource(sensor=types_pb2.Sensor(id='sensor_id')))


if __name__ == '__main__':
    unittest.main()
