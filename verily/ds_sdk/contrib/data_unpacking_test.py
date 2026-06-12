"""Tests for data unpacking."""

from typing import Any, Dict
import unittest

import apache_beam as beam
from apache_beam.testing.test_pipeline import TestPipeline
from apache_beam.testing.util import assert_that
from apache_beam.testing.util import equal_to
from numba import config
import numpy as np
import pandas as pd

#pylint: disable=wrong-import-position
config.DISABLE_JIT = True
import verily.ds_sdk.contrib.data_unpacking as ds_unpack
from verily.ds_sdk.core import schemas
from verily.ds_sdk.core.transforms import BuildDataPointDataFrames
from verily.ds_sdk.core.utils import timestamps
from verily.ds_sdk.protos import types_pb2


def create_eda_data_point(eda_len: int, true_timestamp: str,
                          true_timestamp_sample_index: int,
                          nominal_sampling_rate: int, sensor_id: int,
                          measurement_timestamp_utc: str,
                          data_source_id: int = 3028052620409363825) -> schemas.Eda: #pylint: disable=line-too-long
    if true_timestamp is not None:
        true_timestamp = timestamps.datetime_to_beam_timestamp(
            pd.Timestamp(true_timestamp))
    return schemas.Eda(
        data_point_metadata=schemas.data_point_metadata_for_raw_data(
            data_source_id=data_source_id,
            device_id='123',
            participant_id='123',
            participant_namespace=1,
            echo_metadata=None,
            sensor_store_metadata=None,
            annotation_labels=set()),
        sensor_id=sensor_id,
        measurement_timestamp_utc=timestamps.datetime_to_beam_timestamp(
            pd.Timestamp(measurement_timestamp_utc)),
        raw_adc=list(np.random.randn(eda_len)),
        true_timestamp_millis=true_timestamp,
        true_timestamp_sample_index=true_timestamp_sample_index,
        sampling_rate=nominal_sampling_rate)


def create_picard_eda_data_point(eda_len: int, true_timestamp: str,
                          true_timestamp_sample_index: int,
                          nominal_sampling_rate: int, sensor_id: int,
                          measurement_timestamp_utc: str) -> schemas.PicardEda:
    if true_timestamp is not None:
        true_timestamp = timestamps.datetime_to_beam_timestamp(
            pd.Timestamp(true_timestamp))
    return schemas.PicardEda(
        data_point_metadata=schemas.data_point_metadata_for_raw_data(
            data_source_id=3028052620409363825,
            device_id='123',
            participant_id='123',
            participant_namespace=1,
            echo_metadata=None,
            sensor_store_metadata=None,
            annotation_labels=set()),
        sensor_id=sensor_id,
        measurement_timestamp_utc=timestamps.datetime_to_beam_timestamp(
            pd.Timestamp(measurement_timestamp_utc)),
        real_adc=list(np.random.randn(eda_len)),
        im_adc=list(np.random.randn(eda_len)),
        z2_adc=list(np.random.randn(eda_len)),
        true_timestamp_millis=true_timestamp,
        true_timestamp_sample_index=true_timestamp_sample_index,
        sampling_rate=nominal_sampling_rate)


def create_ecg_data_point(ecg_len: int, true_timestamp: str,
                          true_timestamp_sample_index: int,
                          nominal_sampling_rate: int, sensor_id: int,
                          measurement_timestamp_utc: str) -> schemas.Ecg:
    if true_timestamp is not None:
        true_timestamp = timestamps.datetime_to_beam_timestamp(
            pd.Timestamp(true_timestamp))
    return schemas.Ecg(
        data_point_metadata=schemas.data_point_metadata_for_raw_data(
            data_source_id=3028052620409363825,
            device_id='123',
            participant_id='123',
            participant_namespace=1,
            echo_metadata=None,
            sensor_store_metadata=None,
            annotation_labels=set()),
        sensor_id=sensor_id,
        measurement_timestamp_utc=timestamps.datetime_to_beam_timestamp(
            pd.Timestamp(measurement_timestamp_utc)),
        raw_adc=list(np.random.randn(ecg_len)),
        true_timestamp_millis=true_timestamp,
        true_timestamp_sample_index=true_timestamp_sample_index,
        sampling_rate=nominal_sampling_rate)


def create_ppg_data_point(ppg_len: int, true_timestamp: str,
                          true_timestamp_sample_index: int,
                          nominal_sampling_rate: int, sensor_id: int,
                          measurement_timestamp_utc: str) -> schemas.Ppg:
    if true_timestamp is not None:
        true_timestamp = timestamps.datetime_to_beam_timestamp(
            pd.Timestamp(true_timestamp))
    return schemas.Ppg(
        data_point_metadata=schemas.data_point_metadata_for_raw_data(
            data_source_id=3028052620409363825,
            device_id='123',
            participant_id='123',
            participant_namespace=1,
            echo_metadata=None,
            sensor_store_metadata=None,
            annotation_labels=set()),
        sensor_id=sensor_id,
        measurement_timestamp_utc=timestamps.datetime_to_beam_timestamp(
            pd.Timestamp(measurement_timestamp_utc)),
        green=list(np.random.randn(ppg_len)),
        true_timestamp_millis=true_timestamp,
        true_timestamp_sample_index=true_timestamp_sample_index,
        sampling_rate=nominal_sampling_rate)


def create_two_channel_ppg_data_point(
        ppg_len: int,
        true_timestamp: str,
        true_timestamp_sample_index: int,
        nominal_sampling_rate: int, sensor_id: int,
        measurement_timestamp_utc: str) -> schemas.StudywatchTwo_Channel_Ppg:
    if true_timestamp is not None:
        true_timestamp = timestamps.datetime_to_beam_timestamp(
            pd.Timestamp(true_timestamp))
    return schemas.StudywatchTwo_Channel_Ppg(
        data_point_metadata=schemas.data_point_metadata_for_raw_data(
            data_source_id=3028052620409363825,
            device_id='123',
            participant_id='123',
            participant_namespace=1,
            echo_metadata=None,
            sensor_store_metadata=None,
            annotation_labels=set()),
        sensor_id=sensor_id,
        measurement_timestamp_utc=timestamps.datetime_to_beam_timestamp(
            pd.Timestamp(measurement_timestamp_utc)),
        green=list(np.random.randn(ppg_len)),
        green_2=list(np.random.randn(ppg_len)),
        true_timestamp_millis=true_timestamp,
        true_timestamp_sample_index=true_timestamp_sample_index,
        sampling_rate=nominal_sampling_rate)


def create_imu_data_point(
    imu_len: int,
    true_timestamp: str,
    true_timestamp_sample_index: int,
    nominal_sampling_rate: int,
    sensor_id: int,
    measurement_timestamp_utc: str,
) -> schemas.Imu:
    if true_timestamp is not None:
        true_timestamp = timestamps.datetime_to_beam_timestamp(
            pd.Timestamp(true_timestamp))
    gyro_x = None
    gyro_y = None
    gyro_z = None
    if sensor_id in ['0', '1']:
        gyro_x = list(np.random.randn(imu_len))
        gyro_y = list(np.random.randn(imu_len))
        gyro_z = list(np.random.randn(imu_len))
    return schemas.Imu(
        data_point_metadata=schemas.data_point_metadata_for_raw_data(
            data_source_id=3028052620409363825,
            device_id='123',
            participant_id='123',
            participant_namespace=1,
            echo_metadata=None,
            sensor_store_metadata=None,
            annotation_labels=set()),
        sensor_id=sensor_id,
        measurement_timestamp_utc=timestamps.datetime_to_beam_timestamp(
            pd.Timestamp(measurement_timestamp_utc)),
        acceleration_x=list(np.random.randn(imu_len)),
        acceleration_y=list(np.random.randn(imu_len)),
        acceleration_z=list(np.random.randn(imu_len)),
        gyro_x=gyro_x,
        gyro_y=gyro_y,
        gyro_z=gyro_z,
        true_timestamp_millis=true_timestamp,
        true_timestamp_sample_index=true_timestamp_sample_index,
        sampling_rate=nominal_sampling_rate)


def create_ppg_data_point_dict(ppg_len: int, true_timestamp: str,
                               true_timestamp_sample_index: int,
                               nominal_sampling_rate: int,
                               sensor_id: int) -> Dict[str, Any]:
    return {
        'data_point_metadata': {
            'data_source_id': 3028052620409363825,
            'device_id': '123',
            'participant_id': '123',
            'participant_namespace': 1,
            'echo_metadata': None,
            'sensor_store_metadata': None,
            'annotation_labels': set()
        },
        'sensor_id': sensor_id,
        'measurement_timestamp_utc': pd.Timestamp('2020-01-01'),
        'green': np.random.randn(ppg_len),
        'true_timestamp_millis': pd.Timestamp(true_timestamp),
        'true_timestamp_sample_index': true_timestamp_sample_index,
        'sampling_rate': nominal_sampling_rate
    }


def estimate_sampling_frequency(sensor_df: pd.DataFrame) -> float:
    '''Function to estimate sampling frequency from unpacked df
    '''
    return 1000.0 / sensor_df['timestamp_ms'].diff()[1]


def return_final_metadata_value(unpacked_df: pd.DataFrame, col: str) -> Any:
    """Function to return the final unpacked metadata value given the column
    """
    return unpacked_df['data_point_metadata'].values[0][col]


class UnpackDataTest(unittest.TestCase):

    def test_unpack_imu_count_sensor2(self):
        data_points = [
            create_imu_data_point(100, '2022-01-01 00:00:00', 10, 100, '2',
                                  '2022-01-01 00:00:00'),
            create_imu_data_point(100, '2022-01-01 00:00:01', 10, 100, '2',
                                  '2022-01-01 00:00:01'),
            create_imu_data_point(100, '2022-01-01 00:00:02', 10, 100, '2',
                                  '2022-01-01 00:00:02'),
            create_imu_data_point(100, '2022-01-01 00:00:03', 10, 100, '2',
                                  '2022-01-01 00:00:03'),
            # Point with not enough data to perform interpolation. This should
            # fall back to using the legacy unpack method.
            create_imu_data_point(
                100,
                '2022-01-01 02:00:03',
                10,
                100,
                '2',
                measurement_timestamp_utc='2022-01-01 02:00:03'),
            create_imu_data_point(
                100,
                '2022-01-01 02:00:04',
                10,
                100,
                '2',
                measurement_timestamp_utc='2022-01-01 02:00:04')
        ]
        ds_cache = {
            3028052620409363825:
                types_pb2.DataSource(sensor=types_pb2.Sensor(id='2'))
        }
        expected_len = [100.0 * len(data_points)]
        with TestPipeline() as p:
            output = (p | beam.Create(data_points) |
                      ds_unpack.UnpackImu(sensor_id='2',
                                          data_source_cache=ds_cache,
                                          fall_back_to_legacy=True) |
                      beam.combiners.Count.Globally())
            assert_that(output, equal_to(expected_len))

    def test_unpack_imu_count_sensor1(self):
        data_points = [
            create_imu_data_point(100, '2022-01-01 00:00:00', 10, 100, '1',
                                  '2022-01-01 00:00:00'),
            create_imu_data_point(100, '2022-01-01 00:00:01', 10, 100, '1',
                                  '2022-01-01 00:00:01'),
            create_imu_data_point(100, '2022-01-01 00:00:02', 10, 100, '1',
                                  '2022-01-01 00:00:02'),
            create_imu_data_point(100, '2022-01-01 00:00:03', 10, 100, '1',
                                  '2022-01-01 00:00:03')
        ]
        ds_cache = {
            3028052620409363825:
                types_pb2.DataSource(sensor=types_pb2.Sensor(id='1'))
        }
        expected_len = [100.0 * len(data_points)]
        with TestPipeline() as p:
            output = (p | beam.Create(data_points) | ds_unpack.UnpackImu(
                sensor_id='1', data_source_cache=ds_cache) |
                      beam.combiners.Count.Globally())
            assert_that(output, equal_to(expected_len))

    def test_unpack_imu_count_sensor0(self):
        data_points = [
            create_imu_data_point(100, '2022-01-01 00:00:00', 10, 100, '0',
                                  '2022-01-01 00:00:00'),
            create_imu_data_point(100, '2022-01-01 00:00:01', 10, 100, '0',
                                  '2022-01-01 00:00:01'),
            create_imu_data_point(100, '2022-01-01 00:00:02', 10, 100, '0',
                                  '2022-01-01 00:00:02'),
            create_imu_data_point(100, '2022-01-01 00:00:03', 10, 100, '0',
                                  '2022-01-01 00:00:03')
        ]
        ds_cache = {
            3028052620409363825:
                types_pb2.DataSource(sensor=types_pb2.Sensor(id='0'))
        }
        expected_len = [100.0 * len(data_points)]
        with TestPipeline() as p:
            output = (p | beam.Create(data_points) | ds_unpack.UnpackImu(
                sensor_id='0', data_source_cache=ds_cache) |
                      beam.combiners.Count.Globally())
            assert_that(output, equal_to(expected_len))

    def test_unpack_eda_count(self):
        data_points = [
            create_eda_data_point(100, '2022-01-01 00:00:00', 10, 100, 1,
                                  '2022-01-01 00:00:00'),
            create_eda_data_point(100, '2022-01-01 00:00:01', 10, 100, 1,
                                  '2022-01-01 00:00:01'),
            create_eda_data_point(100, '2022-01-01 00:00:02', 10, 100, 1,
                                  '2022-01-01 00:00:02'),
            create_eda_data_point(100, '2022-01-01 00:00:03', 10, 100, 1,
                                  '2022-01-01 00:00:03')
        ]
        expected_len = [100.0 * len(data_points)]
        with TestPipeline() as p:
            output = (p | beam.Create(data_points) | ds_unpack.UnpackEda() |
                      beam.combiners.Count.Globally())
            assert_that(output, equal_to(expected_len))

    def test_unpack_eda_count_sensor1(self):
        data_points = [
            create_eda_data_point(100, '2022-01-01 00:00:00', 10, 100, '1',
                                  '2022-01-01 00:00:00'),
            create_eda_data_point(100, '2022-01-01 00:00:01', 10, 100, '1',
                                  '2022-01-01 00:00:01'),
            create_eda_data_point(100, '2022-01-01 00:00:02', 10, 100, '1',
                                  '2022-01-01 00:00:02'),
            create_eda_data_point(100, '2022-01-01 00:00:03', 10, 100, '1',
                                  '2022-01-01 00:00:03')
        ]
        ds_cache = {
            3028052620409363825:
                types_pb2.DataSource(sensor=types_pb2.Sensor(id='1'))
        }
        expected_len = [100.0 * len(data_points)]
        with TestPipeline() as p:
            output = (p | beam.Create(data_points) | ds_unpack.UnpackEda(
                sensor_id='1', data_source_cache=ds_cache) |
                      beam.combiners.Count.Globally())
            assert_that(output, equal_to(expected_len))

    def test_unpack_eda_sensors_filter_0(self):
        data_points = [
            create_eda_data_point(100, '2022-01-01 00:00:00', 10, 100, '0',
                                  '2022-01-01 00:00:00', 3028052620409363825),
            create_eda_data_point(100, '2022-01-01 00:00:01', 10, 100, '0',
                                  '2022-01-01 00:00:01', 3028052620409363825),
            create_eda_data_point(100, '2022-01-01 00:00:02', 10, 100, '0',
                                  '2022-01-01 00:00:02', 3028052620409363825),
            create_eda_data_point(100, '2022-01-01 00:00:03', 10, 100, '0',
                                  '2022-01-01 00:00:03', 3028052620409363825),
            create_eda_data_point(100, '2022-01-01 00:00:00', 10, 100, '1',
                                  '2022-01-01 00:00:00', 3028052620409363826),
            create_eda_data_point(100, '2022-01-01 00:00:01', 10, 100, '1',
                                  '2022-01-01 00:00:01', 3028052620409363826),
        ]
        expected_len_0 = [100.0 * 4]

        ds_cache = {
            3028052620409363825:
                types_pb2.DataSource(sensor=types_pb2.Sensor(id='0')),
            3028052620409363826:
                types_pb2.DataSource(sensor=types_pb2.Sensor(id='1'))
        }
        with TestPipeline() as p:
            data_points_beam = p | beam.Create(data_points)

            output = (data_points_beam | ds_unpack.UnpackEda(
                sensor_id='0', data_source_cache=ds_cache) |
                beam.combiners.Count.Globally())
            assert_that(output, equal_to(expected_len_0))

    def test_unpack_eda_sensors_no_filter(self):
        data_points = [
            create_eda_data_point(100, '2022-01-01 00:00:00', 10, 100, '0',
                                  '2022-01-01 00:00:00', 3028052620409363825),
            create_eda_data_point(100, '2022-01-01 00:00:01', 10, 100, '0',
                                  '2022-01-01 00:00:01', 3028052620409363825),
            create_eda_data_point(100, '2022-01-01 00:00:02', 10, 100, '0',
                                  '2022-01-01 00:00:02', 3028052620409363825),
            create_eda_data_point(100, '2022-01-01 00:00:03', 10, 100, '0',
                                  '2022-01-01 00:00:03', 3028052620409363825),
        ]
        expected_len = [100.0 * len(data_points)]

        with TestPipeline() as p:
            data_points_beam = p | beam.Create(data_points)

            output = (data_points_beam | ds_unpack.UnpackEda() |
                beam.combiners.Count.Globally())
            assert_that(output, equal_to(expected_len))

    def test_unpack_picard_eda_count(self):
        data_points = [
            create_picard_eda_data_point(100, '2022-01-01 00:00:00', 10, 100, 1,
                                  '2022-01-01 00:00:00'),
            create_picard_eda_data_point(100, '2022-01-01 00:00:01', 10, 100, 1,
                                  '2022-01-01 00:00:01'),
            create_picard_eda_data_point(100, '2022-01-01 00:00:02', 10, 100, 1,
                                  '2022-01-01 00:00:02'),
            create_picard_eda_data_point(100, '2022-01-01 00:00:03', 10, 100, 1,
                                  '2022-01-01 00:00:03')
        ]
        expected_len = [100.0 * len(data_points)]
        with TestPipeline() as p:
            output = (p | beam.Create(data_points) |
                      ds_unpack.UnpackPicardEda() |
                      beam.combiners.Count.Globally())
            assert_that(output, equal_to(expected_len))

    def test_unpack_ecg_count(self):
        data_points = [
            create_ecg_data_point(100, '2022-01-01 00:00:00', 10, 100, 1,
                                  '2022-01-01 00:00:00'),
            create_ecg_data_point(100, '2022-01-01 00:00:01', 10, 100, 1,
                                  '2022-01-01 00:00:01'),
            create_ecg_data_point(100, '2022-01-01 00:00:02', 10, 100, 1,
                                  '2022-01-01 00:00:02'),
            create_ecg_data_point(100, '2022-01-01 00:00:03', 10, 100, 1,
                                  '2022-01-01 00:00:03')
        ]
        expected_len = [100.0 * len(data_points)]
        with TestPipeline() as p:
            output = (p | beam.Create(data_points) | ds_unpack.UnpackEcg() |
                      beam.combiners.Count.Globally())
            assert_that(output, equal_to(expected_len))

    def test_unpack_ppg(self):
        data_points = [
            create_ppg_data_point(100, '2022-01-01 00:00:00', 10, 100, 1,
                                  '2022-01-01 00:00:00'),
            create_ppg_data_point(100, '2022-01-01 00:00:01', 10, 100, 1,
                                  '2022-01-01 00:00:01'),
            create_ppg_data_point(100, '2022-01-01 00:00:02', 10, 100, 1,
                                  '2022-01-01 00:00:02'),
            create_ppg_data_point(100, '2022-01-01 00:00:03', 10, 100, 1,
                                  '2022-01-01 00:00:03')
        ]
        expected_len = [100.0 * len(data_points)]
        with TestPipeline() as p:
            output = (p | beam.Create(data_points) | ds_unpack.UnpackPpg() |
                      beam.combiners.Count.Globally())
            assert_that(output, equal_to(expected_len))

    def test_unpack_two_channel_ppg(self):
        data_points = [
            create_two_channel_ppg_data_point(
                100, '2022-01-01 00:00:00', 10, 100, 1, '2022-01-01 00:00:00'),
            create_two_channel_ppg_data_point(
                100, '2022-01-01 00:00:01', 10, 100, 1, '2022-01-01 00:00:01'),
            create_two_channel_ppg_data_point(
                100, '2022-01-01 00:00:02', 10, 100, 1, '2022-01-01 00:00:02'),
            create_two_channel_ppg_data_point(
                100, '2022-01-01 00:00:03', 10, 100, 1, '2022-01-01 00:00:03')
        ]
        expected_len = [100.0 * len(data_points)]
        with TestPipeline() as p:
            output = (p | beam.Create(data_points) |
                      ds_unpack.UnpackTwoChannelPpg() |
                      beam.combiners.Count.Globally())
            assert_that(output, equal_to(expected_len))

    def test_estimated_sampling_frequency(self):
        data_points = [
            create_ppg_data_point(100, '2022-01-01 00:00:00', 10, 100, 1,
                                  '2022-01-01 00:00:00'),
            create_ppg_data_point(100, '2022-01-01 00:00:01', 10, 100, 1,
                                  '2022-01-01 00:00:01'),
            create_ppg_data_point(100, '2022-01-01 00:00:02', 10, 100, 1,
                                  '2022-01-01 00:00:02'),
            create_ppg_data_point(100, '2022-01-01 00:00:03', 10, 100, 1,
                                  '2022-01-01 00:00:03')
        ]
        expected_fs = [100.0]
        with TestPipeline() as p:
            output = (p | beam.Create(data_points) |
                      BuildDataPointDataFrames.PerParticipantDevice() |
                      beam.Map(ds_unpack.unpack_data_frame, ['green']) |
                      beam.Map(estimate_sampling_frequency))
            assert_that(output, equal_to(expected_fs))

    def test_estimated_sampling_frequency_missing_packet(self):
        data_points = [
            create_ppg_data_point(100, '2022-01-01 00:00:00', 10, 100, 1,
                                  '2022-01-01 00:00:00'),
            create_ppg_data_point(100, '2022-01-01 00:00:01', 10, 100, 1,
                                  '2022-01-01 00:00:01'),
            create_ppg_data_point(100, '2022-01-01 00:00:02', 10, 100, 1,
                                  '2022-01-01 00:00:02'),
            create_ppg_data_point(100, '2022-01-01 00:00:03', 10, 100, 1,
                                  '2022-01-01 00:00:03'),
            # dropped packets in this interval
            create_ppg_data_point(100, '2022-01-01 00:00:20', 10, 100, 1,
                                  '2022-01-01 00:00:20'),
            create_ppg_data_point(100, '2022-01-01 00:00:21', 10, 100, 1,
                                  '2022-01-01 00:00:21')
        ]
        expected_fs = [100.0]
        with TestPipeline() as p:
            output = (p | beam.Create(data_points) |
                      BuildDataPointDataFrames.PerParticipantDevice() |
                      beam.Map(ds_unpack.unpack_data_frame, ['green']) |
                      beam.Map(estimate_sampling_frequency))
            assert_that(output, equal_to(expected_fs))

    def test_estimated_sampling_frequency_missing_samples_in_packet(self):
        data_points = [
            create_ppg_data_point(100, '2022-01-01 00:00:00', 10, 100, 1,
                                  '2022-01-01 00:00:00'),
            create_ppg_data_point(100, '2022-01-01 00:00:01', 10, 100, 1,
                                  '2022-01-01 00:00:01'),
            create_ppg_data_point(100, '2022-01-01 00:00:02', 10, 100, 1,
                                  '2022-01-01 00:00:02'),
            create_ppg_data_point(50, '2022-01-01 00:00:03', 10, 100, 1,
                                  '2022-01-01 00:00:03'),
            create_ppg_data_point(100, '2022-01-01 00:00:04', 10, 100, 1,
                                  '2022-01-01 00:00:04'),
            create_ppg_data_point(50, '2022-01-01 00:00:05', 10, 100, 1,
                                  '2022-01-01 00:00:05')
        ]
        expected_fs = [100.0]
        with TestPipeline() as p:
            output = (p | beam.Create(data_points) |
                      BuildDataPointDataFrames.PerParticipantDevice() |
                      beam.Map(ds_unpack.unpack_data_frame, ['green']) |
                      beam.Map(estimate_sampling_frequency))
            assert_that(output, equal_to(expected_fs))

    def test_length_unpacked_data(self):
        data_points = [
            create_ppg_data_point(100, '2022-01-01 00:00:00', 10, 100, 1,
                                  '2022-01-01 00:00:00'),
            create_ppg_data_point(100, '2022-01-01 00:00:01', 10, 100, 1,
                                  '2022-01-01 00:00:01'),
            create_ppg_data_point(100, '2022-01-01 00:00:02', 10, 100, 1,
                                  '2022-01-01 00:00:02'),
            create_ppg_data_point(100, '2022-01-01 00:00:03', 10, 100, 1,
                                  '2022-01-01 00:00:03')
        ]
        expected_len = [100.0 * len(data_points)]
        with TestPipeline() as p:
            output = (p | beam.Create(data_points) | ds_unpack.UnpackPpg() |
                      beam.combiners.Count.Globally())
            assert_that(output, equal_to(expected_len))

    def test_missing_true_timestamps_replacement(self):
        data_points = [
            create_ppg_data_point(100, None, None, 100, 1,
                                  '2022-01-01 00:00:00'),
            create_ppg_data_point(100, None, None, 100, 1,
                                  '2022-01-01 00:00:01'),
            create_ppg_data_point(100, None, None, 100, 1,
                                  '2022-01-01 00:00:02'),
            create_ppg_data_point(100, None, None, 100, 1,
                                  '2022-01-01 00:00:03')
        ]
        expected_len = [100.0 * len(data_points)]
        with TestPipeline() as p:
            output = (p | beam.Create(data_points) |
                      BuildDataPointDataFrames.PerParticipantDevice() |
                      beam.Map(ds_unpack.unpack_data_frame, ['green']) |
                      beam.Map(len))
            assert_that(output, equal_to(expected_len))

    def test_extreme_difference_from_nominal_fs_error(self):
        data_points = [
            create_ppg_data_point_dict(100, '2022-01-01 00:00:00', 10, 100, 1),
            create_ppg_data_point_dict(100, '2022-01-01 00:00:01', 10, 100, 1),
            create_ppg_data_point_dict(100, '2022-01-01 00:00:02', 10, 100, 1),
            create_ppg_data_point_dict(100, '2022-01-01 00:00:03', 10, 100, 1)
        ]
        test_df = pd.DataFrame(data_points)
        fs_nom_incorrect = 9
        test_df['sampling_rate'] = fs_nom_incorrect
        error_msg_regex = 'Stable median sampling rate'
        with self.assertRaisesRegex(ValueError, error_msg_regex):
            ds_unpack.unpack_data_frame(test_df, ['green'])

    def test_extreme_difference_from_nominal_fs_error_ignore(self):
        data_points = [
            create_ppg_data_point_dict(100, '2022-01-01 00:00:00', 10, 100, 1),
            create_ppg_data_point_dict(100, '2022-01-01 00:00:01', 10, 100, 1),
            create_ppg_data_point_dict(100, '2022-01-01 00:00:02', 10, 100, 1),
            create_ppg_data_point_dict(100, '2022-01-01 00:00:03', 10, 100, 1)
        ]
        test_df = pd.DataFrame(data_points)
        fs_nom_incorrect = 9
        test_df['sampling_rate'] = fs_nom_incorrect
        # all excess samples are kept in last packet
        with self.assertWarns(UserWarning):
            unpacked_df = ds_unpack.unpack_data_frame(
                test_df, ['green'], ignore_median_fs_error=True)
            self.assertEqual(unpacked_df.shape[0], 400)

    def test_massive_first_packet(self):
        data_points = [
            create_ppg_data_point_dict(200, '2022-01-01 00:00:00', 10, 100, 1),
            create_ppg_data_point_dict(100, '2022-01-01 00:00:01', 10, 100, 1),
            create_ppg_data_point_dict(100, '2022-01-01 00:00:02', 10, 100, 1),
            create_ppg_data_point_dict(100, '2022-01-01 00:00:03', 10, 100, 1)
        ]
        test_df = pd.DataFrame(data_points)
        unpacked_df = ds_unpack.unpack_data_frame(test_df, ['green'])

        # Excess samples mess up the fs_est of the second packet, which is
        # replaced with N=fs_mean NaN samples
        self.assertEqual(unpacked_df.shape[0], 400)

    def test_massive_last_packet(self):
        data_points = [
            create_ppg_data_point_dict(100, '2022-01-01 00:00:00', 10, 100, 1),
            create_ppg_data_point_dict(100, '2022-01-01 00:00:01', 10, 100, 1),
            create_ppg_data_point_dict(100, '2022-01-01 00:00:02', 10, 100, 1),
            create_ppg_data_point_dict(200, '2022-01-01 00:00:03', 10, 100, 1)
        ]
        test_df = pd.DataFrame(data_points)
        unpacked_df = ds_unpack.unpack_data_frame(test_df, ['green'])
        # all excess samples are kept in last packet
        self.assertEqual(unpacked_df.shape[0], 500)

    def test_massive_middle_packet(self):
        data_points = [
            create_ppg_data_point_dict(100, '2022-01-01 00:00:00', 10, 100, 1),
            create_ppg_data_point_dict(100, '2022-01-01 00:00:01', 10, 100, 1),
            create_ppg_data_point_dict(200, '2022-01-01 00:00:02', 10, 100, 1),
            create_ppg_data_point_dict(100, '2022-01-01 00:00:03', 10, 100, 1)
        ]
        test_df = pd.DataFrame(data_points)
        unpacked_df = ds_unpack.unpack_data_frame(test_df, ['green'])
        # excessive sample rate middle packets are replaced with N=fs_mean
        # NaN samples
        self.assertEqual(unpacked_df.shape[0], 400)

    def test_multiple_sensor_id_error(self):
        data_points = [
            create_ppg_data_point_dict(100, '2022-01-01 00:00:00', 10, 100, 1),
            create_ppg_data_point_dict(100, '2022-01-01 00:00:01', 10, 100, 1),
            create_ppg_data_point_dict(100, '2022-01-01 00:00:02', 10, 100, 2),
            create_ppg_data_point_dict(100, '2022-01-01 00:00:03', 10, 100, 1)
        ]
        test_df = pd.DataFrame(data_points)
        error_msg_regex = 'More than one sensor_id detected'
        with self.assertRaisesRegex(ValueError, error_msg_regex):
            ds_unpack.unpack_data_frame(test_df, ['green'])

    def test_unpacked_device_id(self):
        # test to determine if the device_id is correctly unpacked
        data_points = [
            create_ppg_data_point(100, '2022-01-01 00:00:00', 10, 100, 1,
                                  '2022-01-01 00:00:00'),
            create_ppg_data_point(100, '2022-01-01 00:00:01', 10, 100, 1,
                                  '2022-01-01 00:00:01'),
            create_ppg_data_point(100, '2022-01-01 00:00:02', 10, 100, 1,
                                  '2022-01-01 00:00:02'),
            create_ppg_data_point(100, '2022-01-01 00:00:03', 10, 100, 1,
                                  '2022-01-01 00:00:03')
        ]
        expected_device_id = [data_points[-1].data_point_metadata.device_id]
        with TestPipeline() as p:
            output = (p | beam.Create(data_points) |
                      BuildDataPointDataFrames.PerParticipantDevice() |
                      beam.Map(ds_unpack.unpack_data_frame, ['green'],
                               additional_cols_to_keep=['data_point_metadata'])
                      | beam.Map(return_final_metadata_value, 'device_id'))
            assert_that(output, equal_to(expected_device_id))

    def test_empty_first_packet(self):
        # test to determine if the device_id is correctly unpacked
        data_points = [
            create_ppg_data_point(0, '2022-01-01 00:00:00', 10, 100, 1,
                                  '2022-01-01 00:00:00'),
            create_ppg_data_point(100, '2022-01-01 00:00:01', 10, 100, 1,
                                  '2022-01-01 00:00:01'),
            create_ppg_data_point(100, '2022-01-01 00:00:02', 10, 100, 1,
                                  '2022-01-01 00:00:02'),
            create_ppg_data_point(100, '2022-01-01 00:00:03', 10, 100, 1,
                                  '2022-01-01 00:00:03')
        ]
        expected_length = [300]
        with TestPipeline() as p:
            output = (p | beam.Create(data_points) |
                      BuildDataPointDataFrames.PerParticipantDevice() |
                      beam.Map(ds_unpack.unpack_data_frame, ['green'],
                               additional_cols_to_keep=['data_point_metadata'])
                      | beam.Map(lambda df: df.shape[0]))
            assert_that(output, equal_to(expected_length))

    def test_empty_packets(self):
        # test to determine if the device_id is correctly unpacked
        data_points = [
            create_ppg_data_point(0, '2022-01-01 00:00:00', 10, 100, 1,
                                  '2022-01-01 00:00:00'),
            create_ppg_data_point(100, '2022-01-01 00:00:01', 10, 100, 1,
                                  '2022-01-01 00:00:01'),
            create_ppg_data_point(0, '2022-01-01 00:00:02', 10, 100, 1,
                                  '2022-01-01 00:00:02'),
            create_ppg_data_point(100, '2022-01-01 00:00:03', 10, 100, 1,
                                  '2022-01-01 00:00:03'),
            create_ppg_data_point(100, '2022-01-01 00:00:04', 10, 100, 1,
                                  '2022-01-01 00:00:04'),
            create_ppg_data_point(100, '2022-01-01 00:00:05', 10, 100, 1,
                                  '2022-01-01 00:00:05')
        ]
        with self.assertRaises(ValueError):
            with TestPipeline() as p:
                _ = (p | beam.Create(data_points) |
                     BuildDataPointDataFrames.PerParticipantDevice() |
                     beam.Map(ds_unpack.unpack_data_frame, ['green'],
                              additional_cols_to_keep=['data_point_metadata']) |
                     beam.Map(lambda df: df.shape[0]))

    def test_all_empty_packets(self):
        # test to determine if the device_id is correctly unpacked
        data_points = [
            create_ppg_data_point(0, '2022-01-01 00:00:00', 10, 100, 1,
                                  '2022-01-01 00:00:00'),
            create_ppg_data_point(0, '2022-01-01 00:00:01', 10, 100, 1,
                                  '2022-01-01 00:00:01'),
            create_ppg_data_point(0, '2022-01-01 00:00:02', 10, 100, 1,
                                  '2022-01-01 00:00:02'),
            create_ppg_data_point(0, '2022-01-01 00:00:03', 10, 100, 1,
                                  '2022-01-01 00:00:03'),
            create_ppg_data_point(0, '2022-01-01 00:00:04', 10, 100, 1,
                                  '2022-01-01 00:00:04')
        ]
        expected_length = [0]
        with TestPipeline() as p:
            output = (p | beam.Create(data_points) |
                      BuildDataPointDataFrames.PerParticipantDevice() |
                      beam.Map(ds_unpack.unpack_data_frame, ['green'],
                               additional_cols_to_keep=['data_point_metadata'])
                      | beam.Map(lambda df: df.shape[0]))
            assert_that(output, equal_to(expected_length))

    def test_no_fs_valid(self):
        df = pd.DataFrame([{
            'measurement_timestamp_utc':
                pd.Timestamp('2019-11-07 17:10:29.534000 UTC'),
            'acceleration_x': [
                -31744, -31744, -30720, -31488, -30976, -31232, -31488, -31744,
                -31232, -31488, -30976, -31744, -31232, -31232, -31488, -31232,
                -31488, -31232, -32000, -30976, -30976, -31744, -30720, -30976,
                -31488, -31744, -31744, -32000, -31744, -31232, -31232, -30976,
                -30976, -31488, -30976, -31232, -31232, -31488, -31232, -30976,
                -30976, -31488, -31488, -31232, -30976, -31232, -30976, -30976,
                -32000, -32000, -31488, -31488, -31232, -30720, -30976, -32000,
                -30976, -31744, -31744, -30720, -31232, -31232, -31232, -31488,
                -31488, -32256, -30976, -30976, -31488, -31232, -31488, -31488,
                -31232, -31744, -31744
            ],
            'sampling_rate':
                50,
            'sensor_id':
                '',
            'true_timestamp_millis':
                None
        }, {
            'measurement_timestamp_utc':
                pd.Timestamp('2019-11-07 17:10:31.058000 UTC'),
            'acceleration_x': [
                -31744, -31232, -31232, -30720, -32000, -31232, -30720, -31488,
                -31232, -31744, -30976, -30976, -31232, -30976, -31232, -31744,
                -32000, -31232, -32256, -31232, -31488, -31744, -30720, -32000,
                -31488, -31488, -31232, -31744, -31744, -32000, -30464, -31488,
                -32000, -31744, -31232, -31488, -31488, -31488, -31232, -32000,
                -31744, -31744, -32000, -30976, -31232, -31232, -31232, -31488,
                -31232, -30976, -30464, -31232, -31488, -31488, -30720, -30720,
                -30720, -31488, -31744, -32000, -31488, -31488, -31488, -31488,
                -30976, -31232, -30976, -31488, -31488, -32512, -31488, -30720,
                -30976, -30976, -31232
            ],
            'sampling_rate':
                50,
            'sensor_id':
                '',
            'true_timestamp_millis':
                None
        }, {
            'measurement_timestamp_utc':
                pd.Timestamp('2019-11-07 17:10:32.583000 UTC'),
            'acceleration_x': [-30720, -30464, -31744, -30976, -31488],
            'sampling_rate':
                50,
            'sensor_id':
                '',
            'true_timestamp_millis':
                None
        }, {
            'measurement_timestamp_utc':
                pd.Timestamp('2019-11-07 17:10:32.683000 UTC'),
            'acceleration_x': [
                -32512, -32000, -31488, -30976, -31232, -30720, -30976, -31232,
                -31488, -31232, -31232, -31744, -32256, -31488, -31488, -31232,
                -30208, -31232, -31488, -30976, -31232, -31744, -31488, -30976,
                -31744, -31488, -30976, -30976, -30976, -31488, -31488, -31744,
                -30976, -30208, -32256, -31488, -30720, -31232, -31744, -31232,
                -30976, -30976, -30976, -31232, -30720, -30976, -30464, -31232,
                -31232, -31232, -31488, -30976, -31744, -30976, -31488, -31232,
                -31744, -31232, -30976, -31232, -31488, -31488, -30720, -30976,
                -30720, -30464, -31232, -30464, -30976, -32256
            ],
            'sampling_rate':
                50,
            'sensor_id':
                '',
            'true_timestamp_millis':
                None
        }])

        df_out: pd.DataFrame = ds_unpack.unpack_data_frame(
            df, cols_to_unpack=['acceleration_x'], fall_back_to_legacy=True)

        self.assertEqual(df_out.shape[0], 0)


class LegacyUnpackDataTest(unittest.TestCase):

    def test_estimated_sampling_frequency(self):
        data_points = [
            create_ppg_data_point(100, '2022-01-01 00:00:00', 10, 100, 1,
                                  '2022-01-01 00:00:00'),
            create_ppg_data_point(100, '2022-01-01 00:00:01', 10, 100, 1,
                                  '2022-01-01 00:00:01'),
            create_ppg_data_point(100, '2022-01-01 00:00:02', 10, 100, 1,
                                  '2022-01-01 00:00:02'),
            create_ppg_data_point(100, '2022-01-01 00:00:03', 10, 100, 1,
                                  '2022-01-01 00:00:03')
        ]
        expected_fs = [100.0]
        with TestPipeline() as p:
            output = (p | beam.Create(data_points) |
                      BuildDataPointDataFrames.PerParticipantDevice() |
                      beam.Map(ds_unpack.unpack_data_frame,
                               cols_to_unpack=['green'],
                               fall_back_to_legacy=True,
                               use_legacy=True) |
                      beam.Map(estimate_sampling_frequency))
            assert_that(output, equal_to(expected_fs))

    def test_length_unpacked_data(self):
        data_points = [
            create_ppg_data_point(100, '2022-01-01 00:00:00', 10, 100, 1,
                                  '2022-01-01 00:00:00'),
            create_ppg_data_point(100, '2022-01-01 00:00:01', 10, 100, 1,
                                  '2022-01-01 00:00:01'),
            create_ppg_data_point(100, '2022-01-01 00:00:02', 10, 100, 1,
                                  '2022-01-01 00:00:02'),
            create_ppg_data_point(100, '2022-01-01 00:00:03', 10, 100, 1,
                                  '2022-01-01 00:00:03')
        ]
        expected_len = [100.0 * len(data_points)]
        with TestPipeline() as p:
            output = (p | beam.Create(data_points) |
                      BuildDataPointDataFrames.PerParticipantDevice() |
                      beam.Map(ds_unpack.unpack_data_frame,
                               cols_to_unpack=['green'],
                               fall_back_to_legacy=True,
                               use_legacy=True) | beam.Map(len))
            assert_that(output, equal_to(expected_len))

    def test_empty_data_frame(self):
        data_points = [
            create_ppg_data_point(0, '2022-01-01 00:00:00', 10, 100, 1,
                                  '2022-01-01 00:00:00'),
        ]
        expected_len = [0]
        with TestPipeline() as p:
            output = (p | beam.Create(data_points) |
                      BuildDataPointDataFrames.PerParticipantDevice() |
                      beam.Map(ds_unpack.unpack_data_frame,
                               cols_to_unpack=['green'],
                               fall_back_to_legacy=True,
                               use_legacy=True) | beam.Map(len))
            assert_that(output, equal_to(expected_len))

    def test_overlapping_samples(self):
        # The default is to drop overlapping samples.
        data_points = [
            create_ppg_data_point(
                100,
                '2022-01-01 00:00:00',
                10,
                100,
                1,
                '2022-01-01 00:00:00',
            ),
            create_ppg_data_point(  # 50% overlap with previous data point.
                100,
                '2022-01-01 00:00:00.5',
                10,
                100,
                1,
                '2022-01-01 00:00:00.5',
            ),
        ]
        expected_len = [150]
        with TestPipeline() as p:
            output = (p | beam.Create(data_points) |
                      BuildDataPointDataFrames.PerParticipantDevice() |
                      beam.Map(ds_unpack.unpack_data_frame,
                               cols_to_unpack=['green'],
                               fall_back_to_legacy=True,
                               use_legacy=True) | beam.Map(len))
            assert_that(output, equal_to(expected_len))


if __name__ == '__main__':
    unittest.main()
