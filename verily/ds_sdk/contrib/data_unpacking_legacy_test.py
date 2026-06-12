"""Tests for data_unpacking_legacy.py"""
from typing import Any, Dict
import unittest

import apache_beam as beam
from apache_beam.testing.test_pipeline import TestPipeline
from apache_beam.testing.util import assert_that
from apache_beam.testing.util import equal_to
import numpy as np
import pandas as pd

import verily.ds_sdk.contrib.data_unpacking_legacy as ds_unpack_legacy
from verily.ds_sdk.core import schemas
from verily.ds_sdk.core.transforms import BuildDataPointDataFrames
from verily.ds_sdk.core.utils import timestamps


def create_ppg_data_point(ppg_len: int, measurement_timestamp: str,
                          true_timestamp: str, true_timestamp_sample_index: int,
                          nominal_sampling_rate: int,
                          sensor_id: int) -> schemas.Ppg:
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
            pd.Timestamp(measurement_timestamp)),
        green=list(np.arange(ppg_len)),
        true_timestamp_millis=timestamps.datetime_to_beam_timestamp(
            pd.Timestamp(true_timestamp)),
        true_timestamp_sample_index=true_timestamp_sample_index,
        sampling_rate=nominal_sampling_rate)


def create_ppg_data_point_dict(ppg_len: int, measurement_timestamp: str,
                               true_timestamp: str,
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
        'measurement_timestamp_utc': pd.Timestamp(measurement_timestamp),
        'green': np.arange(ppg_len),
        'true_timestamp_millis': pd.Timestamp(true_timestamp),
        'true_timestamp_sample_index': true_timestamp_sample_index,
        'sampling_rate': nominal_sampling_rate
    }


def estimate_sampling_frequency(sensor_df: pd.DataFrame) -> float:
    """Function to estimate sampling frequency from unpacked df"""
    return 1000.0 / sensor_df['timestamp_ms'].diff()[1]


class UnpackDataTest(unittest.TestCase):

    def test_estimated_sampling_frequency(self):
        data_points = [
            create_ppg_data_point(100, '2022-01-01 00:00:00',
                                  '2022-01-01 00:00:00', np.nan, 100, 1),
            create_ppg_data_point(100, '2022-01-01 00:00:01',
                                  '2022-01-01 00:00:01', np.nan, 100, 1),
            create_ppg_data_point(100, '2022-01-01 00:00:02',
                                  '2022-01-01 00:00:02', np.nan, 100, 1),
            create_ppg_data_point(100, '2022-01-01 00:00:03',
                                  '2022-01-01 00:00:03', np.nan, 100, 1)
        ]
        expected_fs = [100.0]
        with TestPipeline() as p:
            output = (p | beam.Create(data_points) |
                      BuildDataPointDataFrames.PerParticipantDevice() |
                      beam.Map(ds_unpack_legacy.unpack_data_frame,
                               cols_to_unpack=['green']) |
                      beam.Map(estimate_sampling_frequency))
            assert_that(output, equal_to(expected_fs))

    def test_length_unpacked_data(self):
        data_points = [
            create_ppg_data_point(100, '2022-01-01 00:00:00',
                                  '2022-01-01 00:00:00', np.nan, 100, 1),
            create_ppg_data_point(100, '2022-01-01 00:00:01',
                                  '2022-01-01 00:00:01', np.nan, 100, 1),
            create_ppg_data_point(100, '2022-01-01 00:00:02',
                                  '2022-01-01 00:00:02', np.nan, 100, 1),
            create_ppg_data_point(100, '2022-01-01 00:00:03',
                                  '2022-01-01 00:00:03', np.nan, 100, 1)
        ]
        expected_len = [100.0 * len(data_points)]
        with TestPipeline() as p:
            output = (p | beam.Create(data_points) |
                      BuildDataPointDataFrames.PerParticipantDevice() |
                      beam.Map(ds_unpack_legacy.unpack_data_frame,
                               cols_to_unpack=['green']) | beam.Map(len))
            assert_that(output, equal_to(expected_len))

    def test_empty_data_frame(self):
        data_points = [
            create_ppg_data_point(0, '2022-01-01 00:00:00',
                                  '2022-01-01 00:00:00', np.nan, 100, 1),
        ]
        expected_len = [0]
        with TestPipeline() as p:
            output = (p | beam.Create(data_points) |
                      BuildDataPointDataFrames.PerParticipantDevice() |
                      beam.Map(ds_unpack_legacy.unpack_data_frame,
                               cols_to_unpack=['green']) | beam.Map(len))
            assert_that(output, equal_to(expected_len))

    def test_overlapping_samples(self):
        # The default is to drop overlapping samples.
        data_points = [
            create_ppg_data_point(
                100,
                '2022-01-01 00:00:00',
                '2022-01-01 00:00:00',
                np.nan,
                100,
                1,
            ),
            create_ppg_data_point(  # 50% overlap with previous data point.
                100,
                '2022-01-01 00:00:00.5',
                '2022-01-01 00:00:00.5',
                np.nan,
                100,
                1,
            ),
        ]
        expected_len = [150]
        with TestPipeline() as p:
            output = (p | beam.Create(data_points) |
                      BuildDataPointDataFrames.PerParticipantDevice() |
                      beam.Map(ds_unpack_legacy.unpack_data_frame,
                               cols_to_unpack=['green'],
                               drop_overlapping_samples=True) | beam.Map(len))
            assert_that(output, equal_to(expected_len))

    def test_overlapping_samples_do_not_drop(self):
        data_points = [
            create_ppg_data_point(
                100,
                '2022-01-01 00:00:00',
                '2022-01-01 00:00:00',
                np.nan,
                100,
                1,
            ),
            create_ppg_data_point(  # 50% overlap with previous data point.
                100,
                '2022-01-01 00:00:00.5',
                '2022-01-01 00:00:00.5',
                np.nan,
                100,
                1,
            ),
        ]
        expected_len = [200]
        with TestPipeline() as p:
            output = (p | beam.Create(data_points) |
                      BuildDataPointDataFrames.PerParticipantDevice() |
                      beam.Map(ds_unpack_legacy.unpack_data_frame,
                               cols_to_unpack=['green'],
                               drop_overlapping_samples=False) | beam.Map(len))
            assert_that(output, equal_to(expected_len))


if __name__ == '__main__':
    unittest.main()
