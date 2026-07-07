"""Example of integrating a custom algorithm into the pipeline.

This example demonstrates:
1. Defining a custom heart rate detection algorithm
2. Applying it with apply_to_dataframe
3. Processing PPG sensor data end-to-end

Set GOOGLE_PROJECT and BQ_DATASET env vars to run against real data.
Without them, the example runs a synthetic-data demo.
"""

import os

import apache_beam as beam
import numpy as np
import pandas as pd
from apache_beam.utils.timestamp import Timestamp

from verily.raw_data_tools import RawDataIO, apply_to_dataframe
from verily.raw_data_tools.schemas.schemas.shared_schemas import (
    DataPoint, DataPointMetadata, _STATE_KEY)
from verily.raw_data_tools.transforms.key_by import KeyDataPointsBy
from verily.raw_data_tools.transforms.group_into_data_frames import (
    GroupIntoDataFrames)
from verily.raw_data_tools.unpacking import UnpackPpg
from verily.raw_data_tools.transforms import BuildDataFrames, KeyBy


PROJECT = os.getenv('GOOGLE_PROJECT')
DATASET = os.getenv('BQ_DATASET', 'my_sensor_dataset')


def detect_heart_rate(df: pd.DataFrame) -> pd.DataFrame:
    """Simplified heart rate detection from PPG signal."""
    if 'ppg_value' not in df.columns or len(df) < 100:
        df['heart_rate'] = np.nan
        return df

    signal = df['ppg_value'].values
    peaks = []
    for i in range(1, len(signal) - 1):
        if signal[i] > signal[i - 1] and signal[i] > signal[i + 1]:
            if signal[i] > np.median(signal):
                peaks.append(i)

    if len(peaks) >= 2:
        avg_interval = np.mean(np.diff(peaks)) / 50.0
        heart_rate = 60.0 / avg_interval
        df['heart_rate'] = heart_rate if 40 <= heart_rate <= 200 else np.nan
    else:
        df['heart_rate'] = np.nan

    return df


def main():
    if PROJECT:
        io = RawDataIO(
            project=PROJECT,
            dataset=DATASET,
            runner='DirectRunner',
        )
        pipeline = io.create_pipeline(name='HeartRateDetection')
        ppg_data = pipeline | 'Read PPG' >> io.read_datapoints(
            data_types=['PPG'],
            start_time='2024-01-01',
            end_time='2024-01-02',
            limit=50,
        )
        unpacked = ppg_data | 'Unpack' >> UnpackPpg()
        keyed = unpacked | 'Key by Device' >> KeyBy(key_field='DeviceID')
        dataframes = keyed | 'Build DataFrames' >> BuildDataFrames()
        with_heart_rate = dataframes | 'Detect HR' >> apply_to_dataframe(
            detect_heart_rate)

        def print_hr(kv):
            key, df = kv
            if 'heart_rate' in df.columns and not df['heart_rate'].isna().all():
                print(f'Key {key}: HR = {df["heart_rate"].iloc[0]:.1f} bpm')

        with_heart_rate | 'Print' >> beam.Map(print_hr)
        result = pipeline.run()
        result.wait_until_finish()
    else:
        print('No GOOGLE_PROJECT set — running synthetic demo.')
        metadata = DataPointMetadata(
            data_source_id=1,
            device_id='dev-syn-001',
            participant_id='part-001',
            participant_namespace=1,
            echo_metadata=None,
            sensor_store_metadata=None,
            annotation_labels=set(),
            _state_key=_STATE_KEY.CREATED_USING_BUILDER)
        ts = Timestamp.from_utc_datetime(
            pd.Timestamp('2024-01-01 12:00:00', tz='UTC'))
        data_points = [
            DataPoint(data_point_metadata=metadata,
                      measurement_timestamp_utc=ts),
        ]

        with beam.Pipeline() as p:
            output = (
                p
                | beam.Create(data_points)
                | KeyDataPointsBy(by_device=True, by_participant=True)
                | GroupIntoDataFrames()
                | 'Detect HR' >> beam.Map(detect_heart_rate)
            )
            output | 'Print' >> beam.Map(print)
        print('Synthetic demo complete.')


if __name__ == '__main__':
    main()
