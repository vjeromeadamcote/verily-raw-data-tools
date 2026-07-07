"""Example of unpacking sensor data and transforming to DataFrames.

This example demonstrates:
1. Reading compressed sensor data
2. Unpacking IMU into time series with UnpackImu
3. Keying by participant + device
4. Building Pandas DataFrames

Set GOOGLE_PROJECT and BQ_DATASET env vars to run against real data.
Without them, the example runs a synthetic-data demo using the
KeyDataPointsBy / GroupIntoDataFrames transforms directly.
"""

import os

import apache_beam as beam
from apache_beam.utils.timestamp import Timestamp
import pandas as pd

from verily.raw_data_tools import RawDataIO
from verily.raw_data_tools.schemas.schemas.shared_schemas import (
    DataPoint, DataPointMetadata, _STATE_KEY)
from verily.raw_data_tools.transforms.key_by import KeyDataPointsBy
from verily.raw_data_tools.transforms.group_into_data_frames import (
    GroupIntoDataFrames)
from verily.raw_data_tools.unpacking import UnpackImu
from verily.raw_data_tools.transforms import BuildDataFrames, KeyBy


PROJECT = os.getenv('GOOGLE_PROJECT')
DATASET = os.getenv('BQ_DATASET', 'my_sensor_dataset')


def print_dataframe_info(kv):
    key, df = kv
    print(f'\nKey: {key}')
    print(f'DataFrame shape: {df.shape}')
    print(f'Columns: {df.columns.tolist()}')
    print(df.head())
    return kv


def main():
    if PROJECT:
        io = RawDataIO(
            project=PROJECT,
            dataset=DATASET,
            runner='DirectRunner',
        )
        pipeline = io.create_pipeline(name='UnpackAndTransform')
        compressed_data = pipeline | 'Read IMU' >> io.read_datapoints(
            data_types=['IMU'],
            start_time='2024-01-01',
            end_time='2024-01-02',
            limit=100,
        )
        unpacked = compressed_data | 'Unpack' >> UnpackImu()
        keyed = unpacked | 'Key by Device' >> KeyBy(key_field='DeviceID')
        dataframes = keyed | 'Build DataFrames' >> BuildDataFrames()
        dataframes | 'Print Info' >> beam.Map(print_dataframe_info)

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
            )
            output | 'Print Info' >> beam.Map(print_dataframe_info)
        print('Synthetic demo complete.')


if __name__ == '__main__':
    main()
