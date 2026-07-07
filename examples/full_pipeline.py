"""Complete end-to-end pipeline example on Dataflow.

This example demonstrates:
1. Setting up Dataflow configuration
2. Reading large datasets
3. Unpacking IMU data with UnpackImu
4. Building DataFrames and computing features
5. Writing results to BigQuery

Set GOOGLE_PROJECT, BQ_DATASET, WORKSPACE_BUCKET, and DATAFLOW_REGION
env vars to run against real data on Dataflow.
Without GOOGLE_PROJECT, the example runs a synthetic-data demo locally.
"""

import os

import apache_beam as beam
import pandas as pd
from apache_beam.utils.timestamp import Timestamp

from verily.raw_data_tools import RawDataIO, apply_to_dataframe, DataflowOptions
from verily.raw_data_tools.schemas.schemas.shared_schemas import (
    DataPoint, DataPointMetadata, _STATE_KEY)
from verily.raw_data_tools.transforms.key_by import KeyDataPointsBy
from verily.raw_data_tools.transforms.group_into_data_frames import (
    GroupIntoDataFrames)
from verily.raw_data_tools.unpacking import UnpackImu
from verily.raw_data_tools.transforms import BuildDataFrames, KeyBy


PROJECT = os.getenv('GOOGLE_PROJECT')
DATASET = os.getenv('BQ_DATASET', 'my_sensor_dataset')
BUCKET = os.getenv('WORKSPACE_BUCKET', 'my-workspace-bucket')
REGION = os.getenv('DATAFLOW_REGION', 'us-central1')


def compute_activity_features(df: pd.DataFrame) -> pd.DataFrame:
    """Compute acceleration features from IMU data."""
    if all(col in df.columns for col in ['accel_x', 'accel_y', 'accel_z']):
        df['accel_magnitude'] = (
            df['accel_x'] ** 2 + df['accel_y'] ** 2 + df['accel_z'] ** 2
        ) ** 0.5
        df['accel_mean'] = df['accel_magnitude'].rolling(window=100).mean()
        df['accel_std'] = df['accel_magnitude'].rolling(window=100).std()
    return df


def format_for_bigquery(kv):
    """Yield BigQuery-compatible rows from a keyed DataFrame."""
    key, df = kv
    for _, row in df.iterrows():
        yield {
            'key': str(key),
            'timestamp': int(row.get('timestamp', 0)),
            'accel_magnitude': float(row.get('accel_magnitude', 0)),
            'accel_mean': float(row.get('accel_mean', 0)),
            'accel_std': float(row.get('accel_std', 0)),
        }


def main():
    if PROJECT:
        dataflow_opts = DataflowOptions(
            job_name='activity-features-pipeline',
            temp_location=f'gs://{BUCKET}/dataflow/temp',
            staging_location=f'gs://{BUCKET}/dataflow/staging',
            region=REGION,
            num_workers=10,
            max_num_workers=50,
            machine_type='n1-standard-4',
            disk_size_gb=100,
            use_public_ips=False,
        )

        io = RawDataIO(
            project=PROJECT,
            dataset=DATASET,
            runner='DataflowRunner',
            dataflow_options=dataflow_opts,
        )

        pipeline = io.create_pipeline(name='ActivityFeaturePipeline')
        imu_data = pipeline | 'Read IMU' >> io.read_datapoints(
            data_types=['IMU'],
            start_time='2024-01-01',
            end_time='2024-01-31',
        )
        unpacked = imu_data | 'Unpack' >> UnpackImu()
        keyed = unpacked | 'Key by Device' >> KeyBy(key_field='DeviceID')
        dataframes = keyed | 'Build DataFrames' >> BuildDataFrames()
        with_features = dataframes | 'Features' >> apply_to_dataframe(
            compute_activity_features)
        bq_rows = with_features | 'Format' >> beam.FlatMap(format_for_bigquery)

        output_table = f'{PROJECT}:{DATASET}.activity_features'
        schema = {
            'fields': [
                {'name': 'key', 'type': 'STRING'},
                {'name': 'timestamp', 'type': 'INTEGER'},
                {'name': 'accel_magnitude', 'type': 'FLOAT'},
                {'name': 'accel_mean', 'type': 'FLOAT'},
                {'name': 'accel_std', 'type': 'FLOAT'},
            ]
        }
        bq_rows | 'Write to BQ' >> beam.io.WriteToBigQuery(
            output_table,
            schema=schema,
            create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED,
            write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND,
        )

        result = pipeline.run()
        result.wait_until_finish()
    else:
        print('No GOOGLE_PROJECT set — running synthetic demo locally.')
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
                | 'Features' >> beam.Map(compute_activity_features)
            )
            output | 'Print' >> beam.Map(
                lambda df: print(f'DataFrame shape: {df.shape}'))
        print('Synthetic demo complete.')


if __name__ == '__main__':
    main()
