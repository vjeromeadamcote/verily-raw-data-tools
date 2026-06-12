"""Complete end-to-end pipeline example on Dataflow.

This example demonstrates:
1. Setting up Dataflow configuration
2. Reading large datasets
3. Unpacking and transformation
4. Custom processing
5. Writing results to BigQuery
6. Monitoring the pipeline
"""

import os
import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions
from verily.raw_data_tools import RawDataIO, DataUnpacker, apply_to_dataframe
from verily.raw_data_tools.transforms import BuildDataFrames, KeyBy
from verily.raw_data_tools.pipeline import DataflowOptions
import pandas as pd


def compute_activity_features(df: pd.DataFrame) -> pd.DataFrame:
    """Compute activity features from IMU data.

    Args:
        df: DataFrame with IMU sensor data (accel_x, accel_y, accel_z)

    Returns:
        DataFrame with computed features
    """
    # Calculate acceleration magnitude
    if all(col in df.columns for col in ['accel_x', 'accel_y', 'accel_z']):
        df['accel_magnitude'] = (
            df['accel_x']**2 + df['accel_y']**2 + df['accel_z']**2
        ) ** 0.5

        # Calculate statistics over windows
        df['accel_mean'] = df['accel_magnitude'].rolling(window=100).mean()
        df['accel_std'] = df['accel_magnitude'].rolling(window=100).std()
        df['accel_max'] = df['accel_magnitude'].rolling(window=100).max()

    return df


def format_for_bigquery(kv):
    """Format DataFrame as BigQuery rows.

    Args:
        kv: Tuple of (key, DataFrame)

    Yields:
        Dictionary rows for BigQuery
    """
    device_id, df = kv

    # Convert DataFrame to BigQuery-compatible rows
    for _, row in df.iterrows():
        yield {
            'device_id': device_id,
            'timestamp': int(row.get('timestamp', 0)),
            'accel_magnitude': float(row.get('accel_magnitude', 0)),
            'accel_mean': float(row.get('accel_mean', 0)),
            'accel_std': float(row.get('accel_std', 0)),
            'accel_max': float(row.get('accel_max', 0)),
        }


def main():
    """Run the complete pipeline on Dataflow."""

    # Configuration
    PROJECT = os.getenv('GOOGLE_PROJECT', 'my-gcp-project')
    DATASET = os.getenv('BQ_DATASET', 'my_sensor_dataset')
    BUCKET = os.getenv('WORKSPACE_BUCKET', 'my-workspace-bucket')
    REGION = os.getenv('DATAFLOW_REGION', 'us-central1')

    # Configure Dataflow
    dataflow_opts = DataflowOptions(
        job_name='activity-features-pipeline',
        temp_location=f'gs://{BUCKET}/dataflow/temp',
        staging_location=f'gs://{BUCKET}/dataflow/staging',
        region=REGION,

        # Worker configuration
        num_workers=10,
        max_num_workers=50,
        machine_type='n1-standard-4',
        disk_size_gb=100,

        # Network configuration (for Workbench)
        use_public_ips=False,
        additional_options={
            'subnetwork': f'regions/{REGION}/subnetworks/workbench-subnet',
            'experiments': ['use_runner_v2'],
        }
    )

    # Initialize I/O
    io = RawDataIO(
        project=PROJECT,
        dataset=DATASET,
        runner='DataflowRunner',
        dataflow_options=dataflow_opts
    )

    # Create pipeline
    pipeline = io.create_pipeline(name='ActivityFeaturePipeline')

    # Step 1: Read IMU data for the past month
    imu_data = pipeline | 'Read IMU Data' >> io.read_datapoints(
        data_types=['IMU'],
        start_time='2024-01-01',
        end_time='2024-01-31'
    )

    # Step 2: Unpack compressed sensor data
    unpacker = DataUnpacker(error_thresh=0.05)
    unpacked = imu_data | 'Unpack IMU' >> beam.ParDo(unpacker)

    # Step 3: Key by device for parallel processing
    by_device = unpacked | 'Key by Device' >> KeyBy(key_field='DeviceID')

    # Step 4: Build DataFrames
    dataframes = by_device | 'Build DataFrames' >> BuildDataFrames(
        include_metadata=True,
        sort_by_time=True
    )

    # Step 5: Compute activity features
    with_features = dataframes | 'Compute Features' >> apply_to_dataframe(
        compute_activity_features
    )

    # Step 6: Format for BigQuery
    bq_rows = with_features | 'Format for BQ' >> beam.FlatMap(format_for_bigquery)

    # Step 7: Write to BigQuery
    output_table = f'{PROJECT}:{DATASET}.activity_features'
    schema = {
        'fields': [
            {'name': 'device_id', 'type': 'STRING'},
            {'name': 'timestamp', 'type': 'INTEGER'},
            {'name': 'accel_magnitude', 'type': 'FLOAT'},
            {'name': 'accel_mean', 'type': 'FLOAT'},
            {'name': 'accel_std', 'type': 'FLOAT'},
            {'name': 'accel_max', 'type': 'FLOAT'},
        ]
    }

    bq_rows | 'Write to BigQuery' >> beam.io.WriteToBigQuery(
        output_table,
        schema=schema,
        create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED,
        write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND
    )

    # Launch the pipeline
    print(f"Launching Dataflow job in project: {PROJECT}")
    print(f"Region: {REGION}")
    print(f"Job name: activity-features-pipeline")

    result = pipeline.run()

    # Print monitoring URL
    print(f"\nDataflow job started!")
    print(f"Monitor at: https://console.cloud.google.com/dataflow/jobs/{REGION}/{result.job_id()}")
    print(f"\nWaiting for job to complete...")

    # Wait for completion (optional - remove for async execution)
    result.wait_until_finish()

    print("\nPipeline completed successfully!")
    print(f"Results written to: {output_table}")


if __name__ == '__main__':
    main()
