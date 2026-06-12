"""Example of unpacking sensor data and transforming to DataFrames.

This example demonstrates:
1. Reading compressed sensor data
2. Unpacking into time series
3. Converting to Pandas DataFrames
4. Keying by device ID
"""

import apache_beam as beam
from verily.raw_data_tools import RawDataIO, DataUnpacker
from verily.raw_data_tools.transforms import BuildDataFrames, KeyBy


def main():
    # Initialize I/O
    io = RawDataIO(
        project='my-gcp-project',
        dataset='my_sensor_dataset',
        runner='DirectRunner'
    )

    # Create pipeline
    pipeline = io.create_pipeline(name='UnpackAndTransform')

    # Read compressed IMU data
    compressed_data = pipeline | 'Read IMU' >> io.read_datapoints(
        data_types=['IMU'],
        start_time='2024-01-01',
        end_time='2024-01-02',
        limit=100
    )

    # Unpack the compressed sensor data
    unpacker = DataUnpacker(
        error_thresh=0.05,           # 5% sampling rate error tolerance
        ignore_median_fs_error=False
    )
    unpacked_data = compressed_data | 'Unpack' >> beam.ParDo(unpacker)

    # Key data by device ID for parallel processing
    by_device = unpacked_data | 'Key by Device' >> KeyBy(key_field='DeviceID')

    # Convert to DataFrames
    dataframes = by_device | 'Build DataFrames' >> BuildDataFrames(
        include_metadata=True,
        sort_by_time=True
    )

    # Print results
    def print_dataframe_info(kv):
        device_id, df = kv
        print(f"\nDevice: {device_id}")
        print(f"DataFrame shape: {df.shape}")
        print(f"Columns: {df.columns.tolist()}")
        print(df.head())
        return kv

    dataframes | 'Print Info' >> beam.Map(print_dataframe_info)

    # Run pipeline
    result = pipeline.run()
    result.wait_until_finish()
    print("\nPipeline completed successfully!")


if __name__ == '__main__':
    main()
