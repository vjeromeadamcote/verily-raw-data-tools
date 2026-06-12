"""Basic example of reading sensor data from BigQuery.

This example demonstrates:
1. Initializing RawDataIO
2. Reading DataPoints with filters
3. Running a simple pipeline
"""

import apache_beam as beam
from verily.raw_data_tools import RawDataIO


def main():
    # Initialize I/O for your Workbench project
    io = RawDataIO(
        project='my-gcp-project',        # Your GCP project
        dataset='my_sensor_dataset',     # Your BigQuery dataset
        runner='DirectRunner'             # Local execution
    )

    # Create a pipeline
    pipeline = io.create_pipeline(name='BasicDataRead')

    # Read DataPoints from BigQuery with filters
    data = pipeline | 'Read Data' >> io.read_datapoints(
        device_ids=['device_001', 'device_002'],  # Filter by devices
        start_time='2024-01-01',                   # Start date
        end_time='2024-01-31',                     # End date
        data_types=['IMU', 'PPG'],                 # Sensor types
        limit=1000                                 # Limit results for testing
    )

    # Print the data (for testing)
    data | 'Print' >> beam.Map(print)

    # Run the pipeline
    result = pipeline.run()
    result.wait_until_finish()
    print("Pipeline completed successfully!")


if __name__ == '__main__':
    main()
