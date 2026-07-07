"""Basic example of reading sensor data from BigQuery.

This example demonstrates:
1. Initializing RawDataIO
2. Reading DataPoints with filters
3. Running a simple pipeline

Set GOOGLE_PROJECT and BQ_DATASET env vars to run against real data.
Without them, the example runs a synthetic-data demo.
"""

import os

import apache_beam as beam
from verily.raw_data_tools import RawDataIO


PROJECT = os.getenv('GOOGLE_PROJECT')
DATASET = os.getenv('BQ_DATASET', 'my_sensor_dataset')


def main():
    if PROJECT:
        io = RawDataIO(
            project=PROJECT,
            dataset=DATASET,
            runner='DirectRunner',
        )

        pipeline = io.create_pipeline(name='BasicDataRead')

        data = pipeline | 'Read Data' >> io.read_datapoints(
            device_ids=['device_001', 'device_002'],
            start_time='2024-01-01',
            end_time='2024-01-31',
            data_types=['IMU', 'PPG'],
            limit=1000,
        )

        data | 'Print' >> beam.Map(print)

        result = pipeline.run()
        result.wait_until_finish()
    else:
        print('No GOOGLE_PROJECT set — running synthetic demo.')
        with beam.Pipeline() as p:
            rows = p | beam.Create([
                {'DeviceID': 'device_001', 'DataPointTime': 1704067200000,
                 'value': 42},
                {'DeviceID': 'device_002', 'DataPointTime': 1704067260000,
                 'value': 7},
            ])
            rows | 'Print' >> beam.Map(print)
        print('Synthetic demo complete.')


if __name__ == '__main__':
    main()
