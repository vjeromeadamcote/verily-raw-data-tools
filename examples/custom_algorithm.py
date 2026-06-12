"""Example of integrating a custom algorithm into the pipeline.

This example demonstrates:
1. Defining a custom heart rate detection algorithm
2. Integrating it using apply_to_dataframe
3. Processing PPG sensor data
4. Writing results back to BigQuery
"""

import apache_beam as beam
import pandas as pd
import numpy as np
from verily.raw_data_tools import RawDataIO, DataUnpacker, apply_to_dataframe
from verily.raw_data_tools.transforms import BuildDataFrames, KeyBy


def detect_heart_rate(df: pd.DataFrame) -> pd.DataFrame:
    """Custom algorithm to detect heart rate from PPG signal.

    This is a simplified example. Real heart rate detection would be more
    sophisticated, using signal processing techniques like FFT, peak detection,
    or machine learning models.

    Args:
        df: DataFrame with PPG sensor data

    Returns:
        DataFrame with added 'heart_rate' column
    """
    if 'ppg_value' not in df.columns or len(df) < 100:
        # Not enough data or wrong format
        df['heart_rate'] = np.nan
        return df

    # Simple peak detection (for demonstration only)
    signal = df['ppg_value'].values

    # Find peaks (local maxima)
    peaks = []
    for i in range(1, len(signal) - 1):
        if signal[i] > signal[i-1] and signal[i] > signal[i+1]:
            # Threshold to avoid noise
            if signal[i] > np.median(signal):
                peaks.append(i)

    # Calculate heart rate from peak intervals
    if len(peaks) >= 2:
        # Time between peaks (assuming ~50Hz sampling rate)
        avg_interval = np.mean(np.diff(peaks)) / 50.0  # seconds
        heart_rate = 60.0 / avg_interval  # beats per minute

        # Sanity check (typical range: 40-200 bpm)
        if 40 <= heart_rate <= 200:
            df['heart_rate'] = heart_rate
        else:
            df['heart_rate'] = np.nan
    else:
        df['heart_rate'] = np.nan

    return df


def filter_valid_results(kv):
    """Filter out results without valid heart rate."""
    device_id, df = kv
    if 'heart_rate' in df.columns and not df['heart_rate'].isna().all():
        return True
    return False


def main():
    # Initialize I/O
    io = RawDataIO(
        project='my-gcp-project',
        dataset='my_sensor_dataset',
        runner='DirectRunner'
    )

    # Create pipeline
    pipeline = io.create_pipeline(name='HeartRateDetection')

    # Read PPG data
    ppg_data = pipeline | 'Read PPG' >> io.read_datapoints(
        data_types=['PPG'],
        start_time='2024-01-01',
        end_time='2024-01-02',
        limit=50
    )

    # Unpack sensor data
    unpacker = DataUnpacker()
    unpacked = ppg_data | 'Unpack' >> beam.ParDo(unpacker)

    # Group by device
    by_device = unpacked | 'Key by Device' >> KeyBy(key_field='DeviceID')

    # Build DataFrames
    dataframes = by_device | 'Build DataFrames' >> BuildDataFrames()

    # Apply custom heart rate detection algorithm
    with_heart_rate = dataframes | 'Detect Heart Rate' >> apply_to_dataframe(
        detect_heart_rate
    )

    # Filter to only valid results
    valid_results = with_heart_rate | 'Filter Valid' >> beam.Filter(
        filter_valid_results
    )

    # Print results
    def print_heart_rate(kv):
        device_id, df = kv
        hr = df['heart_rate'].iloc[0]
        print(f"Device {device_id}: Heart Rate = {hr:.1f} bpm")
        return kv

    valid_results | 'Print Results' >> beam.Map(print_heart_rate)

    # Run pipeline
    result = pipeline.run()
    result.wait_until_finish()
    print("\nHeart rate detection completed!")


if __name__ == '__main__':
    main()
