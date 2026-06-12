"""Verily Raw Data Tools - SDK for processing sensor data in BigQuery.

This package provides tools for reading, unpacking, transforming, and analyzing
sensor data stored in BigQuery, designed for use in Verily Workbench.

Core Capabilities:
    1. Data Read: Query BigQuery, filter, deduplicate, and join sensor data
    2. Unpacking: Unpack compressed sensor data into time series
    3. Transform: Convert DataPoints to Pandas DataFrames
    4. Segmentation: Key data by device ID, participant ID, time windows
    5. Custom Algorithms: Integration point for 3rd-party analysis functions
    6. Pipeline Building: Tools for building and launching Dataflow pipelines

Example:
    >>> from verily.raw_data_tools import RawDataIO
    >>> from verily.raw_data_tools.unpacking import DataUnpacker
    >>>
    >>> # Initialize I/O
    >>> io = RawDataIO(
    ...     project='my-project',
    ...     dataset='my_dataset',
    ...     runner='DataflowRunner'
    ... )
    >>>
    >>> # Read and process data
    >>> pipeline = io.create_pipeline()
    >>> data = pipeline | io.read_datapoints(
    ...     data_types=['IMU', 'PPG'],
    ...     start_time='2024-01-01',
    ...     end_time='2024-01-31'
    ... )
    >>> unpacked = data | DataUnpacker()
"""

__version__ = '1.0.0'

# Core I/O
from verily.raw_data_tools.io.raw_data_io import RawDataIO

# Unpacking
from verily.raw_data_tools.unpacking.data_unpacking import DataUnpacker

# Transforms
from verily.raw_data_tools.transforms.build_data_frames import BuildDataFrames
from verily.raw_data_tools.transforms.key_by import KeyBy
from verily.raw_data_tools.transforms.custom_transform import (
    MapWithCustomFunction,
    apply_to_dataframe,
    apply_algorithm,
)

# Pipeline utilities
from verily.raw_data_tools.pipeline.options import DataflowOptions

__all__ = [
    'RawDataIO',
    'DataUnpacker',
    'BuildDataFrames',
    'KeyBy',
    'MapWithCustomFunction',
    'apply_to_dataframe',
    'apply_algorithm',
    'DataflowOptions',
]
