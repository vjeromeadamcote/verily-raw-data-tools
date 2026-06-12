"""Transform modules for data segmentation and DataFrame building.

This module provides Apache Beam transforms for:
- Keying data by device ID, participant ID, or time windows
- Grouping DataPoints into DataFrames
- Building structured DataFrames from sensor data
- Integrating custom 3rd-party algorithms
"""

from verily.raw_data_tools.transforms.key_by import KeyBy
from verily.raw_data_tools.transforms.build_data_frames import BuildDataFrames
from verily.raw_data_tools.transforms.group_into_data_frames import GroupIntoDataFrames
from verily.raw_data_tools.transforms.custom_transform import (
    MapWithCustomFunction,
    FlatMapWithCustomFunction,
    FilterWithCustomPredicate,
    apply_to_dataframe,
    apply_algorithm,
)

__all__ = [
    'KeyBy',
    'BuildDataFrames',
    'GroupIntoDataFrames',
    'MapWithCustomFunction',
    'FlatMapWithCustomFunction',
    'FilterWithCustomPredicate',
    'apply_to_dataframe',
    'apply_algorithm',
]
