# Lint as: python3
# pylint: disable=invalid-name
"""Registers the schema for com.verily.imu with Beam."""

import dataclasses
from typing import List, Optional

from apache_beam.utils.timestamp import Timestamp

from verily.ds_sdk.core.schemas import DataPoint
from verily.ds_sdk.core.schemas import dataspec


@dataspec('com.verily.imu')
@dataclasses.dataclass
class Imu(DataPoint):
    """Beam RowSchema for com.verily.imu."""
    acceleration_x: Optional[List[int]] = None
    acceleration_y: Optional[List[int]] = None
    acceleration_z: Optional[List[int]] = None
    gyro_x: Optional[List[int]] = None
    gyro_y: Optional[List[int]] = None
    gyro_z: Optional[List[int]] = None
    sampling_rate: Optional[int] = None
    sensor_id: Optional[int] = None
    true_timestamp_millis: Optional[Timestamp] = None
    true_timestamp_sample_index: Optional[int] = None
