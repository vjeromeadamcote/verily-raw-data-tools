# Lint as: python3
# pylint: disable=invalid-name
"""Registers the schema for com.verily.myalo.accelerometer with Beam."""

import dataclasses
from typing import List, Optional

from verily.raw_data_tools.schemas.schemas.shared_schemas import DataPoint
from verily.raw_data_tools.schemas.schemas.decorators import dataspec


@dataspec('com.verily.myalo.accelerometer')
@dataclasses.dataclass
class MyaloAccelerometer(DataPoint):
    """Beam RowSchema for com.verily.myalo.accelerometer."""
    reported_time: Optional[int] = None
    sample_time_list: Optional[List[int]] = None
    sampling_rate_hz: Optional[float] = None
    x_acceleration_list: Optional[List[float]] = None
    y_acceleration_list: Optional[List[float]] = None
    z_acceleration_list: Optional[List[float]] = None
