# Lint as: python3
# pylint: disable=invalid-name
"""Registers the schema for com.verily.myalo.gyroscope with Beam."""

import dataclasses
from typing import List, Optional

from verily.ds_sdk.core.schemas import DataPoint
from verily.ds_sdk.core.schemas import dataspec


@dataspec('com.verily.myalo.gyroscope')
@dataclasses.dataclass
class MyaloGyroscope(DataPoint):
    """Beam RowSchema for com.verily.myalo.gyroscope."""
    reported_time: Optional[int] = None
    sample_time_list: Optional[List[int]] = None
    sampling_rate_hz: Optional[float] = None
    x_axis_velocity_list: Optional[List[float]] = None
    y_axis_velocity_list: Optional[List[float]] = None
    z_axis_velocity_list: Optional[List[float]] = None
