# Lint as: python3
# pylint: disable=invalid-name
"""Registers the schema for com.verily.myalo.magnetometer with Beam."""

import dataclasses
from typing import List, Optional

from verily.ds_sdk.core.schemas import DataPoint
from verily.ds_sdk.core.schemas import dataspec


@dataspec('com.verily.myalo.magnetometer')
@dataclasses.dataclass
class MyaloMagnetometer(DataPoint):
    """Beam RowSchema for com.verily.myalo.magnetometer."""
    reported_time: Optional[int] = None
    sample_time_list: Optional[List[int]] = None
    sampling_rate_hz: Optional[float] = None
    x_magnetic_field_list: Optional[List[float]] = None
    y_magnetic_field_list: Optional[List[float]] = None
    z_magnetic_field_list: Optional[List[float]] = None
