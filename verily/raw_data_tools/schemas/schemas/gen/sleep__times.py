# Lint as: python3
# pylint: disable=invalid-name
"""Registers the schema for com.verily.sleep_times with Beam."""

import dataclasses
from typing import Optional

from verily.raw_data_tools.schemas.schemas.shared_schemas import DataPoint
from verily.raw_data_tools.schemas.schemas.decorators import dataspec


@dataspec('com.verily.sleep_times')
@dataclasses.dataclass
class Sleep_Times(DataPoint):
    """Beam RowSchema for com.verily.sleep_times."""
    end_time: Optional[str] = None
    imu_ppg_availability: Optional[float] = None
    label: Optional[str] = None
    start_time: Optional[str] = None
