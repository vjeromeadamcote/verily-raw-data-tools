# Lint as: python3
# pylint: disable=invalid-name
"""Registers the schema for com.verily.sleep_metrics with Beam."""

import dataclasses
from typing import Optional

from apache_beam.utils.timestamp import Timestamp

from verily.raw_data_tools.schemas.schemas.shared_schemas import DataPoint
from verily.raw_data_tools.schemas.schemas.decorators import dataspec


@dataspec('com.verily.sleep_metrics')
@dataclasses.dataclass
class Sleep_Metrics(DataPoint):
    """Beam RowSchema for com.verily.sleep_metrics."""
    deep: Optional[float] = None
    imu_ppg_availability: Optional[float] = None
    light: Optional[float] = None
    nrem: Optional[float] = None
    num_awakenings: Optional[int] = None
    num_awakenings_5min: Optional[int] = None
    rem: Optional[float] = None
    sleep_efficiency: Optional[float] = None
    sleep_offset_absolute_timestamp: Optional[Timestamp] = None
    sleep_offset_offset: Optional[int] = None
    sleep_onset_absolute_timestamp: Optional[Timestamp] = None
    sleep_onset_offset: Optional[int] = None
    time_to_sleep: Optional[int] = None
    total_sleep_time: Optional[int] = None
    waso_time: Optional[int] = None
