# Lint as: python3
# pylint: disable=invalid-name
"""Registers the schema for com.verily.phone_usage_stats with Beam."""

import dataclasses
from typing import Optional

from verily.raw_data_tools.schemas.schemas.shared_schemas import DataPoint
from verily.raw_data_tools.schemas.schemas.decorators import dataspec


@dataspec('com.verily.phone_usage_stats')
@dataclasses.dataclass
class Phone_Usage_Stats(DataPoint):
    """Beam RowSchema for com.verily.phone_usage_stats."""
    interval_start_time: Optional[float] = None
    mean_duration_between_consecutive_phone_use_sessions: Optional[float] = None
    mean_user_present_duration_seconds: Optional[float] = None
    number_phone_use_sessions: Optional[int] = None
    std_duration_between_consecutive_phone_use_sessions: Optional[float] = None
    std_user_present_duration_seconds: Optional[float] = None
    time_interval: Optional[float] = None
    user_present_duration_seconds: Optional[float] = None
