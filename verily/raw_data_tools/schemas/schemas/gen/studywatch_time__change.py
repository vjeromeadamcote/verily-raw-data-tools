# Lint as: python3
# pylint: disable=invalid-name
"""Registers the schema for com.verily.studywatch.time_change with Beam."""

import dataclasses
from typing import Optional

from verily.raw_data_tools.schemas.schemas.shared_schemas import DataPoint
from verily.raw_data_tools.schemas.schemas.decorators import dataspec


@dataspec('com.verily.studywatch.time_change')
@dataclasses.dataclass
class StudywatchTime_Change(DataPoint):
    """Beam RowSchema for com.verily.studywatch.time_change."""
    new_dst_offset: Optional[int] = None
    new_rtc_ms: Optional[int] = None
    new_scheduled_dst_event_utc_seconds: Optional[int] = None
    new_scheduled_dst_offset: Optional[int] = None
    new_time_zone: Optional[int] = None
    previous_dst_offset: Optional[int] = None
    previous_rtc_ms: Optional[int] = None
    previous_scheduled_dst_event_utc_seconds: Optional[int] = None
    previous_scheduled_dst_offset: Optional[int] = None
    previous_time_zone: Optional[int] = None
