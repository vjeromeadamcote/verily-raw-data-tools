# Lint as: python3
# pylint: disable=invalid-name
"""Registers the schema for com.verily.studywatch.vme_schedule_change_event with Beam."""  # pylint: disable=line-too-long

import dataclasses
from typing import List, Optional

from apache_beam.utils.timestamp import Timestamp

from verily.ds_sdk.core.schemas import DataPoint
from verily.ds_sdk.core.schemas import dataspec


@dataspec('com.verily.studywatch.vme_schedule_change_event')
@dataclasses.dataclass
class StudywatchVme_Schedule_Change_Event(DataPoint):
    """Beam RowSchema for com.verily.studywatch.vme_schedule_change_event."""
    app_end_timestamp: Optional[Timestamp] = None
    app_start_timestamp: Optional[Timestamp] = None
    is_rescheduled: Optional[bool] = None
    scheduled_times_minute_in_day: Optional[List[int]] = None
    scheduled_times_packed: Optional[List[int]] = None
