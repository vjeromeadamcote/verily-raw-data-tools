# Lint as: python3
# pylint: disable=invalid-name
"""Registers the schema for com.verily.myalo.alarm_volumes with Beam."""

import dataclasses
from typing import Optional

from verily.raw_data_tools.schemas.schemas.shared_schemas import DataPoint
from verily.raw_data_tools.schemas.schemas.decorators import dataspec


@dataspec('com.verily.myalo.alarm_volumes')
@dataclasses.dataclass
class MyaloAlarm_Volumes(DataPoint):
    """Beam RowSchema for com.verily.myalo.alarm_volumes."""
    alarm_time: Optional[int] = None
    alarm_volume: Optional[int] = None
    mode: Optional[str] = None
    music_volume: Optional[int] = None
    notification_volume: Optional[int] = None
    reported_time: Optional[int] = None
    ring_volume: Optional[int] = None
    ringer_mode: Optional[str] = None
    sample_time: Optional[int] = None
    system_volume: Optional[int] = None
    voice_call_volume: Optional[int] = None
