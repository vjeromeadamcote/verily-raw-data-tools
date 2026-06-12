# Lint as: python3
# pylint: disable=invalid-name
"""Registers the schema for com.verily.studywatch.vibration_event with Beam."""

import dataclasses
from typing import Optional

from verily.ds_sdk.core.schemas import DataPoint
from verily.ds_sdk.core.schemas import dataspec


@dataspec('com.verily.studywatch.vibration_event')
@dataclasses.dataclass
class StudywatchVibration_Event(DataPoint):
    """Beam RowSchema for com.verily.studywatch.vibration_event."""
    event_type: Optional[int] = None
