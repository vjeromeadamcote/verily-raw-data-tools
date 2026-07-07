# Lint as: python3
# pylint: disable=invalid-name
"""Registers the schema for com.verily.studywatch.vibration_event with Beam."""

import dataclasses
from typing import Optional

from verily.raw_data_tools.schemas.schemas.shared_schemas import DataPoint
from verily.raw_data_tools.schemas.schemas.decorators import dataspec


@dataspec('com.verily.studywatch.vibration_event')
@dataclasses.dataclass
class StudywatchVibration_Event(DataPoint):
    """Beam RowSchema for com.verily.studywatch.vibration_event."""
    event_type: Optional[int] = None
