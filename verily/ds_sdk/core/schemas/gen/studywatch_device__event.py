# Lint as: python3
# pylint: disable=invalid-name
"""Registers the schema for com.verily.studywatch.device_event with Beam."""

import dataclasses
from typing import Optional

from verily.ds_sdk.core.schemas import DataPoint
from verily.ds_sdk.core.schemas import dataspec


@dataspec('com.verily.studywatch.device_event')
@dataclasses.dataclass
class StudywatchDevice_Event(DataPoint):
    """Beam RowSchema for com.verily.studywatch.device_event."""
    event: Optional[str] = None
