# Lint as: python3
# pylint: disable=invalid-name
"""Registers the schema for com.verily.studywatch.clock_accuracy with Beam."""

import dataclasses
from typing import Optional

from verily.raw_data_tools.schemas.schemas.shared_schemas import DataPoint
from verily.raw_data_tools.schemas.schemas.decorators import dataspec


@dataspec('com.verily.studywatch.clock_accuracy')
@dataclasses.dataclass
class StudywatchClock_Accuracy(DataPoint):
    """Beam RowSchema for com.verily.studywatch.clock_accuracy."""
    clock_delay: Optional[int] = None
    connected_device_id: Optional[str] = None
