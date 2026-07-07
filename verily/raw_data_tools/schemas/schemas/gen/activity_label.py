# Lint as: python3
# pylint: disable=invalid-name
"""Registers the schema for com.verily.activity.label with Beam."""

import dataclasses
from typing import Optional

from verily.raw_data_tools.schemas.schemas.shared_schemas import DataPoint
from verily.raw_data_tools.schemas.schemas.decorators import dataspec


@dataspec('com.verily.activity.label')
@dataclasses.dataclass
class ActivityLabel(DataPoint):
    """Beam RowSchema for com.verily.activity.label."""
    class_label: str
    confidence: int
    intensity: Optional[int] = None
