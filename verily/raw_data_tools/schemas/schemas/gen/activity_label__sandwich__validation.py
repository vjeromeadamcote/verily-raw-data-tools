# Lint as: python3
# pylint: disable=invalid-name
"""Registers the schema for com.verily.activity.label_sandwich_validation with Beam."""  # pylint: disable=line-too-long

import dataclasses
from typing import Optional

from verily.raw_data_tools.schemas.schemas.shared_schemas import DataPoint
from verily.raw_data_tools.schemas.schemas.decorators import dataspec


@dataspec('com.verily.activity.label_sandwich_validation')
@dataclasses.dataclass
class ActivityLabel_Sandwich_Validation(DataPoint):
    """Beam RowSchema for com.verily.activity.label_sandwich_validation."""
    class_label: str
    confidence: int
    intensity: Optional[int] = None
