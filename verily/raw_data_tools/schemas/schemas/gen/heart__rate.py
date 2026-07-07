# Lint as: python3
# pylint: disable=invalid-name
"""Registers the schema for com.verily.heart_rate with Beam."""

import dataclasses
from typing import Optional

from verily.raw_data_tools.schemas.schemas.shared_schemas import DataPoint
from verily.raw_data_tools.schemas.schemas.decorators import dataspec


@dataspec('com.verily.heart_rate')
@dataclasses.dataclass
class Heart_Rate(DataPoint):
    """Beam RowSchema for com.verily.heart_rate."""
    heart_rate: float
    confidence: Optional[int] = None
