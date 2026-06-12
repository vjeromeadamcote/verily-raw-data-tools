# Lint as: python3
# pylint: disable=invalid-name
"""Registers the schema for com.verily.heart_rate with Beam."""

import dataclasses
from typing import Optional

from verily.ds_sdk.core.schemas import DataPoint
from verily.ds_sdk.core.schemas import dataspec


@dataspec('com.verily.heart_rate')
@dataclasses.dataclass
class Heart_Rate(DataPoint):
    """Beam RowSchema for com.verily.heart_rate."""
    heart_rate: float
    confidence: Optional[int] = None
