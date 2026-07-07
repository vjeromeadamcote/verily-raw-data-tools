# Lint as: python3
# pylint: disable=invalid-name
"""Registers the schema for com.verily.overnight_resting_heart_rate with Beam."""  # pylint: disable=line-too-long

import dataclasses
from typing import Optional

from verily.raw_data_tools.schemas.schemas.shared_schemas import DataPoint
from verily.raw_data_tools.schemas.schemas.decorators import dataspec


@dataspec('com.verily.overnight_resting_heart_rate')
@dataclasses.dataclass
class Overnight_Resting_Heart_Rate(DataPoint):
    """Beam RowSchema for com.verily.overnight_resting_heart_rate."""
    confidence: Optional[int] = None
    num_beats: Optional[int] = None
    rhr: Optional[float] = None
