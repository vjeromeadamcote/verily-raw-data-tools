# Lint as: python3
# pylint: disable=invalid-name
"""Registers the schema for com.verily.blood_pressure with Beam."""

import dataclasses
from typing import Optional

from verily.raw_data_tools.schemas.schemas.shared_schemas import DataPoint
from verily.raw_data_tools.schemas.schemas.decorators import dataspec


@dataspec('com.verily.blood_pressure')
@dataclasses.dataclass
class Blood_Pressure(DataPoint):
    """Beam RowSchema for com.verily.blood_pressure."""
    diastolic: float
    systolic: float
    movement_during_reading: Optional[bool] = None
