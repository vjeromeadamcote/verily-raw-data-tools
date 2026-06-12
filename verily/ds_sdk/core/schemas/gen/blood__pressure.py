# Lint as: python3
# pylint: disable=invalid-name
"""Registers the schema for com.verily.blood_pressure with Beam."""

import dataclasses
from typing import Optional

from verily.ds_sdk.core.schemas import DataPoint
from verily.ds_sdk.core.schemas import dataspec


@dataspec('com.verily.blood_pressure')
@dataclasses.dataclass
class Blood_Pressure(DataPoint):
    """Beam RowSchema for com.verily.blood_pressure."""
    diastolic: float
    systolic: float
    movement_during_reading: Optional[bool] = None
