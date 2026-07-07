# Lint as: python3
# pylint: disable=invalid-name
"""Registers the schema for com.verily.onduo.blood_glucose_reading with Beam."""

import dataclasses
from typing import Optional

from verily.raw_data_tools.schemas.schemas.shared_schemas import DataPoint
from verily.raw_data_tools.schemas.schemas.decorators import dataspec


@dataspec('com.verily.onduo.blood_glucose_reading')
@dataclasses.dataclass
class OnduoBlood_Glucose_Reading(DataPoint):
    """Beam RowSchema for com.verily.onduo.blood_glucose_reading."""
    create_time_ms: int
    glucose_reading: float
    meal_context_name: str
    symbolic_time_name: str
    device_address: Optional[str] = None
    sequence_number: Optional[int] = None
