# Lint as: python3
# pylint: disable=invalid-name
"""Registers the schema for com.verily.onduo.continuous_blood_glucose_reading with Beam."""  # pylint: disable=line-too-long

import dataclasses

from verily.raw_data_tools.schemas.schemas.shared_schemas import DataPoint
from verily.raw_data_tools.schemas.schemas.decorators import dataspec


@dataspec('com.verily.onduo.continuous_blood_glucose_reading')
@dataclasses.dataclass
class OnduoContinuous_Blood_Glucose_Reading(DataPoint):
    """Beam RowSchema for com.verily.onduo.continuous_blood_glucose_reading."""
    create_time_ms: int
    glucose_reading: float
