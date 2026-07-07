# Lint as: python3
# pylint: disable=invalid-name
"""Registers the schema for com.verily.battery_percentage with Beam."""

import dataclasses

from verily.raw_data_tools.schemas.schemas.shared_schemas import DataPoint
from verily.raw_data_tools.schemas.schemas.decorators import dataspec


@dataspec('com.verily.battery_percentage')
@dataclasses.dataclass
class Battery_Percentage(DataPoint):
    """Beam RowSchema for com.verily.battery_percentage."""
    percentage: float
