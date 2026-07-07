# Lint as: python3
# pylint: disable=invalid-name
"""Registers the schema for com.verily.battery_level with Beam."""

import dataclasses

from verily.raw_data_tools.schemas.schemas.shared_schemas import DataPoint
from verily.raw_data_tools.schemas.schemas.decorators import dataspec


@dataspec('com.verily.battery_level')
@dataclasses.dataclass
class Battery_Level(DataPoint):
    """Beam RowSchema for com.verily.battery_level."""
    charge: int
