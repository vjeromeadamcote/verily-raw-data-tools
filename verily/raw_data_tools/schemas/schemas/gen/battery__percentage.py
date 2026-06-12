# Lint as: python3
# pylint: disable=invalid-name
"""Registers the schema for com.verily.battery_percentage with Beam."""

import dataclasses

from verily.ds_sdk.core.schemas import DataPoint
from verily.ds_sdk.core.schemas import dataspec


@dataspec('com.verily.battery_percentage')
@dataclasses.dataclass
class Battery_Percentage(DataPoint):
    """Beam RowSchema for com.verily.battery_percentage."""
    percentage: float
