# Lint as: python3
# pylint: disable=invalid-name
"""Registers the schema for com.verily.cam.battery_status with Beam."""

import dataclasses

from verily.raw_data_tools.schemas.schemas.shared_schemas import DataPoint
from verily.raw_data_tools.schemas.schemas.decorators import dataspec


@dataspec('com.verily.cam.battery_status')
@dataclasses.dataclass
class CamBattery_Status(DataPoint):
    """Beam RowSchema for com.verily.cam.battery_status."""
    bitmask: int
