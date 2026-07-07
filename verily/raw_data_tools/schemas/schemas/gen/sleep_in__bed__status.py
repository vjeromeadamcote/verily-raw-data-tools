# Lint as: python3
# pylint: disable=invalid-name
"""Registers the schema for com.verily.sleep.in_bed_status with Beam."""

import dataclasses

from verily.raw_data_tools.schemas.schemas.shared_schemas import DataPoint
from verily.raw_data_tools.schemas.schemas.decorators import dataspec


@dataspec('com.verily.sleep.in_bed_status')
@dataclasses.dataclass
class SleepIn_Bed_Status(DataPoint):
    """Beam RowSchema for com.verily.sleep.in_bed_status."""
    status: str
