# Lint as: python3
# pylint: disable=invalid-name
"""Registers the schema for com.verily.sleep.in_bed_status with Beam."""

import dataclasses

from verily.ds_sdk.core.schemas import DataPoint
from verily.ds_sdk.core.schemas import dataspec


@dataspec('com.verily.sleep.in_bed_status')
@dataclasses.dataclass
class SleepIn_Bed_Status(DataPoint):
    """Beam RowSchema for com.verily.sleep.in_bed_status."""
    status: str
