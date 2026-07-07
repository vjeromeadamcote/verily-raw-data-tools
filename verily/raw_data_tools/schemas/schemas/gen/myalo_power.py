# Lint as: python3
# pylint: disable=invalid-name
"""Registers the schema for com.verily.myalo.power with Beam."""

import dataclasses
from typing import Optional

from verily.raw_data_tools.schemas.schemas.shared_schemas import DataPoint
from verily.raw_data_tools.schemas.schemas.decorators import dataspec


@dataspec('com.verily.myalo.power')
@dataclasses.dataclass
class MyaloPower(DataPoint):
    """Beam RowSchema for com.verily.myalo.power."""
    battery_percent: Optional[int] = None
    power_state: Optional[int] = None
    reported_time: Optional[int] = None
