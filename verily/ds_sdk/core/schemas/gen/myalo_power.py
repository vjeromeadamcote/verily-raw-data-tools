# Lint as: python3
# pylint: disable=invalid-name
"""Registers the schema for com.verily.myalo.power with Beam."""

import dataclasses
from typing import Optional

from verily.ds_sdk.core.schemas import DataPoint
from verily.ds_sdk.core.schemas import dataspec


@dataspec('com.verily.myalo.power')
@dataclasses.dataclass
class MyaloPower(DataPoint):
    """Beam RowSchema for com.verily.myalo.power."""
    battery_percent: Optional[int] = None
    power_state: Optional[int] = None
    reported_time: Optional[int] = None
