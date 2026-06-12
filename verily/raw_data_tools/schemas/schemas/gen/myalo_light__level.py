# Lint as: python3
# pylint: disable=invalid-name
"""Registers the schema for com.verily.myalo.light_level with Beam."""

import dataclasses
from typing import Optional

from verily.ds_sdk.core.schemas import DataPoint
from verily.ds_sdk.core.schemas import dataspec


@dataspec('com.verily.myalo.light_level')
@dataclasses.dataclass
class MyaloLight_Level(DataPoint):
    """Beam RowSchema for com.verily.myalo.light_level."""
    light_level: Optional[float] = None
    reported_time: Optional[int] = None
    sample_time: Optional[int] = None
