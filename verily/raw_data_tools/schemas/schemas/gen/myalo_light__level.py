# Lint as: python3
# pylint: disable=invalid-name
"""Registers the schema for com.verily.myalo.light_level with Beam."""

import dataclasses
from typing import Optional

from verily.raw_data_tools.schemas.schemas.shared_schemas import DataPoint
from verily.raw_data_tools.schemas.schemas.decorators import dataspec


@dataspec('com.verily.myalo.light_level')
@dataclasses.dataclass
class MyaloLight_Level(DataPoint):
    """Beam RowSchema for com.verily.myalo.light_level."""
    light_level: Optional[float] = None
    reported_time: Optional[int] = None
    sample_time: Optional[int] = None
