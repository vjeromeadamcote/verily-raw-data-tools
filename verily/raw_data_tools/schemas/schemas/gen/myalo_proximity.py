# Lint as: python3
# pylint: disable=invalid-name
"""Registers the schema for com.verily.myalo.proximity with Beam."""

import dataclasses
from typing import Optional

from verily.raw_data_tools.schemas.schemas.shared_schemas import DataPoint
from verily.raw_data_tools.schemas.schemas.decorators import dataspec


@dataspec('com.verily.myalo.proximity')
@dataclasses.dataclass
class MyaloProximity(DataPoint):
    """Beam RowSchema for com.verily.myalo.proximity."""
    distance: Optional[float] = None
    is_far: Optional[bool] = None
    reported_time: Optional[int] = None
    sample_time: Optional[int] = None
