# Lint as: python3
# pylint: disable=invalid-name
"""Registers the schema for com.verily.myalo.proximity with Beam."""

import dataclasses
from typing import Optional

from verily.ds_sdk.core.schemas import DataPoint
from verily.ds_sdk.core.schemas import dataspec


@dataspec('com.verily.myalo.proximity')
@dataclasses.dataclass
class MyaloProximity(DataPoint):
    """Beam RowSchema for com.verily.myalo.proximity."""
    distance: Optional[float] = None
    is_far: Optional[bool] = None
    reported_time: Optional[int] = None
    sample_time: Optional[int] = None
