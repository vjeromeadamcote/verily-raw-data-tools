# Lint as: python3
# pylint: disable=invalid-name
"""Registers the schema for com.verily.myalo.pressure with Beam."""

import dataclasses
from typing import Optional

from verily.ds_sdk.core.schemas import DataPoint
from verily.ds_sdk.core.schemas import dataspec


@dataspec('com.verily.myalo.pressure')
@dataclasses.dataclass
class MyaloPressure(DataPoint):
    """Beam RowSchema for com.verily.myalo.pressure."""
    pressure: Optional[float] = None
    reported_time: Optional[int] = None
    sample_time: Optional[int] = None
