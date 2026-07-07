# Lint as: python3
# pylint: disable=invalid-name
"""Registers the schema for com.verily.myalo.pressure with Beam."""

import dataclasses
from typing import Optional

from verily.raw_data_tools.schemas.schemas.shared_schemas import DataPoint
from verily.raw_data_tools.schemas.schemas.decorators import dataspec


@dataspec('com.verily.myalo.pressure')
@dataclasses.dataclass
class MyaloPressure(DataPoint):
    """Beam RowSchema for com.verily.myalo.pressure."""
    pressure: Optional[float] = None
    reported_time: Optional[int] = None
    sample_time: Optional[int] = None
