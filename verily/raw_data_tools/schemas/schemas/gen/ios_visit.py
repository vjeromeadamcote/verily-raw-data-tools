# Lint as: python3
# pylint: disable=invalid-name
"""Registers the schema for com.verily.ios.visit with Beam."""

import dataclasses
from typing import Optional

from verily.raw_data_tools.schemas.schemas.shared_schemas import DataPoint
from verily.raw_data_tools.schemas.schemas.decorators import dataspec


@dataspec('com.verily.ios.visit')
@dataclasses.dataclass
class IosVisit(DataPoint):
    """Beam RowSchema for com.verily.ios.visit."""
    latitude: float
    longitude: float
    departure_time: Optional[int] = None
    horizontal_accuracy: Optional[float] = None
