# Lint as: python3
# pylint: disable=invalid-name
"""Registers the schema for com.verily.ios.visit with Beam."""

import dataclasses
from typing import Optional

from verily.ds_sdk.core.schemas import DataPoint
from verily.ds_sdk.core.schemas import dataspec


@dataspec('com.verily.ios.visit')
@dataclasses.dataclass
class IosVisit(DataPoint):
    """Beam RowSchema for com.verily.ios.visit."""
    latitude: float
    longitude: float
    departure_time: Optional[int] = None
    horizontal_accuracy: Optional[float] = None
