# Lint as: python3
# pylint: disable=invalid-name
"""Registers the schema for com.verily.semantic_location with Beam."""

import dataclasses
from typing import Optional

from verily.ds_sdk.core.schemas import DataPoint
from verily.ds_sdk.core.schemas import dataspec


@dataspec('com.verily.semantic_location')
@dataclasses.dataclass
class Semantic_Location(DataPoint):
    """Beam RowSchema for com.verily.semantic_location."""
    altitude: Optional[int] = None
    bearing: Optional[int] = None
    bearing_accuracy: Optional[float] = None
    departure_time: Optional[int] = None
    horizontal_accuracy: Optional[int] = None
    latitude: Optional[float] = None
    longitude: Optional[float] = None
    speed: Optional[float] = None
    speed_accuracy: Optional[float] = None
    vertical_accuracy: Optional[float] = None
