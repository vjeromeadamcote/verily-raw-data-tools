# Lint as: python3
# pylint: disable=invalid-name
"""Registers the schema for com.verily.myalo.location with Beam."""

import dataclasses
from typing import Optional

from verily.ds_sdk.core.schemas import DataPoint
from verily.ds_sdk.core.schemas import dataspec


@dataspec('com.verily.myalo.location')
@dataclasses.dataclass
class MyaloLocation(DataPoint):
    """Beam RowSchema for com.verily.myalo.location."""
    accuracy: Optional[float] = None
    altitude: Optional[float] = None
    altitude_reference: Optional[int] = None
    bearing: Optional[float] = None
    latitude: Optional[float] = None
    longitude: Optional[float] = None
    provider: Optional[str] = None
    reported_time: Optional[int] = None
    speed: Optional[float] = None
