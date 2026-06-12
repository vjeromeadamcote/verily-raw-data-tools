# Lint as: python3
# pylint: disable=invalid-name
"""Registers the schema for com.verily.location.semantic_location_visit_event with Beam."""  # pylint: disable=line-too-long

import dataclasses
from typing import Optional

from verily.ds_sdk.core.schemas import DataPoint
from verily.ds_sdk.core.schemas import dataspec


@dataspec('com.verily.location.semantic_location_visit_event')
@dataclasses.dataclass
class LocationSemantic_Location_Visit_Event(DataPoint):
    """Beam RowSchema for com.verily.location.semantic_location_visit_event."""
    duration_millis: int
    latitude: float
    longitude: float
    location_name: Optional[str] = None
