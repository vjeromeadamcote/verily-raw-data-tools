# Lint as: python3
# pylint: disable=invalid-name
"""Registers the schema for com.verily.careinnovations.device_event with Beam."""  # pylint: disable=line-too-long

import dataclasses
from typing import Optional

from verily.raw_data_tools.schemas.schemas.shared_schemas import DataPoint
from verily.raw_data_tools.schemas.schemas.decorators import dataspec


@dataspec('com.verily.careinnovations.device_event')
@dataclasses.dataclass
class CareinnovationsDevice_Event(DataPoint):
    """Beam RowSchema for com.verily.careinnovations.device_event."""
    details: Optional[str] = None
    event: Optional[str] = None
