# Lint as: python3
# pylint: disable=invalid-name
"""Registers the schema for com.verily.myalo.app_event with Beam."""

import dataclasses
from typing import Optional

from verily.raw_data_tools.schemas.schemas.shared_schemas import DataPoint
from verily.raw_data_tools.schemas.schemas.decorators import dataspec


@dataspec('com.verily.myalo.app_event')
@dataclasses.dataclass
class MyaloApp_Event(DataPoint):
    """Beam RowSchema for com.verily.myalo.app_event."""
    annotation: Optional[str] = None
    app_event_id: Optional[int] = None
    reported_time: Optional[int] = None
