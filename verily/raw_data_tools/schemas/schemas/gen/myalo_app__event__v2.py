# Lint as: python3
# pylint: disable=invalid-name
"""Registers the schema for com.verily.myalo.app_event_v2 with Beam."""

import dataclasses
from typing import List, Optional

from verily.raw_data_tools.schemas.schemas.shared_schemas import DataPoint
from verily.raw_data_tools.schemas.schemas.decorators import dataspec


@dataspec('com.verily.myalo.app_event_v2')
@dataclasses.dataclass
class MyaloApp_Event_V2(DataPoint):
    """Beam RowSchema for com.verily.myalo.app_event_v2."""
    annotation: Optional[str] = None
    app_event_id: Optional[int] = None
    parameter_key: Optional[List[str]] = None
    parameter_value: Optional[List[str]] = None
