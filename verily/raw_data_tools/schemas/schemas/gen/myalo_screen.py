# Lint as: python3
# pylint: disable=invalid-name
"""Registers the schema for com.verily.myalo.screen with Beam."""

import dataclasses
from typing import Optional

from verily.raw_data_tools.schemas.schemas.shared_schemas import DataPoint
from verily.raw_data_tools.schemas.schemas.decorators import dataspec


@dataspec('com.verily.myalo.screen')
@dataclasses.dataclass
class MyaloScreen(DataPoint):
    """Beam RowSchema for com.verily.myalo.screen."""
    reported_time: Optional[int] = None
    screen_state: Optional[int] = None
