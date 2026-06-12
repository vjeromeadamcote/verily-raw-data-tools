# Lint as: python3
# pylint: disable=invalid-name
"""Registers the schema for com.verily.myalo.screen with Beam."""

import dataclasses
from typing import Optional

from verily.ds_sdk.core.schemas import DataPoint
from verily.ds_sdk.core.schemas import dataspec


@dataspec('com.verily.myalo.screen')
@dataclasses.dataclass
class MyaloScreen(DataPoint):
    """Beam RowSchema for com.verily.myalo.screen."""
    reported_time: Optional[int] = None
    screen_state: Optional[int] = None
