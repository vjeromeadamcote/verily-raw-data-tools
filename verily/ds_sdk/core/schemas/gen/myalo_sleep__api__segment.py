# Lint as: python3
# pylint: disable=invalid-name
"""Registers the schema for com.verily.myalo.sleep_api_segment with Beam."""

import dataclasses

from verily.ds_sdk.core.schemas import DataPoint
from verily.ds_sdk.core.schemas import dataspec


@dataspec('com.verily.myalo.sleep_api_segment')
@dataclasses.dataclass
class MyaloSleep_Api_Segment(DataPoint):
    """Beam RowSchema for com.verily.myalo.sleep_api_segment."""
    end_time_millis: int
    start_time_millis: int
    status: str
