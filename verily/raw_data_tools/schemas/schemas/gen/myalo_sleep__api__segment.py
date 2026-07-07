# Lint as: python3
# pylint: disable=invalid-name
"""Registers the schema for com.verily.myalo.sleep_api_segment with Beam."""

import dataclasses

from verily.raw_data_tools.schemas.schemas.shared_schemas import DataPoint
from verily.raw_data_tools.schemas.schemas.decorators import dataspec


@dataspec('com.verily.myalo.sleep_api_segment')
@dataclasses.dataclass
class MyaloSleep_Api_Segment(DataPoint):
    """Beam RowSchema for com.verily.myalo.sleep_api_segment."""
    end_time_millis: int
    start_time_millis: int
    status: str
