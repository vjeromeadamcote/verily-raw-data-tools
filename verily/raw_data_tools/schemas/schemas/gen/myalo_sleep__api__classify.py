# Lint as: python3
# pylint: disable=invalid-name
"""Registers the schema for com.verily.myalo.sleep_api_classify with Beam."""

import dataclasses
from typing import Optional

from verily.raw_data_tools.schemas.schemas.shared_schemas import DataPoint
from verily.raw_data_tools.schemas.schemas.decorators import dataspec


@dataspec('com.verily.myalo.sleep_api_classify')
@dataclasses.dataclass
class MyaloSleep_Api_Classify(DataPoint):
    """Beam RowSchema for com.verily.myalo.sleep_api_classify."""
    confidence: Optional[int] = None
    light: Optional[int] = None
    motion: Optional[int] = None
    noise: Optional[int] = None
    reported_time: Optional[int] = None
