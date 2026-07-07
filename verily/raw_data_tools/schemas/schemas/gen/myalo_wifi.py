# Lint as: python3
# pylint: disable=invalid-name
"""Registers the schema for com.verily.myalo.wifi with Beam."""

import dataclasses
from typing import List, Optional

from verily.raw_data_tools.schemas.schemas.shared_schemas import DataPoint
from verily.raw_data_tools.schemas.schemas.decorators import dataspec


@dataspec('com.verily.myalo.wifi')
@dataclasses.dataclass
class MyaloWifi(DataPoint):
    """Beam RowSchema for com.verily.myalo.wifi."""
    enabled: Optional[bool] = None
    hashed_wifi_names: Optional[List[int]] = None
    powers: Optional[List[int]] = None
    reported_time: Optional[int] = None
    wifi_names: Optional[List[str]] = None
