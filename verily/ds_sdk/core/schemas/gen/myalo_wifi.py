# Lint as: python3
# pylint: disable=invalid-name
"""Registers the schema for com.verily.myalo.wifi with Beam."""

import dataclasses
from typing import List, Optional

from verily.ds_sdk.core.schemas import DataPoint
from verily.ds_sdk.core.schemas import dataspec


@dataspec('com.verily.myalo.wifi')
@dataclasses.dataclass
class MyaloWifi(DataPoint):
    """Beam RowSchema for com.verily.myalo.wifi."""
    enabled: Optional[bool] = None
    hashed_wifi_names: Optional[List[int]] = None
    powers: Optional[List[int]] = None
    reported_time: Optional[int] = None
    wifi_names: Optional[List[str]] = None
