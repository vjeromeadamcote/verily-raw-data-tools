# Lint as: python3
# pylint: disable=invalid-name
"""Registers the schema for com.verily.myalo.usage_stats with Beam."""

import dataclasses
from typing import List, Optional

from verily.ds_sdk.core.schemas import DataPoint
from verily.ds_sdk.core.schemas import dataspec


@dataspec('com.verily.myalo.usage_stats')
@dataclasses.dataclass
class MyaloUsage_Stats(DataPoint):
    """Beam RowSchema for com.verily.myalo.usage_stats."""
    first_times: Optional[List[int]] = None
    foreground_times: Optional[List[int]] = None
    last_times: Optional[List[int]] = None
    last_used_times: Optional[List[int]] = None
    packages: Optional[List[str]] = None
    reported_time: Optional[int] = None
