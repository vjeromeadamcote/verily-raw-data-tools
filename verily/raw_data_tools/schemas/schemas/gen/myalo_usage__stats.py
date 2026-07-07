# Lint as: python3
# pylint: disable=invalid-name
"""Registers the schema for com.verily.myalo.usage_stats with Beam."""

import dataclasses
from typing import List, Optional

from verily.raw_data_tools.schemas.schemas.shared_schemas import DataPoint
from verily.raw_data_tools.schemas.schemas.decorators import dataspec


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
