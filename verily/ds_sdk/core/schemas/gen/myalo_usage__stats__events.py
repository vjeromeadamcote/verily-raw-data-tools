# Lint as: python3
# pylint: disable=invalid-name
"""Registers the schema for com.verily.myalo.usage_stats_events with Beam."""

import dataclasses
from typing import List, Optional

from verily.ds_sdk.core.schemas import DataPoint
from verily.ds_sdk.core.schemas import dataspec


@dataspec('com.verily.myalo.usage_stats_events')
@dataclasses.dataclass
class MyaloUsage_Stats_Events(DataPoint):
    """Beam RowSchema for com.verily.myalo.usage_stats_events."""
    annotation: Optional[str] = None
    annotations: Optional[List[str]] = None
    event_names: Optional[List[str]] = None
    event_type: Optional[int] = None
    event_type_str: Optional[str] = None
    event_types: Optional[List[int]] = None
    package: Optional[str] = None
    packages: Optional[List[str]] = None
