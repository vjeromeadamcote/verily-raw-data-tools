# Lint as: python3
# pylint: disable=invalid-name
"""Registers the schema for com.verily.hypnos.resmed with Beam."""

import dataclasses
from typing import Optional

from verily.raw_data_tools.schemas.schemas.shared_schemas import DataPoint
from verily.raw_data_tools.schemas.schemas.decorators import dataspec


@dataspec('com.verily.hypnos.resmed')
@dataclasses.dataclass
class HypnosResmed(DataPoint):
    """Beam RowSchema for com.verily.hypnos.resmed."""
    mask_off_count: Optional[int] = None
    mask_on_count: Optional[int] = None
    metrics_leak_50: Optional[float] = None
    metrics_leak_95: Optional[float] = None
    metrics_leak_max: Optional[float] = None
    resp_events_ahi: Optional[float] = None
    resp_events_cai: Optional[float] = None
    settings_pressure: Optional[float] = None
    usage_duration: Optional[int] = None
