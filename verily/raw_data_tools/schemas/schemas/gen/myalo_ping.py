# Lint as: python3
# pylint: disable=invalid-name
"""Registers the schema for com.verily.myalo.ping with Beam."""

import dataclasses
from typing import Optional

from verily.ds_sdk.core.schemas import DataPoint
from verily.ds_sdk.core.schemas import dataspec


@dataspec('com.verily.myalo.ping')
@dataclasses.dataclass
class MyaloPing(DataPoint):
    """Beam RowSchema for com.verily.myalo.ping."""
    boot_time: Optional[int] = None
    current_cpu_clock: Optional[int] = None
    is_airplane_mode_on: Optional[bool] = None
    is_device_idle: Optional[bool] = None
    is_power_save: Optional[bool] = None
    is_screen_locked: Optional[bool] = None
    is_screen_on: Optional[bool] = None
    max_cpu_clock: Optional[int] = None
    min_cpu_clock: Optional[int] = None
    reported_time: Optional[int] = None
    sample_time: Optional[int] = None
