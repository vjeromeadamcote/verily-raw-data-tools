# Lint as: python3
# pylint: disable=invalid-name
"""Registers the schema for com.verily.myalo.device with Beam."""

import dataclasses
from typing import Optional

from verily.raw_data_tools.schemas.schemas.shared_schemas import DataPoint
from verily.raw_data_tools.schemas.schemas.decorators import dataspec


@dataspec('com.verily.myalo.device')
@dataclasses.dataclass
class MyaloDevice(DataPoint):
    """Beam RowSchema for com.verily.myalo.device."""
    app_version: Optional[str] = None
    brand: Optional[str] = None
    fingerprint: Optional[str] = None
    is_device_secure: Optional[bool] = None
    local_time_string: Optional[str] = None
    manufacturer: Optional[str] = None
    max_cpu_clock: Optional[int] = None
    min_cpu_clock: Optional[int] = None
    model: Optional[str] = None
    os_version: Optional[str] = None
    reported_time: Optional[int] = None
    sample_time: Optional[int] = None
    serial_number: Optional[str] = None
    timezone_offset: Optional[int] = None
