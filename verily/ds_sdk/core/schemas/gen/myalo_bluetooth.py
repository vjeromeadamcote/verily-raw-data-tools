# Lint as: python3
# pylint: disable=invalid-name
"""Registers the schema for com.verily.myalo.bluetooth with Beam."""

import dataclasses
from typing import Optional

from verily.ds_sdk.core.schemas import DataPoint
from verily.ds_sdk.core.schemas import dataspec


@dataspec('com.verily.myalo.bluetooth')
@dataclasses.dataclass
class MyaloBluetooth(DataPoint):
    """Beam RowSchema for com.verily.myalo.bluetooth."""
    device_address: Optional[int] = None
    device_class_major: Optional[str] = None
    device_name: Optional[int] = None
    enabled: Optional[bool] = None
    reported_time: Optional[int] = None
    sampling_start_time: Optional[int] = None
