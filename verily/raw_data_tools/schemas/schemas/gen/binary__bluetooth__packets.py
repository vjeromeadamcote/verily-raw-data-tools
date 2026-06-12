# Lint as: python3
# pylint: disable=invalid-name
"""Registers the schema for com.verily.binary_bluetooth_packets with Beam."""

import dataclasses
from typing import Any

from verily.ds_sdk.core.schemas import DataPoint
from verily.ds_sdk.core.schemas import dataspec


@dataspec('com.verily.binary_bluetooth_packets')
@dataclasses.dataclass
class Binary_Bluetooth_Packets(DataPoint):
    """Beam RowSchema for com.verily.binary_bluetooth_packets."""
    value: Any
