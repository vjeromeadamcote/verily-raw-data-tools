# Lint as: python3
# pylint: disable=invalid-name
"""Registers the schema for com.verily.binary_bluetooth_packets with Beam."""

import dataclasses
from typing import Any

from verily.raw_data_tools.schemas.schemas.shared_schemas import DataPoint
from verily.raw_data_tools.schemas.schemas.decorators import dataspec


@dataspec('com.verily.binary_bluetooth_packets')
@dataclasses.dataclass
class Binary_Bluetooth_Packets(DataPoint):
    """Beam RowSchema for com.verily.binary_bluetooth_packets."""
    value: Any
