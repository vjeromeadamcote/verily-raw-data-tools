# Lint as: python3
# pylint: disable=invalid-name
"""Registers the schema for com.verily.cellular.signal_strength with Beam."""

import dataclasses

from verily.ds_sdk.core.schemas import DataPoint
from verily.ds_sdk.core.schemas import dataspec


@dataspec('com.verily.cellular.signal_strength')
@dataclasses.dataclass
class CellularSignal_Strength(DataPoint):
    """Beam RowSchema for com.verily.cellular.signal_strength."""
    percentage: int
