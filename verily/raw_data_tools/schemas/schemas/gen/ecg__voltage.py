# Lint as: python3
# pylint: disable=invalid-name
"""Registers the schema for com.verily.ecg_voltage with Beam."""

import dataclasses
from typing import List, Optional

from verily.ds_sdk.core.schemas import DataPoint
from verily.ds_sdk.core.schemas import dataspec


@dataspec('com.verily.ecg_voltage')
@dataclasses.dataclass
class Ecg_Voltage(DataPoint):
    """Beam RowSchema for com.verily.ecg_voltage."""
    voltage: List[float]
    sampling_rate: Optional[int] = None
