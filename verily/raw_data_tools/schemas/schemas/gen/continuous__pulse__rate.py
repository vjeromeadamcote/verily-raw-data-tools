# Lint as: python3
# pylint: disable=invalid-name
"""Registers the schema for com.verily.continuous_pulse_rate with Beam."""

import dataclasses
from typing import Optional

from verily.ds_sdk.core.schemas import DataPoint
from verily.ds_sdk.core.schemas import dataspec


@dataspec('com.verily.continuous_pulse_rate')
@dataclasses.dataclass
class Continuous_Pulse_Rate(DataPoint):
    """Beam RowSchema for com.verily.continuous_pulse_rate."""
    activity_level: Optional[float] = None
    confidence: Optional[float] = None
    pulse_rate: Optional[float] = None
    pulse_rate_std: Optional[float] = None
