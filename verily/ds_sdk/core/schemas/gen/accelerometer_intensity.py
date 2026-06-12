# Lint as: python3
# pylint: disable=invalid-name
"""Registers the schema for com.verily.accelerometer.intensity with Beam."""

import dataclasses

from verily.ds_sdk.core.schemas import DataPoint
from verily.ds_sdk.core.schemas import dataspec


@dataspec('com.verily.accelerometer.intensity')
@dataclasses.dataclass
class AccelerometerIntensity(DataPoint):
    """Beam RowSchema for com.verily.accelerometer.intensity."""
    intensity: int
