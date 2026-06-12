# Lint as: python3
# pylint: disable=invalid-name
"""Registers the schema for com.verily.studywatch.battery_temperature with Beam."""  # pylint: disable=line-too-long

import dataclasses
from typing import Optional

from verily.ds_sdk.core.schemas import DataPoint
from verily.ds_sdk.core.schemas import dataspec


@dataspec('com.verily.studywatch.battery_temperature')
@dataclasses.dataclass
class StudywatchBattery_Temperature(DataPoint):
    """Beam RowSchema for com.verily.studywatch.battery_temperature."""
    temperature: Optional[int] = None
