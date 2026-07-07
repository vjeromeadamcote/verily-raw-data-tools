# Lint as: python3
# pylint: disable=invalid-name
"""Registers the schema for com.verily.studywatch.battery_temperature with Beam."""  # pylint: disable=line-too-long

import dataclasses
from typing import Optional

from verily.raw_data_tools.schemas.schemas.shared_schemas import DataPoint
from verily.raw_data_tools.schemas.schemas.decorators import dataspec


@dataspec('com.verily.studywatch.battery_temperature')
@dataclasses.dataclass
class StudywatchBattery_Temperature(DataPoint):
    """Beam RowSchema for com.verily.studywatch.battery_temperature."""
    temperature: Optional[int] = None
