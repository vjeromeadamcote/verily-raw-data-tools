# Lint as: python3
# pylint: disable=invalid-name
"""Registers the schema for com.verily.ambient_temperature with Beam."""

import dataclasses

from verily.raw_data_tools.schemas.schemas.shared_schemas import DataPoint
from verily.raw_data_tools.schemas.schemas.decorators import dataspec


@dataspec('com.verily.ambient_temperature')
@dataclasses.dataclass
class Ambient_Temperature(DataPoint):
    """Beam RowSchema for com.verily.ambient_temperature."""
    temperature: int
