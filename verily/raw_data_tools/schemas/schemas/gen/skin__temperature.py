# Lint as: python3
# pylint: disable=invalid-name
"""Registers the schema for com.verily.skin_temperature with Beam."""

import dataclasses

from verily.raw_data_tools.schemas.schemas.shared_schemas import DataPoint
from verily.raw_data_tools.schemas.schemas.decorators import dataspec


@dataspec('com.verily.skin_temperature')
@dataclasses.dataclass
class Skin_Temperature(DataPoint):
    """Beam RowSchema for com.verily.skin_temperature."""
    temperature: int
