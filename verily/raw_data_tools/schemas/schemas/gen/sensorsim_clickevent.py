# Lint as: python3
# pylint: disable=invalid-name
"""Registers the schema for com.verily.sensorsim.clickevent with Beam."""

import dataclasses

from verily.raw_data_tools.schemas.schemas.shared_schemas import DataPoint
from verily.raw_data_tools.schemas.schemas.decorators import dataspec


@dataspec('com.verily.sensorsim.clickevent')
@dataclasses.dataclass
class SensorsimClickevent(DataPoint):
    """Beam RowSchema for com.verily.sensorsim.clickevent."""
    click: bool
