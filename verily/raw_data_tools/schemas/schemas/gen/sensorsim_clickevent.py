# Lint as: python3
# pylint: disable=invalid-name
"""Registers the schema for com.verily.sensorsim.clickevent with Beam."""

import dataclasses

from verily.ds_sdk.core.schemas import DataPoint
from verily.ds_sdk.core.schemas import dataspec


@dataspec('com.verily.sensorsim.clickevent')
@dataclasses.dataclass
class SensorsimClickevent(DataPoint):
    """Beam RowSchema for com.verily.sensorsim.clickevent."""
    click: bool
