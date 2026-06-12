# Lint as: python3
# pylint: disable=invalid-name
"""Registers the schema for com.verily.humidity with Beam."""

import dataclasses

from verily.ds_sdk.core.schemas import DataPoint
from verily.ds_sdk.core.schemas import dataspec


@dataspec('com.verily.humidity')
@dataclasses.dataclass
class Humidity(DataPoint):
    """Beam RowSchema for com.verily.humidity."""
    humidity: int
