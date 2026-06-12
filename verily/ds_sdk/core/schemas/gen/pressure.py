# Lint as: python3
# pylint: disable=invalid-name
"""Registers the schema for com.verily.pressure with Beam."""

import dataclasses

from verily.ds_sdk.core.schemas import DataPoint
from verily.ds_sdk.core.schemas import dataspec


@dataspec('com.verily.pressure')
@dataclasses.dataclass
class Pressure(DataPoint):
    """Beam RowSchema for com.verily.pressure."""
    pressure: int
