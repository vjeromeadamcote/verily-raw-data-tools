# Lint as: python3
# pylint: disable=invalid-name
"""Registers the schema for com.verily.ambient_light with Beam."""

import dataclasses

from verily.ds_sdk.core.schemas import DataPoint
from verily.ds_sdk.core.schemas import dataspec


@dataspec('com.verily.ambient_light')
@dataclasses.dataclass
class Ambient_Light(DataPoint):
    """Beam RowSchema for com.verily.ambient_light."""
    lux: int
