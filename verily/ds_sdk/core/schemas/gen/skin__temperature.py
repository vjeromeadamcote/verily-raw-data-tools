# Lint as: python3
# pylint: disable=invalid-name
"""Registers the schema for com.verily.skin_temperature with Beam."""

import dataclasses

from verily.ds_sdk.core.schemas import DataPoint
from verily.ds_sdk.core.schemas import dataspec


@dataspec('com.verily.skin_temperature')
@dataclasses.dataclass
class Skin_Temperature(DataPoint):
    """Beam RowSchema for com.verily.skin_temperature."""
    temperature: int
