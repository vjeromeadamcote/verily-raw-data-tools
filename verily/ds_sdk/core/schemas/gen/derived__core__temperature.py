# Lint as: python3
# pylint: disable=invalid-name
"""Registers the schema for com.verily.derived_core_temperature with Beam."""

import dataclasses

from verily.ds_sdk.core.schemas import DataPoint
from verily.ds_sdk.core.schemas import dataspec


@dataspec('com.verily.derived_core_temperature')
@dataclasses.dataclass
class Derived_Core_Temperature(DataPoint):
    """Beam RowSchema for com.verily.derived_core_temperature."""
    temperature: int
