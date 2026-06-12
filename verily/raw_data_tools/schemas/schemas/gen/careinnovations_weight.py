# Lint as: python3
# pylint: disable=invalid-name
"""Registers the schema for com.verily.careinnovations.weight with Beam."""

import dataclasses

from verily.ds_sdk.core.schemas import DataPoint
from verily.ds_sdk.core.schemas import dataspec


@dataspec('com.verily.careinnovations.weight')
@dataclasses.dataclass
class CareinnovationsWeight(DataPoint):
    """Beam RowSchema for com.verily.careinnovations.weight."""
    weight: float
