# Lint as: python3
# pylint: disable=invalid-name
"""Registers the schema for com.verily.careinnovations.spirometry with Beam."""

import dataclasses
from typing import Optional

from verily.ds_sdk.core.schemas import DataPoint
from verily.ds_sdk.core.schemas import dataspec


@dataspec('com.verily.careinnovations.spirometry')
@dataclasses.dataclass
class CareinnovationsSpirometry(DataPoint):
    """Beam RowSchema for com.verily.careinnovations.spirometry."""
    forced_expiratory_volume1: Optional[float] = None
    forced_vital_capacity: Optional[float] = None
    peak_expiratory_flow: Optional[float] = None
