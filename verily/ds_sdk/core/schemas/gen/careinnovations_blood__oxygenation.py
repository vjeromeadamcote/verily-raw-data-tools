# Lint as: python3
# pylint: disable=invalid-name
"""Registers the schema for com.verily.careinnovations.blood_oxygenation with Beam."""  # pylint: disable=line-too-long

import dataclasses

from verily.ds_sdk.core.schemas import DataPoint
from verily.ds_sdk.core.schemas import dataspec


@dataspec('com.verily.careinnovations.blood_oxygenation')
@dataclasses.dataclass
class CareinnovationsBlood_Oxygenation(DataPoint):
    """Beam RowSchema for com.verily.careinnovations.blood_oxygenation."""
    blood_oxygenation: float
