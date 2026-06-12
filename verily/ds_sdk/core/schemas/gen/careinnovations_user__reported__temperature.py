# Lint as: python3
# pylint: disable=invalid-name
"""Registers the schema for com.verily.careinnovations.user_reported_temperature with Beam."""  # pylint: disable=line-too-long

import dataclasses

from verily.ds_sdk.core.schemas import DataPoint
from verily.ds_sdk.core.schemas import dataspec


@dataspec('com.verily.careinnovations.user_reported_temperature')
@dataclasses.dataclass
class CareinnovationsUser_Reported_Temperature(DataPoint):
    """Beam RowSchema for com.verily.careinnovations.user_reported_temperature."""  # pylint: disable=line-too-long
    temperature: float
