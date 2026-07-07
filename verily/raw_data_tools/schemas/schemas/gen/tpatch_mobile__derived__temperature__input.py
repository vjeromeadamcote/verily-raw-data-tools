# Lint as: python3
# pylint: disable=invalid-name
"""Registers the schema for com.verily.tpatch.mobile_derived_temperature_input with Beam."""  # pylint: disable=line-too-long

import dataclasses

from verily.raw_data_tools.schemas.schemas.shared_schemas import DataPoint
from verily.raw_data_tools.schemas.schemas.decorators import dataspec


@dataspec('com.verily.tpatch.mobile_derived_temperature_input')
@dataclasses.dataclass
class TpatchMobile_Derived_Temperature_Input(DataPoint):
    """Beam RowSchema for com.verily.tpatch.mobile_derived_temperature_input."""
    ambient_temperature: int
    app_version: str
    operating_system: str
    skin_temperature: int
