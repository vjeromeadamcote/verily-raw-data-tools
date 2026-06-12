# Lint as: python3
# pylint: disable=invalid-name
"""Registers the schema for com.verily.tpatch.mobile_derived_temperature_output with Beam."""  # pylint: disable=line-too-long

import dataclasses

from verily.ds_sdk.core.schemas import DataPoint
from verily.ds_sdk.core.schemas import dataspec


@dataspec('com.verily.tpatch.mobile_derived_temperature_output')
@dataclasses.dataclass
class TpatchMobile_Derived_Temperature_Output(DataPoint):
    """Beam RowSchema for com.verily.tpatch.mobile_derived_temperature_output."""  # pylint: disable=line-too-long
    app_version: str
    derived_temperature: int
    operating_system: str
