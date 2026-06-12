# Lint as: python3
# pylint: disable=invalid-name
"""Registers the schema for com.verily.tpatch.derived_core_temperature with Beam."""  # pylint: disable=line-too-long

import dataclasses

from verily.ds_sdk.core.schemas import DataPoint
from verily.ds_sdk.core.schemas import dataspec


@dataspec('com.verily.tpatch.derived_core_temperature')
@dataclasses.dataclass
class TpatchDerived_Core_Temperature(DataPoint):
    """Beam RowSchema for com.verily.tpatch.derived_core_temperature."""
    temperature: int
