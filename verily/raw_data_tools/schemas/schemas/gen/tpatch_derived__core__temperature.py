# Lint as: python3
# pylint: disable=invalid-name
"""Registers the schema for com.verily.tpatch.derived_core_temperature with Beam."""  # pylint: disable=line-too-long

import dataclasses

from verily.raw_data_tools.schemas.schemas.shared_schemas import DataPoint
from verily.raw_data_tools.schemas.schemas.decorators import dataspec


@dataspec('com.verily.tpatch.derived_core_temperature')
@dataclasses.dataclass
class TpatchDerived_Core_Temperature(DataPoint):
    """Beam RowSchema for com.verily.tpatch.derived_core_temperature."""
    temperature: int
