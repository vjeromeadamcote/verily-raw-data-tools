# Lint as: python3
# pylint: disable=invalid-name
"""Registers the schema for com.verily.tpatch.usage_statistic with Beam."""

import dataclasses

from verily.raw_data_tools.schemas.schemas.shared_schemas import DataPoint
from verily.raw_data_tools.schemas.schemas.decorators import dataspec


@dataspec('com.verily.tpatch.usage_statistic')
@dataclasses.dataclass
class TpatchUsage_Statistic(DataPoint):
    """Beam RowSchema for com.verily.tpatch.usage_statistic."""
    count: int
    stat: str
