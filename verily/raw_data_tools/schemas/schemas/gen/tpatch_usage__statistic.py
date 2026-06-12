# Lint as: python3
# pylint: disable=invalid-name
"""Registers the schema for com.verily.tpatch.usage_statistic with Beam."""

import dataclasses

from verily.ds_sdk.core.schemas import DataPoint
from verily.ds_sdk.core.schemas import dataspec


@dataspec('com.verily.tpatch.usage_statistic')
@dataclasses.dataclass
class TpatchUsage_Statistic(DataPoint):
    """Beam RowSchema for com.verily.tpatch.usage_statistic."""
    count: int
    stat: str
