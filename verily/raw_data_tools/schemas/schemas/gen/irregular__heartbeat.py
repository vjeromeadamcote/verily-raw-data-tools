# Lint as: python3
# pylint: disable=invalid-name
"""Registers the schema for com.verily.irregular_heartbeat with Beam."""

import dataclasses

from verily.ds_sdk.core.schemas import DataPoint
from verily.ds_sdk.core.schemas import dataspec


@dataspec('com.verily.irregular_heartbeat')
@dataclasses.dataclass
class Irregular_Heartbeat(DataPoint):
    """Beam RowSchema for com.verily.irregular_heartbeat."""
    is_irregular: bool
