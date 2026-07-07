# Lint as: python3
# pylint: disable=invalid-name
"""Registers the schema for com.verily.irregular_heartbeat with Beam."""

import dataclasses

from verily.raw_data_tools.schemas.schemas.shared_schemas import DataPoint
from verily.raw_data_tools.schemas.schemas.decorators import dataspec


@dataspec('com.verily.irregular_heartbeat')
@dataclasses.dataclass
class Irregular_Heartbeat(DataPoint):
    """Beam RowSchema for com.verily.irregular_heartbeat."""
    is_irregular: bool
