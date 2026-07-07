# Lint as: python3
# pylint: disable=invalid-name
"""Registers the schema for com.verily.activity.walking with Beam."""

import dataclasses

from verily.raw_data_tools.schemas.schemas.shared_schemas import DataPoint
from verily.raw_data_tools.schemas.schemas.decorators import dataspec


@dataspec('com.verily.activity.walking')
@dataclasses.dataclass
class ActivityWalking(DataPoint):
    """Beam RowSchema for com.verily.activity.walking."""
    seconds: float
