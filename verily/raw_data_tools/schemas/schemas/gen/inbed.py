# Lint as: python3
# pylint: disable=invalid-name
"""Registers the schema for com.verily.inbed with Beam."""

import dataclasses

from verily.raw_data_tools.schemas.schemas.shared_schemas import DataPoint
from verily.raw_data_tools.schemas.schemas.decorators import dataspec


@dataspec('com.verily.inbed')
@dataclasses.dataclass
class Inbed(DataPoint):
    """Beam RowSchema for com.verily.inbed."""
    seconds: float
