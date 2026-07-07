# Lint as: python3
# pylint: disable=invalid-name
"""Registers the schema for com.verily.wearing with Beam."""

import dataclasses

from verily.raw_data_tools.schemas.schemas.shared_schemas import DataPoint
from verily.raw_data_tools.schemas.schemas.decorators import dataspec


@dataspec('com.verily.wearing')
@dataclasses.dataclass
class Wearing(DataPoint):
    """Beam RowSchema for com.verily.wearing."""
    seconds: float
