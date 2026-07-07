# Lint as: python3
# pylint: disable=invalid-name
"""Registers the schema for com.verily.actigraphy_counts with Beam."""

import dataclasses

from verily.raw_data_tools.schemas.schemas.shared_schemas import DataPoint
from verily.raw_data_tools.schemas.schemas.decorators import dataspec


@dataspec('com.verily.actigraphy_counts')
@dataclasses.dataclass
class Actigraphy_Counts(DataPoint):
    """Beam RowSchema for com.verily.actigraphy_counts."""
    count: int
