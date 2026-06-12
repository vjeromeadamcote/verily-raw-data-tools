# Lint as: python3
# pylint: disable=invalid-name
"""Registers the schema for com.verily.actigraphy_counts with Beam."""

import dataclasses

from verily.ds_sdk.core.schemas import DataPoint
from verily.ds_sdk.core.schemas import dataspec


@dataspec('com.verily.actigraphy_counts')
@dataclasses.dataclass
class Actigraphy_Counts(DataPoint):
    """Beam RowSchema for com.verily.actigraphy_counts."""
    count: int
