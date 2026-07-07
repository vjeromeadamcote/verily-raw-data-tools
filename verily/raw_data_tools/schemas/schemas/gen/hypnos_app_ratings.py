# Lint as: python3
# pylint: disable=invalid-name
"""Registers the schema for com.verily.hypnos.app.ratings with Beam."""

import dataclasses
from typing import List, Optional

from apache_beam.utils.timestamp import Timestamp

from verily.raw_data_tools.schemas.schemas.shared_schemas import DataPoint
from verily.raw_data_tools.schemas.schemas.decorators import dataspec


@dataspec('com.verily.hypnos.app.ratings')
@dataclasses.dataclass
class HypnosAppRatings(DataPoint):
    """Beam RowSchema for com.verily.hypnos.app.ratings."""
    rating: float
    recorded_timestamp: Timestamp
    reasons: Optional[List[str]] = None
