# Lint as: python3
# pylint: disable=invalid-name
"""Registers the schema for com.verily.r_wave with Beam."""

import dataclasses
from typing import Optional

from apache_beam.utils.timestamp import Timestamp

from verily.raw_data_tools.schemas.schemas.shared_schemas import DataPoint
from verily.raw_data_tools.schemas.schemas.decorators import dataspec


@dataspec('com.verily.r_wave')
@dataclasses.dataclass
class R_Wave(DataPoint):
    """Beam RowSchema for com.verily.r_wave."""
    on_left_hand: Optional[bool] = None
    rr_interval_confidence: Optional[float] = None
    rr_interval_seconds: Optional[float] = None
    timestamp_millis: Optional[Timestamp] = None
