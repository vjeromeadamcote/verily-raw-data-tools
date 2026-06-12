# Lint as: python3
# pylint: disable=invalid-name
"""Registers the schema for com.verily.r_wave with Beam."""

import dataclasses
from typing import Optional

from apache_beam.utils.timestamp import Timestamp

from verily.ds_sdk.core.schemas import DataPoint
from verily.ds_sdk.core.schemas import dataspec


@dataspec('com.verily.r_wave')
@dataclasses.dataclass
class R_Wave(DataPoint):
    """Beam RowSchema for com.verily.r_wave."""
    on_left_hand: Optional[bool] = None
    rr_interval_confidence: Optional[float] = None
    rr_interval_seconds: Optional[float] = None
    timestamp_millis: Optional[Timestamp] = None
