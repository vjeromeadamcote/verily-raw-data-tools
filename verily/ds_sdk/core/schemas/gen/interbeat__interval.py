# Lint as: python3
# pylint: disable=invalid-name
"""Registers the schema for com.verily.interbeat_interval with Beam."""

import dataclasses
from typing import Optional

from apache_beam.utils.timestamp import Timestamp

from verily.ds_sdk.core.schemas import DataPoint
from verily.ds_sdk.core.schemas import dataspec


@dataspec('com.verily.interbeat_interval')
@dataclasses.dataclass
class Interbeat_Interval(DataPoint):
    """Beam RowSchema for com.verily.interbeat_interval."""
    interbeat_interval: int
    delta_time: Optional[int] = None
    error_estimate: Optional[int] = None
    jump_measure: Optional[float] = None
    jump_threshold: Optional[int] = None
    originating_packet_timestamp: Optional[Timestamp] = None
    quality_ibi: Optional[bool] = None
