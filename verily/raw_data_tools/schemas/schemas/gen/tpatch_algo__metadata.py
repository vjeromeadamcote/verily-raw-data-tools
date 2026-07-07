# Lint as: python3
# pylint: disable=invalid-name
"""Registers the schema for com.verily.tpatch.algo_metadata with Beam."""

import dataclasses
from typing import Optional

from apache_beam.utils.timestamp import Timestamp

from verily.raw_data_tools.schemas.schemas.shared_schemas import DataPoint
from verily.raw_data_tools.schemas.schemas.decorators import dataspec


@dataspec('com.verily.tpatch.algo_metadata')
@dataclasses.dataclass
class TpatchAlgo_Metadata(DataPoint):
    """Beam RowSchema for com.verily.tpatch.algo_metadata."""
    event_time: Optional[int] = None
    phone_event_timestamp: Optional[Timestamp] = None
    rtc_timestamp_first_sample: Optional[Timestamp] = None
    rtc_timestamp_now: Optional[Timestamp] = None
