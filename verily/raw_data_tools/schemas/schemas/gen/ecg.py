# Lint as: python3
# pylint: disable=invalid-name
"""Registers the schema for com.verily.ecg with Beam."""

import dataclasses
from typing import List, Optional

from apache_beam.utils.timestamp import Timestamp

from verily.ds_sdk.core.schemas import DataPoint
from verily.ds_sdk.core.schemas import dataspec


@dataspec('com.verily.ecg')
@dataclasses.dataclass
class Ecg(DataPoint):
    """Beam RowSchema for com.verily.ecg."""
    raw_adc: Optional[List[int]] = None
    sampling_rate: Optional[int] = None
    sensor_id: Optional[int] = None
    true_timestamp_millis: Optional[Timestamp] = None
    true_timestamp_sample_index: Optional[int] = None
