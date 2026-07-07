# Lint as: python3
# pylint: disable=invalid-name
"""Registers the schema for com.verily.ppg with Beam."""

import dataclasses
from typing import List, Optional

from apache_beam.utils.timestamp import Timestamp

from verily.raw_data_tools.schemas.schemas.shared_schemas import DataPoint
from verily.raw_data_tools.schemas.schemas.decorators import dataspec


@dataspec('com.verily.ppg')
@dataclasses.dataclass
class Ppg(DataPoint):
    """Beam RowSchema for com.verily.ppg."""
    green: Optional[List[int]] = None
    infrared: Optional[List[int]] = None
    red: Optional[List[int]] = None
    sampling_rate: Optional[int] = None
    sensor_id: Optional[int] = None
    true_timestamp_millis: Optional[Timestamp] = None
    true_timestamp_sample_index: Optional[int] = None
    wavelength_green: Optional[float] = None
    wavelength_infrared: Optional[float] = None
    wavelength_red: Optional[float] = None
