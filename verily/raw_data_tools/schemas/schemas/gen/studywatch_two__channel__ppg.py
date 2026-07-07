# Lint as: python3
# pylint: disable=invalid-name
"""Registers the schema for com.verily.studywatch.two_channel_ppg with Beam."""

import dataclasses
from typing import List, Optional

from verily.raw_data_tools.schemas.schemas.shared_schemas import DataPoint
from verily.raw_data_tools.schemas.schemas.decorators import dataspec


@dataspec('com.verily.studywatch.two_channel_ppg')
@dataclasses.dataclass
class StudywatchTwo_Channel_Ppg(DataPoint):
    """Beam RowSchema for com.verily.studywatch.two_channel_ppg."""
    green: Optional[List[int]] = None
    green_2: Optional[List[int]] = None
    infrared: Optional[List[int]] = None
    infrared_2: Optional[List[int]] = None
    red: Optional[List[int]] = None
    red_2: Optional[List[int]] = None
    sampling_rate: Optional[int] = None
    sensor_id: Optional[int] = None
    tag: Optional[List[int]] = None
    tag_1: Optional[List[int]] = None
    tag_2: Optional[List[int]] = None
    true_timestamp_millis: Optional[int] = None
    true_timestamp_sample_index: Optional[int] = None
    wavelength_green: Optional[float] = None
    wavelength_infrared: Optional[float] = None
    wavelength_red: Optional[float] = None
