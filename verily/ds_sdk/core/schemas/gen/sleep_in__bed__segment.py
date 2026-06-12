# Lint as: python3
# pylint: disable=invalid-name
"""Registers the schema for com.verily.sleep.in_bed_segment with Beam."""

import dataclasses
from typing import Optional

from verily.ds_sdk.core.schemas import DataPoint
from verily.ds_sdk.core.schemas import dataspec


@dataspec('com.verily.sleep.in_bed_segment')
@dataclasses.dataclass
class SleepIn_Bed_Segment(DataPoint):
    """Beam RowSchema for com.verily.sleep.in_bed_segment."""
    end_time: int
    start_time: int
    algorithm_param_max_in_bed_gap: Optional[int] = None
    algorithm_type: Optional[str] = None
    end_tod_offset: Optional[int] = None
    quality_end_time: Optional[int] = None
    quality_num_datapoints: Optional[int] = None
    quality_num_micro_segments: Optional[int] = None
    quality_start_time: Optional[int] = None
    start_tod_offset: Optional[int] = None
