# Lint as: python3
# pylint: disable=invalid-name
"""Registers the schema for com.verily.in_bed_segment_test with Beam."""

import dataclasses
from typing import Optional

from verily.ds_sdk.core.schemas import DataPoint
from verily.ds_sdk.core.schemas import dataspec


@dataspec('com.verily.in_bed_segment_test')
@dataclasses.dataclass
class In_Bed_Segment_Test(DataPoint):
    """Beam RowSchema for com.verily.in_bed_segment_test."""
    end_time: int
    start_time: int
    algorithm_param_max_in_bed_gap: Optional[int] = None
    algorithm_type: Optional[str] = None
