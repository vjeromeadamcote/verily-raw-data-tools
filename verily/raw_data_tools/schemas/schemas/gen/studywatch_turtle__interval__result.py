# Lint as: python3
# pylint: disable=invalid-name
"""Registers the schema for com.verily.studywatch.turtle_interval_result with Beam."""  # pylint: disable=line-too-long

import dataclasses
from typing import Optional

from verily.ds_sdk.core.schemas import DataPoint
from verily.ds_sdk.core.schemas import dataspec


@dataspec('com.verily.studywatch.turtle_interval_result')
@dataclasses.dataclass
class StudywatchTurtle_Interval_Result(DataPoint):
    """Beam RowSchema for com.verily.studywatch.turtle_interval_result."""
    af_avg_norm: Optional[float] = None
    is_afib: Optional[bool] = None
    res_state: Optional[str] = None
    unanalyzable_avg: Optional[float] = None
    window_count: Optional[int] = None
