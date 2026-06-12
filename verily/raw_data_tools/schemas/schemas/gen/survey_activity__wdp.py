# Lint as: python3
# pylint: disable=invalid-name
"""Registers the schema for com.verily.survey.activity_wdp with Beam."""

import dataclasses
from typing import Optional

from verily.ds_sdk.core.schemas import DataPoint
from verily.ds_sdk.core.schemas import dataspec


@dataspec('com.verily.survey.activity_wdp')
@dataclasses.dataclass
class SurveyActivity_Wdp(DataPoint):
    """Beam RowSchema for com.verily.survey.activity_wdp."""
    activity_minutes_error: str
    activity_minutes_time: str
    dont_recall: bool
    everything_good: bool
    step_count_error: str
    step_count_time: str
    timezone_render: str
    device_id: Optional[str] = None
