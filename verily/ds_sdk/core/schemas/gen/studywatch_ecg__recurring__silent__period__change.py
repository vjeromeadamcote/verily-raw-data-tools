# Lint as: python3
# pylint: disable=invalid-name
"""Registers the schema for com.verily.studywatch.ecg_recurring_silent_period_change with Beam."""  # pylint: disable=line-too-long

import dataclasses
from typing import Optional

from verily.ds_sdk.core.schemas import DataPoint
from verily.ds_sdk.core.schemas import dataspec


@dataspec('com.verily.studywatch.ecg_recurring_silent_period_change')
@dataclasses.dataclass
class StudywatchEcg_Recurring_Silent_Period_Change(DataPoint):
    """Beam RowSchema for com.verily.studywatch.ecg_recurring_silent_period_change."""  # pylint: disable=line-too-long
    end_minutes: Optional[int] = None
    is_enabled: Optional[bool] = None
    start_minutes: Optional[int] = None
