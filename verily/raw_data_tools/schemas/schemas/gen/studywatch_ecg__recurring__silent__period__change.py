# Lint as: python3
# pylint: disable=invalid-name
"""Registers the schema for com.verily.studywatch.ecg_recurring_silent_period_change with Beam."""  # pylint: disable=line-too-long

import dataclasses
from typing import Optional

from verily.raw_data_tools.schemas.schemas.shared_schemas import DataPoint
from verily.raw_data_tools.schemas.schemas.decorators import dataspec


@dataspec('com.verily.studywatch.ecg_recurring_silent_period_change')
@dataclasses.dataclass
class StudywatchEcg_Recurring_Silent_Period_Change(DataPoint):
    """Beam RowSchema for com.verily.studywatch.ecg_recurring_silent_period_change."""  # pylint: disable=line-too-long
    end_minutes: Optional[int] = None
    is_enabled: Optional[bool] = None
    start_minutes: Optional[int] = None
