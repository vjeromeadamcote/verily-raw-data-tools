# Lint as: python3
# pylint: disable=invalid-name
"""Registers the schema for com.verily.step_count_sandwich_validation with Beam."""  # pylint: disable=line-too-long

import dataclasses
from typing import Optional

from verily.ds_sdk.core.schemas import DataPoint
from verily.ds_sdk.core.schemas import dataspec


@dataspec('com.verily.step_count_sandwich_validation')
@dataclasses.dataclass
class Step_Count_Sandwich_Validation(DataPoint):
    """Beam RowSchema for com.verily.step_count_sandwich_validation."""
    step_count: float
    step_interval: float
    step_filter: Optional[str] = None
