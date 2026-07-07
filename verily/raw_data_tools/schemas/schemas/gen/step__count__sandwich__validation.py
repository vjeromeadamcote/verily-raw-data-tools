# Lint as: python3
# pylint: disable=invalid-name
"""Registers the schema for com.verily.step_count_sandwich_validation with Beam."""  # pylint: disable=line-too-long

import dataclasses
from typing import Optional

from verily.raw_data_tools.schemas.schemas.shared_schemas import DataPoint
from verily.raw_data_tools.schemas.schemas.decorators import dataspec


@dataspec('com.verily.step_count_sandwich_validation')
@dataclasses.dataclass
class Step_Count_Sandwich_Validation(DataPoint):
    """Beam RowSchema for com.verily.step_count_sandwich_validation."""
    step_count: float
    step_interval: float
    step_filter: Optional[str] = None
