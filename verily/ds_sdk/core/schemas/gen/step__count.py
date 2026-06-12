# Lint as: python3
# pylint: disable=invalid-name
"""Registers the schema for com.verily.step_count with Beam."""

import dataclasses
from typing import Optional

from verily.ds_sdk.core.schemas import DataPoint
from verily.ds_sdk.core.schemas import dataspec


@dataspec('com.verily.step_count')
@dataclasses.dataclass
class Step_Count(DataPoint):
    """Beam RowSchema for com.verily.step_count."""
    step_count: float
    step_interval: float
    step_filter: Optional[str] = None
