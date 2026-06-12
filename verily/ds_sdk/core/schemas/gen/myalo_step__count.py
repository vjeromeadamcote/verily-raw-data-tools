# Lint as: python3
# pylint: disable=invalid-name
"""Registers the schema for com.verily.myalo.step_count with Beam."""

import dataclasses
from typing import Optional

from verily.ds_sdk.core.schemas import DataPoint
from verily.ds_sdk.core.schemas import dataspec


@dataspec('com.verily.myalo.step_count')
@dataclasses.dataclass
class MyaloStep_Count(DataPoint):
    """Beam RowSchema for com.verily.myalo.step_count."""
    delta_duration: Optional[int] = None
    delta_step_count: Optional[int] = None
    raw_step_count: Optional[int] = None
    reported_time: Optional[int] = None
    sample_time: Optional[int] = None
