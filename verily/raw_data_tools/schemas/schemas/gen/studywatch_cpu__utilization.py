# Lint as: python3
# pylint: disable=invalid-name
"""Registers the schema for com.verily.studywatch.cpu_utilization with Beam."""

import dataclasses
from typing import Optional

from verily.raw_data_tools.schemas.schemas.shared_schemas import DataPoint
from verily.raw_data_tools.schemas.schemas.decorators import dataspec


@dataspec('com.verily.studywatch.cpu_utilization')
@dataclasses.dataclass
class StudywatchCpu_Utilization(DataPoint):
    """Beam RowSchema for com.verily.studywatch.cpu_utilization."""
    critical_util_pct: Optional[int] = None
    high_util_pct: Optional[int] = None
    low_util_pct: Optional[int] = None
    normal_util_pct: Optional[int] = None
