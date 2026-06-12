# Lint as: python3
# pylint: disable=invalid-name
"""Registers the schema for com.verily.studyhub.processor_stats with Beam."""

import dataclasses
from typing import Optional

from verily.ds_sdk.core.schemas import DataPoint
from verily.ds_sdk.core.schemas import dataspec


@dataspec('com.verily.studyhub.processor_stats')
@dataclasses.dataclass
class StudyhubProcessor_Stats(DataPoint):
    """Beam RowSchema for com.verily.studyhub.processor_stats."""
    cpu_temperature: Optional[float] = None
    gpu_temperature: Optional[float] = None
