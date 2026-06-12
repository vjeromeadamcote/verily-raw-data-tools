# Lint as: python3
# pylint: disable=invalid-name
"""Registers the schema for com.verily.sleep.stages with Beam."""

import dataclasses
from typing import Optional

from verily.ds_sdk.core.schemas import DataPoint
from verily.ds_sdk.core.schemas import dataspec


@dataspec('com.verily.sleep.stages')
@dataclasses.dataclass
class SleepStages(DataPoint):
    """Beam RowSchema for com.verily.sleep.stages."""
    duration_millis: int
    stage: str
    availability_bad_signal_quality: Optional[bool] = None
    availability_gap_too_large: Optional[bool] = None
    availability_insufficient_coverage: Optional[bool] = None
    confidence: Optional[int] = None
    imu_ppg_availability: Optional[float] = None
    ppg_signal_quality: Optional[float] = None
    ppg_signal_quality_good: Optional[bool] = None
    pr_sleep: Optional[float] = None
    sleep: Optional[int] = None
