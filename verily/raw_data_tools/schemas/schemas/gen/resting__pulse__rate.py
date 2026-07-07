# Lint as: python3
# pylint: disable=invalid-name
"""Registers the schema for com.verily.resting_pulse_rate with Beam."""

import dataclasses
from typing import Optional

from verily.raw_data_tools.schemas.schemas.shared_schemas import DataPoint
from verily.raw_data_tools.schemas.schemas.decorators import dataspec


@dataspec('com.verily.resting_pulse_rate')
@dataclasses.dataclass
class Resting_Pulse_Rate(DataPoint):
    """Beam RowSchema for com.verily.resting_pulse_rate."""
    acc_stddev: Optional[float] = None
    beat_quality_estimate: Optional[float] = None
    confidence: Optional[float] = None
    gyro_stddev: Optional[float] = None
    is_rest: Optional[bool] = None
    motion_flag: Optional[bool] = None
    pulse_rate: Optional[float] = None
    pulse_rate_psd: Optional[float] = None
    quality_label: Optional[bool] = None
    quality_score: Optional[float] = None
    relative_power: Optional[float] = None
    spectral_snr_db: Optional[float] = None
    status_code: Optional[int] = None
