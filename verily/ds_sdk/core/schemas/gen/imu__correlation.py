# Lint as: python3
# pylint: disable=invalid-name
"""Registers the schema for com.verily.imu_correlation with Beam."""

import dataclasses
from typing import Optional

from verily.ds_sdk.core.schemas import DataPoint
from verily.ds_sdk.core.schemas import dataspec


@dataspec('com.verily.imu_correlation')
@dataclasses.dataclass
class Imu_Correlation(DataPoint):
    """Beam RowSchema for com.verily.imu_correlation."""
    count: Optional[int] = None
    duration_ms: Optional[float] = None
    imu_ppg_availability: Optional[float] = None
    xy: Optional[float] = None
    yz: Optional[float] = None
    zx: Optional[float] = None
