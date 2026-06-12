# Lint as: python3
# pylint: disable=invalid-name
"""Registers the schema for com.verily.overnight_heart_metrics with Beam."""

import dataclasses
from typing import Optional

from verily.ds_sdk.core.schemas import DataPoint
from verily.ds_sdk.core.schemas import dataspec


@dataspec('com.verily.overnight_heart_metrics')
@dataclasses.dataclass
class Overnight_Heart_Metrics(DataPoint):
    """Beam RowSchema for com.verily.overnight_heart_metrics."""
    confidence: Optional[int] = None
    imu_ppg_availability: Optional[float] = None
    nrmssd_mean: Optional[float] = None
    nrmssd_overall: Optional[float] = None
    nsdnn_index: Optional[float] = None
    nsdnn_overall: Optional[float] = None
    num_beats: Optional[int] = None
    p_hf: Optional[float] = None
    p_hf_relative: Optional[float] = None
    p_lf: Optional[float] = None
    p_lf_relative: Optional[float] = None
    p_vlf: Optional[float] = None
    peak_hf: Optional[float] = None
    peak_lf: Optional[float] = None
    ratio_lf_hf: Optional[float] = None
    rhr: Optional[float] = None
    rmssd_mean: Optional[float] = None
    rmssd_overall: Optional[float] = None
    sdnn_index: Optional[float] = None
    sdnn_overall: Optional[float] = None
    total_sleep: Optional[int] = None
