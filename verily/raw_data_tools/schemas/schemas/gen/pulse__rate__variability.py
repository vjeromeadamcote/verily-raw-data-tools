# Lint as: python3
# pylint: disable=invalid-name
"""Registers the schema for com.verily.pulse_rate_variability with Beam."""

import dataclasses
from typing import Optional

from verily.ds_sdk.core.schemas import DataPoint
from verily.ds_sdk.core.schemas import dataspec


@dataspec('com.verily.pulse_rate_variability')
@dataclasses.dataclass
class Pulse_Rate_Variability(DataPoint):
    """Beam RowSchema for com.verily.pulse_rate_variability."""
    mean_pulse_rate: float
    number_intervals: int
    sdnn: float
    time_interval: float
    alpha1: Optional[float] = None
    alpha2: Optional[float] = None
    approximate_entropy: Optional[float] = None
    confidence: Optional[int] = None
    hf_nu: Optional[float] = None
    hf_percent: Optional[float] = None
    hf_power: Optional[float] = None
    hrv_triangle: Optional[float] = None
    lf_nu: Optional[float] = None
    lf_percent: Optional[float] = None
    lf_power: Optional[float] = None
    lfhf_ratio: Optional[float] = None
    mean: Optional[float] = None
    nn50: Optional[int] = None
    pnn50: Optional[float] = None
    poincare_ratio: Optional[float] = None
    poincare_s: Optional[float] = None
    poincare_sd1: Optional[float] = None
    poincare_sd2: Optional[float] = None
    rmssd: Optional[float] = None
    sample_entropy: Optional[float] = None
    tinn: Optional[float] = None
    ulf_percent: Optional[float] = None
    ulf_power: Optional[float] = None
    vlf_percent: Optional[float] = None
    vlf_power: Optional[float] = None
