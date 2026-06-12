# Lint as: python3
# pylint: disable=invalid-name
"""Registers the schema for com.verily.ppg_signal_quality with Beam."""

import dataclasses
from typing import Optional

from verily.ds_sdk.core.schemas import DataPoint
from verily.ds_sdk.core.schemas import dataspec


@dataspec('com.verily.ppg_signal_quality')
@dataclasses.dataclass
class Ppg_Signal_Quality(DataPoint):
    """Beam RowSchema for com.verily.ppg_signal_quality."""
    bin1: Optional[float] = None
    bin2: Optional[float] = None
    bin3: Optional[float] = None
    bin4: Optional[float] = None
    ddx_bin1: Optional[float] = None
    ddx_bin2: Optional[float] = None
    ddx_bin3: Optional[float] = None
    ddx_bin4: Optional[float] = None
    ddx_shannon_entropy: Optional[float] = None
    dx_bin1: Optional[float] = None
    dx_bin2: Optional[float] = None
    dx_bin3: Optional[float] = None
    dx_bin4: Optional[float] = None
    dx_shannon_entropy: Optional[float] = None
    max_filtered: Optional[float] = None
    median_ibi: Optional[float] = None
    min_filtered: Optional[float] = None
    quality_label: Optional[bool] = None
    shannon_entropy: Optional[float] = None
    shannon_entropy_good: Optional[bool] = None
    snr: Optional[float] = None
