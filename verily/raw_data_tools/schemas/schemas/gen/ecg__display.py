# Lint as: python3
# pylint: disable=invalid-name
"""Registers the schema for com.verily.ecg_display with Beam."""

import dataclasses
from typing import List, Optional

from verily.ds_sdk.core.schemas import DataPoint
from verily.ds_sdk.core.schemas import dataspec


@dataspec('com.verily.ecg_display')
@dataclasses.dataclass
class Ecg_Display(DataPoint):
    """Beam RowSchema for com.verily.ecg_display."""
    effective_sampling_rate: Optional[float] = None
    finger_wrist: Optional[List[float]] = None
    finger_wrists_confidence: Optional[List[int]] = None
    finger_wrists_value: Optional[List[float]] = None
