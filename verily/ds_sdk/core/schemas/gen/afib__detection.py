# Lint as: python3
# pylint: disable=invalid-name
"""Registers the schema for com.verily.afib_detection with Beam."""

import dataclasses
from typing import Optional

from verily.ds_sdk.core.schemas import DataPoint
from verily.ds_sdk.core.schemas import dataspec


@dataspec('com.verily.afib_detection')
@dataclasses.dataclass
class Afib_Detection(DataPoint):
    """Beam RowSchema for com.verily.afib_detection."""
    afib_burden: Optional[float] = None
    confidence: Optional[int] = None
    heart_rate_bpm: Optional[int] = None
    motion_estimate: Optional[int] = None
    num_samples_processed: Optional[int] = None
    percentage_reporting: Optional[int] = None
