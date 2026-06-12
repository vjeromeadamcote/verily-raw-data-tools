# Lint as: python3
# pylint: disable=invalid-name
"""Registers the schema for com.verily.picard.ppg with Beam."""

import dataclasses
from typing import List, Optional

from verily.ds_sdk.core.schemas import DataPoint
from verily.ds_sdk.core.schemas import dataspec


@dataspec('com.verily.picard.ppg')
@dataclasses.dataclass
class PicardPpg(DataPoint):
    """Beam RowSchema for com.verily.picard.ppg."""
    channel1: Optional[List[int]] = None
    channel2: Optional[List[int]] = None
    raw_adc: Optional[List[int]] = None
    sampling_rate: Optional[int] = None
