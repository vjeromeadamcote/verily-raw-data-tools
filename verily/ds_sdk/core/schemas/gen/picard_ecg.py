# Lint as: python3
# pylint: disable=invalid-name
"""Registers the schema for com.verily.picard.ecg with Beam."""

import dataclasses
from typing import List, Optional

from verily.ds_sdk.core.schemas import DataPoint
from verily.ds_sdk.core.schemas import dataspec


@dataspec('com.verily.picard.ecg')
@dataclasses.dataclass
class PicardEcg(DataPoint):
    """Beam RowSchema for com.verily.picard.ecg."""
    raw_adc: Optional[List[int]] = None
    sampling_rate: Optional[int] = None
