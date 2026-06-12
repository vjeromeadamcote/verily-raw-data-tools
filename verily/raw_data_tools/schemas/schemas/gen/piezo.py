# Lint as: python3
# pylint: disable=invalid-name
"""Registers the schema for com.verily.piezo with Beam."""

import dataclasses
from typing import List, Optional

from verily.ds_sdk.core.schemas import DataPoint
from verily.ds_sdk.core.schemas import dataspec


@dataspec('com.verily.piezo')
@dataclasses.dataclass
class Piezo(DataPoint):
    """Beam RowSchema for com.verily.piezo."""
    raw_adc: Optional[List[int]] = None
    sampling_rate: Optional[int] = None
