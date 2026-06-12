# Lint as: python3
# pylint: disable=invalid-name
"""Registers the schema for com.verily.dormio.temp5 with Beam."""

import dataclasses
from typing import List

from verily.ds_sdk.core.schemas import DataPoint
from verily.ds_sdk.core.schemas import dataspec


@dataspec('com.verily.dormio.temp5')
@dataclasses.dataclass
class DormioTemp5(DataPoint):
    """Beam RowSchema for com.verily.dormio.temp5."""
    sampling_rate: int
    value1: List[int]
    value2: List[int]
