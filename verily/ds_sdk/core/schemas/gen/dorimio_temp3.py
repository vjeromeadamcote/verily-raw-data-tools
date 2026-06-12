# Lint as: python3
# pylint: disable=invalid-name
"""Registers the schema for com.verily.dorimio.temp3 with Beam."""

import dataclasses
from typing import List

from verily.ds_sdk.core.schemas import DataPoint
from verily.ds_sdk.core.schemas import dataspec


@dataspec('com.verily.dorimio.temp3')
@dataclasses.dataclass
class DorimioTemp3(DataPoint):
    """Beam RowSchema for com.verily.dorimio.temp3."""
    value1: List[int]
    value2: List[int]
