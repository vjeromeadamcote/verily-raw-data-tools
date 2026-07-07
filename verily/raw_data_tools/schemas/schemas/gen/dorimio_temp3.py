# Lint as: python3
# pylint: disable=invalid-name
"""Registers the schema for com.verily.dorimio.temp3 with Beam."""

import dataclasses
from typing import List

from verily.raw_data_tools.schemas.schemas.shared_schemas import DataPoint
from verily.raw_data_tools.schemas.schemas.decorators import dataspec


@dataspec('com.verily.dorimio.temp3')
@dataclasses.dataclass
class DorimioTemp3(DataPoint):
    """Beam RowSchema for com.verily.dorimio.temp3."""
    value1: List[int]
    value2: List[int]
