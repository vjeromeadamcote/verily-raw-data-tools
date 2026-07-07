# Lint as: python3
# pylint: disable=invalid-name
"""Registers the schema for com.verily.piezo with Beam."""

import dataclasses
from typing import List, Optional

from verily.raw_data_tools.schemas.schemas.shared_schemas import DataPoint
from verily.raw_data_tools.schemas.schemas.decorators import dataspec


@dataspec('com.verily.piezo')
@dataclasses.dataclass
class Piezo(DataPoint):
    """Beam RowSchema for com.verily.piezo."""
    raw_adc: Optional[List[int]] = None
    sampling_rate: Optional[int] = None
