# Lint as: python3
# pylint: disable=invalid-name
"""Registers the schema for com.verily.mimic.ppg with Beam."""

import dataclasses
from typing import List, Optional

from verily.raw_data_tools.schemas.schemas.shared_schemas import DataPoint
from verily.raw_data_tools.schemas.schemas.decorators import dataspec


@dataspec('com.verily.mimic.ppg')
@dataclasses.dataclass
class MimicPpg(DataPoint):
    """Beam RowSchema for com.verily.mimic.ppg."""
    samples: Optional[List[float]] = None
    sampling_rate: Optional[int] = None
