# Lint as: python3
# pylint: disable=invalid-name
"""Registers the schema for com.verily.trestle.knee_motion with Beam."""

import dataclasses
from typing import List

from verily.raw_data_tools.schemas.schemas.shared_schemas import DataPoint
from verily.raw_data_tools.schemas.schemas.decorators import dataspec


@dataspec('com.verily.trestle.knee_motion')
@dataclasses.dataclass
class TrestleKnee_Motion(DataPoint):
    """Beam RowSchema for com.verily.trestle.knee_motion."""
    angle_deg: List[float]
    cos_count: List[int]
    sampling_rate: int
    sin_count: List[int]
