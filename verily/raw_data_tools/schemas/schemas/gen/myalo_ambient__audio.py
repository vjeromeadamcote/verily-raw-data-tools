# Lint as: python3
# pylint: disable=invalid-name
"""Registers the schema for com.verily.myalo.ambient_audio with Beam."""

import dataclasses
from typing import Optional

from verily.raw_data_tools.schemas.schemas.shared_schemas import DataPoint
from verily.raw_data_tools.schemas.schemas.decorators import dataspec


@dataspec('com.verily.myalo.ambient_audio')
@dataclasses.dataclass
class MyaloAmbient_Audio(DataPoint):
    """Beam RowSchema for com.verily.myalo.ambient_audio."""
    noise_level: Optional[int] = None
    reported_time: Optional[int] = None
