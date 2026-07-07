# Lint as: python3
# pylint: disable=invalid-name
"""Registers the schema for com.verily.tpatch.hexoskin with Beam."""

import dataclasses
from typing import Optional

from verily.raw_data_tools.schemas.schemas.shared_schemas import DataPoint
from verily.raw_data_tools.schemas.schemas.decorators import dataspec


@dataspec('com.verily.tpatch.hexoskin')
@dataclasses.dataclass
class TpatchHexoskin(DataPoint):
    """Beam RowSchema for com.verily.tpatch.hexoskin."""
    activity: Optional[float] = None
    expiration: Optional[float] = None
    heart_rate_bpm: Optional[float] = None
    heart_rate_quality: Optional[float] = None
    inspiration: Optional[float] = None
    respiratory_rate_quality: Optional[float] = None
    sleep_position: Optional[float] = None
    step_count: Optional[float] = None
