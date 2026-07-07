# Lint as: python3
# pylint: disable=invalid-name
"""Registers the schema for com.verily.ziopatch.beat_data with Beam."""

import dataclasses
from typing import Optional

from verily.raw_data_tools.schemas.schemas.shared_schemas import DataPoint
from verily.raw_data_tools.schemas.schemas.decorators import dataspec


@dataspec('com.verily.ziopatch.beat_data')
@dataclasses.dataclass
class ZiopatchBeat_Data(DataPoint):
    """Beam RowSchema for com.verily.ziopatch.beat_data."""
    beat_index: Optional[int] = None
    beat_type: Optional[str] = None
    beat_type_raw: Optional[str] = None
