# Lint as: python3
# pylint: disable=invalid-name
"""Registers the schema for com.verily.pitch with Beam."""

import dataclasses
from typing import List, Optional

from verily.ds_sdk.core.schemas import DataPoint
from verily.ds_sdk.core.schemas import dataspec


@dataspec('com.verily.pitch')
@dataclasses.dataclass
class Pitch(DataPoint):
    """Beam RowSchema for com.verily.pitch."""
    frequency_list: Optional[List[float]] = None
    timestamp_list: Optional[List[float]] = None
    voice_activity_confidence_list: Optional[List[float]] = None
