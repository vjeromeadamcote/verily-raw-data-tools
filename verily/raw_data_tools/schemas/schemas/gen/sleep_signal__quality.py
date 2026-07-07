# Lint as: python3
# pylint: disable=invalid-name
"""Registers the schema for com.verily.sleep.signal_quality with Beam."""

import dataclasses
from typing import Optional

from verily.raw_data_tools.schemas.schemas.shared_schemas import DataPoint
from verily.raw_data_tools.schemas.schemas.decorators import dataspec


@dataspec('com.verily.sleep.signal_quality')
@dataclasses.dataclass
class SleepSignal_Quality(DataPoint):
    """Beam RowSchema for com.verily.sleep.signal_quality."""
    signal_quality: Optional[int] = None
