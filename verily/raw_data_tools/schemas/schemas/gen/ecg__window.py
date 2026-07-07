# Lint as: python3
# pylint: disable=invalid-name
"""Registers the schema for com.verily.ecg_window with Beam."""

import dataclasses

from verily.raw_data_tools.schemas.schemas.shared_schemas import DataPoint
from verily.raw_data_tools.schemas.schemas.decorators import dataspec


@dataspec('com.verily.ecg_window')
@dataclasses.dataclass
class Ecg_Window(DataPoint):
    """Beam RowSchema for com.verily.ecg_window."""
    seconds: float
