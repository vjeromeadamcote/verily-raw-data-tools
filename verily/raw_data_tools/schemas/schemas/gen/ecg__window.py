# Lint as: python3
# pylint: disable=invalid-name
"""Registers the schema for com.verily.ecg_window with Beam."""

import dataclasses

from verily.ds_sdk.core.schemas import DataPoint
from verily.ds_sdk.core.schemas import dataspec


@dataspec('com.verily.ecg_window')
@dataclasses.dataclass
class Ecg_Window(DataPoint):
    """Beam RowSchema for com.verily.ecg_window."""
    seconds: float
