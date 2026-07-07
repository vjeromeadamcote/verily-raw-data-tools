# Lint as: python3
# pylint: disable=invalid-name
"""Registers the schema for com.verily.studywatch.watch_reset with Beam."""

import dataclasses
from typing import Optional

from verily.raw_data_tools.schemas.schemas.shared_schemas import DataPoint
from verily.raw_data_tools.schemas.schemas.decorators import dataspec


@dataspec('com.verily.studywatch.watch_reset')
@dataclasses.dataclass
class StudywatchWatch_Reset(DataPoint):
    """Beam RowSchema for com.verily.studywatch.watch_reset."""
    bor: Optional[bool] = None
    fw: Optional[bool] = None
    iwdg: Optional[bool] = None
    lpwr: Optional[bool] = None
    obl: Optional[bool] = None
    pin: Optional[bool] = None
    sft: Optional[bool] = None
    wwdg: Optional[bool] = None
