# Lint as: python3
# pylint: disable=invalid-name
"""Registers the schema for com.verily.studywatch.crashdump with Beam."""

import dataclasses
from typing import List, Optional

from verily.ds_sdk.core.schemas import DataPoint
from verily.ds_sdk.core.schemas import dataspec


@dataspec('com.verily.studywatch.crashdump')
@dataclasses.dataclass
class StudywatchCrashdump(DataPoint):
    """Beam RowSchema for com.verily.studywatch.crashdump."""
    error_code: int
    line_number: int
    backtrace: Optional[List[int]] = None
    filename: Optional[str] = None
    registers_frame: Optional[int] = None
    system_registers: Optional[List[int]] = None
    trace_registers: Optional[List[int]] = None
    type: Optional[str] = None
