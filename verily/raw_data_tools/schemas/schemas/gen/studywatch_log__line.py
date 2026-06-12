# Lint as: python3
# pylint: disable=invalid-name
"""Registers the schema for com.verily.studywatch.log_line with Beam."""

import dataclasses

from verily.ds_sdk.core.schemas import DataPoint
from verily.ds_sdk.core.schemas import dataspec


@dataspec('com.verily.studywatch.log_line')
@dataclasses.dataclass
class StudywatchLog_Line(DataPoint):
    """Beam RowSchema for com.verily.studywatch.log_line."""
    log_line: str
