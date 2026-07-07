# Lint as: python3
# pylint: disable=invalid-name
"""Registers the schema for com.verily.studywatch.log_line with Beam."""

import dataclasses

from verily.raw_data_tools.schemas.schemas.shared_schemas import DataPoint
from verily.raw_data_tools.schemas.schemas.decorators import dataspec


@dataspec('com.verily.studywatch.log_line')
@dataclasses.dataclass
class StudywatchLog_Line(DataPoint):
    """Beam RowSchema for com.verily.studywatch.log_line."""
    log_line: str
