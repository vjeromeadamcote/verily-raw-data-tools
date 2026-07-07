# Lint as: python3
# pylint: disable=invalid-name
"""Registers the schema for com.verily.diary_transcription with Beam."""

import dataclasses
from typing import List, Optional

from verily.raw_data_tools.schemas.schemas.shared_schemas import DataPoint
from verily.raw_data_tools.schemas.schemas.decorators import dataspec


@dataspec('com.verily.diary_transcription')
@dataclasses.dataclass
class Diary_Transcription(DataPoint):
    """Beam RowSchema for com.verily.diary_transcription."""
    text: Optional[List[str]] = None
    transcription_confidence: Optional[List[float]] = None
    transcription_succeeded: Optional[bool] = None
