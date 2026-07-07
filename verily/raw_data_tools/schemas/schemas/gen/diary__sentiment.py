# Lint as: python3
# pylint: disable=invalid-name
"""Registers the schema for com.verily.diary_sentiment with Beam."""

import dataclasses
from typing import List, Optional

from verily.raw_data_tools.schemas.schemas.shared_schemas import DataPoint
from verily.raw_data_tools.schemas.schemas.decorators import dataspec


@dataspec('com.verily.diary_sentiment')
@dataclasses.dataclass
class Diary_Sentiment(DataPoint):
    """Beam RowSchema for com.verily.diary_sentiment."""
    sentiment_magnitudes: Optional[List[float]] = None
    sentiment_scores: Optional[List[float]] = None
    text: Optional[List[str]] = None
