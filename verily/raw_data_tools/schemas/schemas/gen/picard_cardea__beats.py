# Lint as: python3
# pylint: disable=invalid-name
"""Registers the schema for com.verily.picard.cardea_beats with Beam."""

import dataclasses
from typing import Optional

from verily.raw_data_tools.schemas.schemas.shared_schemas import DataPoint
from verily.raw_data_tools.schemas.schemas.decorators import dataspec


@dataspec('com.verily.picard.cardea_beats')
@dataclasses.dataclass
class PicardCardea_Beats(DataPoint):
    """Beam RowSchema for com.verily.picard.cardea_beats."""
    beat_label: Optional[str] = None
    relative_beat_location: Optional[float] = None
    rhythm_label: Optional[str] = None
