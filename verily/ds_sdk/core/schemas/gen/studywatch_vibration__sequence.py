# Lint as: python3
# pylint: disable=invalid-name
"""Registers the schema for com.verily.studywatch.vibration_sequence with Beam."""  # pylint: disable=line-too-long

import dataclasses
from typing import List, Optional

from verily.ds_sdk.core.schemas import DataPoint
from verily.ds_sdk.core.schemas import dataspec


@dataspec('com.verily.studywatch.vibration_sequence')
@dataclasses.dataclass
class StudywatchVibration_Sequence(DataPoint):
    """Beam RowSchema for com.verily.studywatch.vibration_sequence."""
    repeat_count: Optional[int] = None
    repeat_last_two_forever: Optional[bool] = None
    sequence: Optional[List[int]] = None
    sequence_type: Optional[int] = None
