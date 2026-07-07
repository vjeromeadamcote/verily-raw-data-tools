# Lint as: python3
# pylint: disable=invalid-name
"""Registers the schema for com.verily.studywatch.turtle_ppg_feature_data with Beam."""  # pylint: disable=line-too-long

import dataclasses
from typing import Optional

from verily.raw_data_tools.schemas.schemas.shared_schemas import DataPoint
from verily.raw_data_tools.schemas.schemas.decorators import dataspec


@dataspec('com.verily.studywatch.turtle_ppg_feature_data')
@dataclasses.dataclass
class StudywatchTurtle_Ppg_Feature_Data(DataPoint):
    """Beam RowSchema for com.verily.studywatch.turtle_ppg_feature_data."""
    feature_data: Optional[bytes] = None
