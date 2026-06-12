# Lint as: python3
# pylint: disable=invalid-name
"""Registers the schema for com.verily.studywatch.turtle_ppg_feature_data with Beam."""  # pylint: disable=line-too-long

import dataclasses
from typing import Optional

from verily.ds_sdk.core.schemas import DataPoint
from verily.ds_sdk.core.schemas import dataspec


@dataspec('com.verily.studywatch.turtle_ppg_feature_data')
@dataclasses.dataclass
class StudywatchTurtle_Ppg_Feature_Data(DataPoint):
    """Beam RowSchema for com.verily.studywatch.turtle_ppg_feature_data."""
    feature_data: Optional[bytes] = None
