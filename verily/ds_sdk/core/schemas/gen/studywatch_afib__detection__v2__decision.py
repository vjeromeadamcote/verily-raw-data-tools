# Lint as: python3
# pylint: disable=invalid-name
"""Registers the schema for com.verily.studywatch.afib_detection_v2_decision with Beam."""  # pylint: disable=line-too-long

import dataclasses
from typing import Optional

from verily.ds_sdk.core.schemas import DataPoint
from verily.ds_sdk.core.schemas import dataspec


@dataspec('com.verily.studywatch.afib_detection_v2_decision')
@dataclasses.dataclass
class StudywatchAfib_Detection_V2_Decision(DataPoint):
    """Beam RowSchema for com.verily.studywatch.afib_detection_v2_decision."""
    good_quality_count: Optional[int] = None
    good_quality_positive_af_count: Optional[int] = None
    is_afib: Optional[bool] = None
    mlp_result: Optional[float] = None
    total_count: Optional[int] = None
