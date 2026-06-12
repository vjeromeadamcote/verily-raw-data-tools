# Lint as: python3
# pylint: disable=invalid-name
"""Registers the schema for com.verily.studywatch.ecg_quality with Beam."""

import dataclasses
from typing import List, Optional

from verily.ds_sdk.core.schemas import DataPoint
from verily.ds_sdk.core.schemas import dataspec


@dataspec('com.verily.studywatch.ecg_quality')
@dataclasses.dataclass
class StudywatchEcg_Quality(DataPoint):
    """Beam RowSchema for com.verily.studywatch.ecg_quality."""
    quality: Optional[List[float]] = None
    sampling_rate: Optional[int] = None
    sensor_id: Optional[int] = None
    true_timestamp_millis: Optional[int] = None
    true_timestamp_sample_index: Optional[int] = None
