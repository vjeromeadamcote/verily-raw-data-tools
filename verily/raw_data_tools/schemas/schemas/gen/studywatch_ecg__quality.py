# Lint as: python3
# pylint: disable=invalid-name
"""Registers the schema for com.verily.studywatch.ecg_quality with Beam."""

import dataclasses
from typing import List, Optional

from verily.raw_data_tools.schemas.schemas.shared_schemas import DataPoint
from verily.raw_data_tools.schemas.schemas.decorators import dataspec


@dataspec('com.verily.studywatch.ecg_quality')
@dataclasses.dataclass
class StudywatchEcg_Quality(DataPoint):
    """Beam RowSchema for com.verily.studywatch.ecg_quality."""
    quality: Optional[List[float]] = None
    sampling_rate: Optional[int] = None
    sensor_id: Optional[int] = None
    true_timestamp_millis: Optional[int] = None
    true_timestamp_sample_index: Optional[int] = None
