# Lint as: python3
# pylint: disable=invalid-name
"""Registers the schema for com.verily.irtc_turtle_interval_features with Beam."""  # pylint: disable=line-too-long

import dataclasses
from typing import Optional

from verily.raw_data_tools.schemas.schemas.shared_schemas import DataPoint
from verily.raw_data_tools.schemas.schemas.decorators import dataspec


@dataspec('com.verily.irtc_turtle_interval_features')
@dataclasses.dataclass
class Irtc_Turtle_Interval_Features(DataPoint):
    """Beam RowSchema for com.verily.irtc_turtle_interval_features."""
    af_avg_norm: Optional[float] = None
    af_interval_classification: Optional[str] = None
    af_interval_result: Optional[int] = None
    timestamp: Optional[int] = None
    unanalyzable_avg: Optional[float] = None
    window_count: Optional[int] = None
