# Lint as: python3
# pylint: disable=invalid-name
"""Registers the schema for com.verily.interbeat_interval_metrics with Beam."""

import dataclasses
from typing import Optional

from verily.raw_data_tools.schemas.schemas.shared_schemas import DataPoint
from verily.raw_data_tools.schemas.schemas.decorators import dataspec


@dataspec('com.verily.interbeat_interval_metrics')
@dataclasses.dataclass
class Interbeat_Interval_Metrics(DataPoint):
    """Beam RowSchema for com.verily.interbeat_interval_metrics."""
    absolute_afentropy: float
    absolute_kurtdibi: float
    absolute_norm_ibi: float
    absolute_nsrentropy: float
    absolute_sd1: float
    absolute_sd2: float
    absolute_stddibi: float
    absolute_stdibi: float
    normalized_afentropy: float
    normalized_kurtdibi: float
    normalized_norm_ibi: float
    normalized_nsrentropy: float
    normalized_sd1: float
    normalized_sd2: float
    normalized_stddibi: float
    normalized_stdibi: float
    window_start: Optional[int] = None
