# Lint as: python3
# pylint: disable=invalid-name
"""Registers the schema for com.verily.annotation.overlapping_data with Beam."""

import dataclasses
from typing import Optional

from verily.ds_sdk.core.schemas import DataPoint
from verily.ds_sdk.core.schemas import dataspec


@dataspec('com.verily.annotation.overlapping_data')
@dataclasses.dataclass
class AnnotationOverlapping_Data(DataPoint):
    """Beam RowSchema for com.verily.annotation.overlapping_data."""
    end_time_millis: Optional[int] = None
