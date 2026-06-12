# Lint as: python3
# pylint: disable=invalid-name
"""Registers the schema for com.verily.myalo.activity_recognition with Beam."""

import dataclasses
from typing import List, Optional

from verily.ds_sdk.core.schemas import DataPoint
from verily.ds_sdk.core.schemas import dataspec


@dataspec('com.verily.myalo.activity_recognition')
@dataclasses.dataclass
class MyaloActivity_Recognition(DataPoint):
    """Beam RowSchema for com.verily.myalo.activity_recognition."""
    activity_list: Optional[List[str]] = None
    confidence_list: Optional[List[int]] = None
    most_probable_activity: Optional[str] = None
    most_probable_activity_confidence: Optional[int] = None
    reported_time: Optional[int] = None
    sample_time: Optional[int] = None
