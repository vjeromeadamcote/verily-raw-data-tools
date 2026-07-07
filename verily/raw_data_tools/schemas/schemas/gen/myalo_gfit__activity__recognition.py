# Lint as: python3
# pylint: disable=invalid-name
"""Registers the schema for com.verily.myalo.gfit_activity_recognition with Beam."""  # pylint: disable=line-too-long

import dataclasses
from typing import Optional

from verily.raw_data_tools.schemas.schemas.shared_schemas import DataPoint
from verily.raw_data_tools.schemas.schemas.decorators import dataspec


@dataspec('com.verily.myalo.gfit_activity_recognition')
@dataclasses.dataclass
class MyaloGfit_Activity_Recognition(DataPoint):
    """Beam RowSchema for com.verily.myalo.gfit_activity_recognition."""
    activity: Optional[str] = None
    activity_str: Optional[str] = None
    duration: Optional[int] = None
    sample_time: Optional[int] = None
