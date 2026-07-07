# Lint as: python3
# pylint: disable=invalid-name
"""Registers the schema for com.verily.studywatch.upload_size with Beam."""

import dataclasses
from typing import Optional

from verily.raw_data_tools.schemas.schemas.shared_schemas import DataPoint
from verily.raw_data_tools.schemas.schemas.decorators import dataspec


@dataspec('com.verily.studywatch.upload_size')
@dataclasses.dataclass
class StudywatchUpload_Size(DataPoint):
    """Beam RowSchema for com.verily.studywatch.upload_size."""
    connected_device_id: Optional[str] = None
    upload_session_id: Optional[int] = None
    upload_size: Optional[int] = None
