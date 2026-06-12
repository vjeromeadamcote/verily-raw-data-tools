# Lint as: python3
# pylint: disable=invalid-name
"""Registers the schema for com.verily.studywatch.upload_size with Beam."""

import dataclasses
from typing import Optional

from verily.ds_sdk.core.schemas import DataPoint
from verily.ds_sdk.core.schemas import dataspec


@dataspec('com.verily.studywatch.upload_size')
@dataclasses.dataclass
class StudywatchUpload_Size(DataPoint):
    """Beam RowSchema for com.verily.studywatch.upload_size."""
    connected_device_id: Optional[str] = None
    upload_session_id: Optional[int] = None
    upload_size: Optional[int] = None
