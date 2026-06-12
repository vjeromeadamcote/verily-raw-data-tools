# Lint as: python3
# pylint: disable=invalid-name
"""Registers the schema for com.verily.studywatch.sync_event with Beam."""

import dataclasses
from typing import Optional

from verily.ds_sdk.core.schemas import DataPoint
from verily.ds_sdk.core.schemas import dataspec


@dataspec('com.verily.studywatch.sync_event')
@dataclasses.dataclass
class StudywatchSync_Event(DataPoint):
    """Beam RowSchema for com.verily.studywatch.sync_event."""
    connected_device_id: Optional[str] = None
    event: Optional[str] = None
    upload_session_id: Optional[int] = None
