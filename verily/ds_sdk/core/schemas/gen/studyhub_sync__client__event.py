# Lint as: python3
# pylint: disable=invalid-name
"""Registers the schema for com.verily.studyhub.sync_client_event with Beam."""

import dataclasses
from typing import Optional

from verily.ds_sdk.core.schemas import DataPoint
from verily.ds_sdk.core.schemas import dataspec


@dataspec('com.verily.studyhub.sync_client_event')
@dataclasses.dataclass
class StudyhubSync_Client_Event(DataPoint):
    """Beam RowSchema for com.verily.studyhub.sync_client_event."""
    connected_device_id: Optional[str] = None
    event: Optional[str] = None
    upload_session_id: Optional[int] = None
