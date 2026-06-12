# Lint as: python3
# pylint: disable=invalid-name
"""Registers the schema for com.verily.studywatch.http_connection_event with Beam."""  # pylint: disable=line-too-long

import dataclasses
from typing import Optional

from verily.ds_sdk.core.schemas import DataPoint
from verily.ds_sdk.core.schemas import dataspec


@dataspec('com.verily.studywatch.http_connection_event')
@dataclasses.dataclass
class StudywatchHttp_Connection_Event(DataPoint):
    """Beam RowSchema for com.verily.studywatch.http_connection_event."""
    error_code: Optional[int] = None
    event_type: Optional[str] = None
