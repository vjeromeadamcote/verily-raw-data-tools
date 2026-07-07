# Lint as: python3
# pylint: disable=invalid-name
"""Registers the schema for com.verily.studywatch.protocol_bridge_request_event with Beam."""  # pylint: disable=line-too-long

import dataclasses
from typing import Optional

from verily.raw_data_tools.schemas.schemas.shared_schemas import DataPoint
from verily.raw_data_tools.schemas.schemas.decorators import dataspec


@dataspec('com.verily.studywatch.protocol_bridge_request_event')
@dataclasses.dataclass
class StudywatchProtocol_Bridge_Request_Event(DataPoint):
    """Beam RowSchema for com.verily.studywatch.protocol_bridge_request_event."""  # pylint: disable=line-too-long
    error_code: Optional[int] = None
    event_type: Optional[str] = None
    request_type: Optional[str] = None
