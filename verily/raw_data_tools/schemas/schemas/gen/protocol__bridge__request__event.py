# Lint as: python3
# pylint: disable=invalid-name
"""Registers the schema for com.verily.protocol_bridge_request_event with Beam."""  # pylint: disable=line-too-long

import dataclasses

from verily.raw_data_tools.schemas.schemas.shared_schemas import DataPoint
from verily.raw_data_tools.schemas.schemas.decorators import dataspec


@dataspec('com.verily.protocol_bridge_request_event')
@dataclasses.dataclass
class Protocol_Bridge_Request_Event(DataPoint):
    """Beam RowSchema for com.verily.protocol_bridge_request_event."""
