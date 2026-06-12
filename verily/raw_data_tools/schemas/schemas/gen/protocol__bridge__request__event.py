# Lint as: python3
# pylint: disable=invalid-name
"""Registers the schema for com.verily.protocol_bridge_request_event with Beam."""  # pylint: disable=line-too-long

import dataclasses

from verily.ds_sdk.core.schemas import DataPoint
from verily.ds_sdk.core.schemas import dataspec


@dataspec('com.verily.protocol_bridge_request_event')
@dataclasses.dataclass
class Protocol_Bridge_Request_Event(DataPoint):
    """Beam RowSchema for com.verily.protocol_bridge_request_event."""
