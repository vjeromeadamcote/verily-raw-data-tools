# Lint as: python3
# pylint: disable=invalid-name
"""Registers the schema for com.verily.http_connection_event with Beam."""

import dataclasses

from verily.raw_data_tools.schemas.schemas.shared_schemas import DataPoint
from verily.raw_data_tools.schemas.schemas.decorators import dataspec


@dataspec('com.verily.http_connection_event')
@dataclasses.dataclass
class Http_Connection_Event(DataPoint):
    """Beam RowSchema for com.verily.http_connection_event."""
