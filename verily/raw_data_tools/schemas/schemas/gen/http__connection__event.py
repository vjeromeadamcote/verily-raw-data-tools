# Lint as: python3
# pylint: disable=invalid-name
"""Registers the schema for com.verily.http_connection_event with Beam."""

import dataclasses

from verily.ds_sdk.core.schemas import DataPoint
from verily.ds_sdk.core.schemas import dataspec


@dataspec('com.verily.http_connection_event')
@dataclasses.dataclass
class Http_Connection_Event(DataPoint):
    """Beam RowSchema for com.verily.http_connection_event."""
