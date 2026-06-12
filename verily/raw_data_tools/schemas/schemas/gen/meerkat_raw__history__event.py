# Lint as: python3
# pylint: disable=invalid-name
"""Registers the schema for com.verily.meerkat.raw_history_event with Beam."""

import dataclasses
from typing import Optional

from verily.ds_sdk.core.schemas import DataPoint
from verily.ds_sdk.core.schemas import dataspec


@dataspec('com.verily.meerkat.raw_history_event')
@dataclasses.dataclass
class MeerkatRaw_History_Event(DataPoint):
    """Beam RowSchema for com.verily.meerkat.raw_history_event."""
    connection_time: Optional[int] = None
    discovery_time: Optional[int] = None
    event_temperature: Optional[int] = None
    event_ticker_count: Optional[int] = None
    event_type: Optional[str] = None
    event_type_code: Optional[int] = None
    event_voltage: Optional[int] = None
    index: Optional[int] = None
    medication_name: Optional[str] = None
    session_resumed: Optional[int] = None
    timezone: Optional[int] = None
