# Lint as: python3
# pylint: disable=invalid-name
"""Registers the schema for com.verily.studywatch.wifi_connection_event with Beam."""  # pylint: disable=line-too-long

import dataclasses
from typing import Optional

from verily.ds_sdk.core.schemas import DataPoint
from verily.ds_sdk.core.schemas import dataspec


@dataspec('com.verily.studywatch.wifi_connection_event')
@dataclasses.dataclass
class StudywatchWifi_Connection_Event(DataPoint):
    """Beam RowSchema for com.verily.studywatch.wifi_connection_event."""
    band: Optional[str] = None
    channel: Optional[int] = None
    error_code: Optional[int] = None
    event_type: Optional[str] = None
    ip_addr_type: Optional[str] = None
    security_scheme: Optional[str] = None
    signal_strength: Optional[int] = None
