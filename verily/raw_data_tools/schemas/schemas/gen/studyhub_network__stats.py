# Lint as: python3
# pylint: disable=invalid-name
"""Registers the schema for com.verily.studyhub.network_stats with Beam."""

import dataclasses
from typing import Optional

from verily.ds_sdk.core.schemas import DataPoint
from verily.ds_sdk.core.schemas import dataspec


@dataspec('com.verily.studyhub.network_stats')
@dataclasses.dataclass
class StudyhubNetwork_Stats(DataPoint):
    """Beam RowSchema for com.verily.studyhub.network_stats."""
    avg_upload_rate: Optional[float] = None
    cellular_rssi_dbm: Optional[float] = None
    ping_time_ms: Optional[int] = None
