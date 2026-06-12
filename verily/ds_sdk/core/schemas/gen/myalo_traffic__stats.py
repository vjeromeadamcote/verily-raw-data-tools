# Lint as: python3
# pylint: disable=invalid-name
"""Registers the schema for com.verily.myalo.traffic_stats with Beam."""

import dataclasses
from typing import List, Optional

from verily.ds_sdk.core.schemas import DataPoint
from verily.ds_sdk.core.schemas import dataspec


@dataspec('com.verily.myalo.traffic_stats')
@dataclasses.dataclass
class MyaloTraffic_Stats(DataPoint):
    """Beam RowSchema for com.verily.myalo.traffic_stats."""
    package_list: Optional[List[str]] = None
    reported_time: Optional[int] = None
    rx_bytes_list: Optional[List[int]] = None
    rx_packets_list: Optional[List[int]] = None
    tx_bytes_list: Optional[List[int]] = None
    tx_packets_list: Optional[List[int]] = None
