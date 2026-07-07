# Lint as: python3
# pylint: disable=invalid-name
"""Registers the schema for com.verily.myalo.network_stats with Beam."""

import dataclasses
from typing import List, Optional

from verily.raw_data_tools.schemas.schemas.shared_schemas import DataPoint
from verily.raw_data_tools.schemas.schemas.decorators import dataspec


@dataspec('com.verily.myalo.network_stats')
@dataclasses.dataclass
class MyaloNetwork_Stats(DataPoint):
    """Beam RowSchema for com.verily.myalo.network_stats."""
    end_time: Optional[int] = None
    metered_list: Optional[List[str]] = None
    network_type_list: Optional[List[int]] = None
    network_type_str_list: Optional[List[str]] = None
    package_list: Optional[List[str]] = None
    reported_time: Optional[int] = None
    roaming_list: Optional[List[str]] = None
    rx_bytes_list: Optional[List[int]] = None
    rx_packets_list: Optional[List[int]] = None
    start_time: Optional[int] = None
    state_list: Optional[List[str]] = None
    tx_bytes_list: Optional[List[int]] = None
    tx_packets_list: Optional[List[int]] = None
