# Lint as: python3
# pylint: disable=invalid-name
"""Registers the schema for com.verily.myalo.network with Beam."""

import dataclasses
from typing import Optional

from verily.raw_data_tools.schemas.schemas.shared_schemas import DataPoint
from verily.raw_data_tools.schemas.schemas.decorators import dataspec


@dataspec('com.verily.myalo.network')
@dataclasses.dataclass
class MyaloNetwork(DataPoint):
    """Beam RowSchema for com.verily.myalo.network."""
    is_connected: Optional[bool] = None
    is_connecting: Optional[bool] = None
    network_name_hash: Optional[int] = None
    network_type: Optional[str] = None
    reported_time: Optional[int] = None
    sampling_start_time: Optional[int] = None
