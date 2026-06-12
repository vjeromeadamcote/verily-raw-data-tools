# Lint as: python3
# pylint: disable=invalid-name
"""Registers the schema for com.verily.myalo.phone_call_v2 with Beam."""

import dataclasses
from typing import Optional

from verily.ds_sdk.core.schemas import DataPoint
from verily.ds_sdk.core.schemas import dataspec


@dataspec('com.verily.myalo.phone_call_v2')
@dataclasses.dataclass
class MyaloPhone_Call_V2(DataPoint):
    """Beam RowSchema for com.verily.myalo.phone_call_v2."""
    call_event_name: Optional[str] = None
    other_number_hash: Optional[int] = None
    reported_time: Optional[int] = None
    type: Optional[str] = None
