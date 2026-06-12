# Lint as: python3
# pylint: disable=invalid-name
"""Registers the schema for com.verily.myalo.phone_call with Beam."""

import dataclasses
from typing import Optional

from verily.ds_sdk.core.schemas import DataPoint
from verily.ds_sdk.core.schemas import dataspec


@dataspec('com.verily.myalo.phone_call')
@dataclasses.dataclass
class MyaloPhone_Call(DataPoint):
    """Beam RowSchema for com.verily.myalo.phone_call."""
    call_event: Optional[int] = None
    other_number_hash: Optional[int] = None
    reported_time: Optional[int] = None
    type: Optional[int] = None
