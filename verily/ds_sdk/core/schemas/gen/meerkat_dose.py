# Lint as: python3
# pylint: disable=invalid-name
"""Registers the schema for com.verily.meerkat.dose with Beam."""

import dataclasses
from typing import Optional

from verily.ds_sdk.core.schemas import DataPoint
from verily.ds_sdk.core.schemas import dataspec


@dataspec('com.verily.meerkat.dose')
@dataclasses.dataclass
class MeerkatDose(DataPoint):
    """Beam RowSchema for com.verily.meerkat.dose."""
    close_event_pending: Optional[int] = None
    close_index: Optional[int] = None
    close_temperature: Optional[int] = None
    close_ticker_count: Optional[int] = None
    close_time: Optional[int] = None
    close_voltage: Optional[int] = None
    dose_pending: Optional[int] = None
    ignored_event: Optional[int] = None
    medication_name: Optional[str] = None
    open_event_pending: Optional[int] = None
    open_index: Optional[int] = None
    open_temperature: Optional[int] = None
    open_ticker_count: Optional[int] = None
    open_time: Optional[int] = None
    open_voltage: Optional[int] = None
    table_time: Optional[int] = None
    timezone: Optional[int] = None
