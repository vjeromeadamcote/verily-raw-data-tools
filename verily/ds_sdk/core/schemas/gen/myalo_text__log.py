# Lint as: python3
# pylint: disable=invalid-name
"""Registers the schema for com.verily.myalo.text_log with Beam."""

import dataclasses
from typing import List, Optional

from apache_beam.utils.timestamp import Timestamp

from verily.ds_sdk.core.schemas import DataPoint
from verily.ds_sdk.core.schemas import dataspec


@dataspec('com.verily.myalo.text_log')
@dataclasses.dataclass
class MyaloText_Log(DataPoint):
    """Beam RowSchema for com.verily.myalo.text_log."""
    before: Optional[List[int]] = None
    char_sequence: Optional[List[str]] = None
    count: Optional[List[int]] = None
    device_id: Optional[str] = None
    event_timestamp: Optional[List[Timestamp]] = None
    participant_id: Optional[str] = None
    prompt: Optional[str] = None
    prompt_type: Optional[str] = None
    reported_time: Optional[int] = None
    start: Optional[List[int]] = None
    start_timestamp: Optional[Timestamp] = None
    submit_timestamp: Optional[Timestamp] = None
    task_id: Optional[str] = None
