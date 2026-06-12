# Lint as: python3
# pylint: disable=invalid-name
"""Registers the schema for com.verily.myalo.sms_text_messages with Beam."""

import dataclasses
from typing import Optional

from verily.ds_sdk.core.schemas import DataPoint
from verily.ds_sdk.core.schemas import dataspec


@dataspec('com.verily.myalo.sms_text_messages')
@dataclasses.dataclass
class MyaloSms_Text_Messages(DataPoint):
    """Beam RowSchema for com.verily.myalo.sms_text_messages."""
    emoji_count: Optional[int] = None
    hashed_address: Optional[int] = None
    is_read: Optional[bool] = None
    message_direction: Optional[str] = None
    message_id: Optional[int] = None
    message_type: Optional[str] = None
    non_latin1_char_count: Optional[int] = None
    obfuscated_text: Optional[str] = None
    reported_time: Optional[int] = None
    text: Optional[str] = None
    thread_id: Optional[int] = None
    total_char_count: Optional[int] = None
    word_count: Optional[int] = None
