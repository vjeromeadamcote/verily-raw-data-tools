# Lint as: python3
# pylint: disable=invalid-name
"""Registers the schema for com.verily.myalo.notification with Beam."""

import dataclasses
from typing import List, Optional

from verily.ds_sdk.core.schemas import DataPoint
from verily.ds_sdk.core.schemas import dataspec


@dataspec('com.verily.myalo.notification')
@dataclasses.dataclass
class MyaloNotification(DataPoint):
    """Beam RowSchema for com.verily.myalo.notification."""
    category: Optional[str] = None
    is_ongoing: Optional[bool] = None
    number: Optional[int] = None
    package_name: Optional[str] = None
    people_list_hashed: Optional[List[int]] = None
    title_hashed: Optional[int] = None
