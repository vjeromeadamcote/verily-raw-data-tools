# Lint as: python3
# pylint: disable=invalid-name
"""Registers the schema for com.veirly.myalo.notification with Beam."""

import dataclasses
from typing import List, Optional

from verily.ds_sdk.core.schemas import DataPoint
from verily.ds_sdk.core.schemas import dataspec


@dataspec('com.veirly.myalo.notification')
@dataclasses.dataclass
class ComVeirlyMyaloNotification(DataPoint):
    """Beam RowSchema for com.veirly.myalo.notification."""
    category: Optional[str] = None
    is_ongoing: Optional[bool] = None
    number: Optional[int] = None
    package_name: Optional[str] = None
    people_list_hashed: Optional[List[int]] = None
    title_hashed: Optional[int] = None
