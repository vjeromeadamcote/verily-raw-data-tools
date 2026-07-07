# Lint as: python3
# pylint: disable=invalid-name
"""Registers the schema for com.verily.myalo.permissions with Beam."""

import dataclasses
from typing import List, Optional

from verily.raw_data_tools.schemas.schemas.shared_schemas import DataPoint
from verily.raw_data_tools.schemas.schemas.decorators import dataspec


@dataspec('com.verily.myalo.permissions')
@dataclasses.dataclass
class MyaloPermissions(DataPoint):
    """Beam RowSchema for com.verily.myalo.permissions."""
    permission_status_list: Optional[List[str]] = None
    permission_type_list: Optional[List[str]] = None
