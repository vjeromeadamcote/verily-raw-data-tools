# Lint as: python3
# pylint: disable=invalid-name
"""Registers the schema for com.verily.myalo.file with Beam."""

import dataclasses
from typing import List, Optional

from verily.raw_data_tools.schemas.schemas.shared_schemas import DataPoint
from verily.raw_data_tools.schemas.schemas.decorators import dataspec


@dataspec('com.verily.myalo.file')
@dataclasses.dataclass
class MyaloFile(DataPoint):
    """Beam RowSchema for com.verily.myalo.file."""
    file_blob: Optional[bytes] = None
    file_metadata_key_list: Optional[List[str]] = None
    file_metadata_value_list: Optional[List[str]] = None
    file_name: Optional[str] = None
    file_type: Optional[str] = None
