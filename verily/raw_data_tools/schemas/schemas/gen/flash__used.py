# Lint as: python3
# pylint: disable=invalid-name
"""Registers the schema for com.verily.flash_used with Beam."""

import dataclasses
from typing import List, Optional

from verily.raw_data_tools.schemas.schemas.shared_schemas import DataPoint
from verily.raw_data_tools.schemas.schemas.decorators import dataspec


@dataspec('com.verily.flash_used')
@dataclasses.dataclass
class Flash_Used(DataPoint):
    """Beam RowSchema for com.verily.flash_used."""
    block_size: Optional[List[int]] = None
    bytes_used: Optional[int] = None
    total_blocks: Optional[List[int]] = None
    used_blocks: Optional[List[int]] = None
