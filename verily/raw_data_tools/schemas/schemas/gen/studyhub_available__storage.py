# Lint as: python3
# pylint: disable=invalid-name
"""Registers the schema for com.verily.studyhub.available_storage with Beam."""

import dataclasses
from typing import Optional

from verily.raw_data_tools.schemas.schemas.shared_schemas import DataPoint
from verily.raw_data_tools.schemas.schemas.decorators import dataspec


@dataspec('com.verily.studyhub.available_storage')
@dataclasses.dataclass
class StudyhubAvailable_Storage(DataPoint):
    """Beam RowSchema for com.verily.studyhub.available_storage."""
    storage: Optional[int] = None
