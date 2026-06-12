# Lint as: python3
# pylint: disable=invalid-name
"""Registers the schema for com.verily.studyhub.available_storage with Beam."""

import dataclasses
from typing import Optional

from verily.ds_sdk.core.schemas import DataPoint
from verily.ds_sdk.core.schemas import dataspec


@dataspec('com.verily.studyhub.available_storage')
@dataclasses.dataclass
class StudyhubAvailable_Storage(DataPoint):
    """Beam RowSchema for com.verily.studyhub.available_storage."""
    storage: Optional[int] = None
