# Lint as: python3
# pylint: disable=invalid-name
"""Registers the schema for com.verily.tpatch.button with Beam."""

import dataclasses
from typing import Optional

from verily.raw_data_tools.schemas.schemas.shared_schemas import DataPoint
from verily.raw_data_tools.schemas.schemas.decorators import dataspec


@dataspec('com.verily.tpatch.button')
@dataclasses.dataclass
class TpatchButton(DataPoint):
    """Beam RowSchema for com.verily.tpatch.button."""
    held_millis: Optional[int] = None
