# Lint as: python3
# pylint: disable=invalid-name
"""Registers the schema for com.verily.hypnos.resmed.raw with Beam."""

import dataclasses
from typing import Optional

from verily.raw_data_tools.schemas.schemas.shared_schemas import DataPoint
from verily.raw_data_tools.schemas.schemas.decorators import dataspec


@dataspec('com.verily.hypnos.resmed.raw')
@dataclasses.dataclass
class HypnosResmedRaw(DataPoint):
    """Beam RowSchema for com.verily.hypnos.resmed.raw."""
    source_json: Optional[bytes] = None
