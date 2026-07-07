# Lint as: python3
# pylint: disable=invalid-name
"""Registers the schema for com.verily.coppa_encrypted with Beam."""

import dataclasses
from typing import List

from verily.raw_data_tools.schemas.schemas.shared_schemas import DataPoint
from verily.raw_data_tools.schemas.schemas.decorators import dataspec


@dataspec('com.verily.coppa_encrypted')
@dataclasses.dataclass
class Coppa_Encrypted(DataPoint):
    """Beam RowSchema for com.verily.coppa_encrypted."""
    frames: List[bytes]
