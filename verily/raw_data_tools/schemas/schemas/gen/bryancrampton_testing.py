# Lint as: python3
# pylint: disable=invalid-name
"""Registers the schema for com.verily.bryancrampton.testing with Beam."""

import dataclasses
from typing import Optional

from verily.raw_data_tools.schemas.schemas.shared_schemas import DataPoint
from verily.raw_data_tools.schemas.schemas.decorators import dataspec


@dataspec('com.verily.bryancrampton.testing')
@dataclasses.dataclass
class BryancramptonTesting(DataPoint):
    """Beam RowSchema for com.verily.bryancrampton.testing."""
    test: Optional[str] = None
