# Lint as: python3
# pylint: disable=invalid-name
"""Registers the schema for com.verily.bryancrampton.testing with Beam."""

import dataclasses
from typing import Optional

from verily.ds_sdk.core.schemas import DataPoint
from verily.ds_sdk.core.schemas import dataspec


@dataspec('com.verily.bryancrampton.testing')
@dataclasses.dataclass
class BryancramptonTesting(DataPoint):
    """Beam RowSchema for com.verily.bryancrampton.testing."""
    test: Optional[str] = None
