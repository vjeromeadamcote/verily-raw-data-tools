# Lint as: python3
# pylint: disable=invalid-name
"""Registers the schema for com.verily.tonostand.pair_account with Beam."""

import dataclasses
from typing import Optional

from verily.raw_data_tools.schemas.schemas.shared_schemas import DataPoint
from verily.raw_data_tools.schemas.schemas.decorators import dataspec


@dataspec('com.verily.tonostand.pair_account')
@dataclasses.dataclass
class TonostandPair_Account(DataPoint):
    """Beam RowSchema for com.verily.tonostand.pair_account."""
    left_eye_iris_id: Optional[str] = None
    right_eye_iris_id: Optional[str] = None
