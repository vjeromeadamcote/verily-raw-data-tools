# Lint as: python3
# pylint: disable=invalid-name
"""Registers the schema for com.verily.tonostand.pair_account with Beam."""

import dataclasses
from typing import Optional

from verily.ds_sdk.core.schemas import DataPoint
from verily.ds_sdk.core.schemas import dataspec


@dataspec('com.verily.tonostand.pair_account')
@dataclasses.dataclass
class TonostandPair_Account(DataPoint):
    """Beam RowSchema for com.verily.tonostand.pair_account."""
    left_eye_iris_id: Optional[str] = None
    right_eye_iris_id: Optional[str] = None
