# Lint as: python3
# pylint: disable=invalid-name
"""Registers the schema for com.verily.height with Beam."""

import dataclasses

from verily.ds_sdk.core.schemas import DataPoint
from verily.ds_sdk.core.schemas import dataspec


@dataspec('com.verily.height')
@dataclasses.dataclass
class Height(DataPoint):
    """Beam RowSchema for com.verily.height."""
    height: int
