# Lint as: python3
# pylint: disable=invalid-name
"""Registers the schema for com.verily.x with Beam."""

import dataclasses

from verily.ds_sdk.core.schemas import DataPoint
from verily.ds_sdk.core.schemas import dataspec


@dataspec('com.verily.x')
@dataclasses.dataclass
class X(DataPoint):
    """Beam RowSchema for com.verily.x."""
