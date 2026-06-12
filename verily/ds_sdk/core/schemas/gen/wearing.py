# Lint as: python3
# pylint: disable=invalid-name
"""Registers the schema for com.verily.wearing with Beam."""

import dataclasses

from verily.ds_sdk.core.schemas import DataPoint
from verily.ds_sdk.core.schemas import dataspec


@dataspec('com.verily.wearing')
@dataclasses.dataclass
class Wearing(DataPoint):
    """Beam RowSchema for com.verily.wearing."""
    seconds: float
