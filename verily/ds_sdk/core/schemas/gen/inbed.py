# Lint as: python3
# pylint: disable=invalid-name
"""Registers the schema for com.verily.inbed with Beam."""

import dataclasses

from verily.ds_sdk.core.schemas import DataPoint
from verily.ds_sdk.core.schemas import dataspec


@dataspec('com.verily.inbed')
@dataclasses.dataclass
class Inbed(DataPoint):
    """Beam RowSchema for com.verily.inbed."""
    seconds: float
