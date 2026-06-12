# Lint as: python3
# pylint: disable=invalid-name
"""Registers the schema for com.verily.activity.idle with Beam."""

import dataclasses

from verily.ds_sdk.core.schemas import DataPoint
from verily.ds_sdk.core.schemas import dataspec


@dataspec('com.verily.activity.idle')
@dataclasses.dataclass
class ActivityIdle(DataPoint):
    """Beam RowSchema for com.verily.activity.idle."""
    seconds: float
