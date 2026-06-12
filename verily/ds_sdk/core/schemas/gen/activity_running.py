# Lint as: python3
# pylint: disable=invalid-name
"""Registers the schema for com.verily.activity.running with Beam."""

import dataclasses

from verily.ds_sdk.core.schemas import DataPoint
from verily.ds_sdk.core.schemas import dataspec


@dataspec('com.verily.activity.running')
@dataclasses.dataclass
class ActivityRunning(DataPoint):
    """Beam RowSchema for com.verily.activity.running."""
    seconds: float
