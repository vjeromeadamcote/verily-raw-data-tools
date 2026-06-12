# Lint as: python3
# pylint: disable=invalid-name
"""Registers the schema for com.verily.activity.walking with Beam."""

import dataclasses

from verily.ds_sdk.core.schemas import DataPoint
from verily.ds_sdk.core.schemas import dataspec


@dataspec('com.verily.activity.walking')
@dataclasses.dataclass
class ActivityWalking(DataPoint):
    """Beam RowSchema for com.verily.activity.walking."""
    seconds: float
