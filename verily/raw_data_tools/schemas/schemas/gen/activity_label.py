# Lint as: python3
# pylint: disable=invalid-name
"""Registers the schema for com.verily.activity.label with Beam."""

import dataclasses
from typing import Optional

from verily.ds_sdk.core.schemas import DataPoint
from verily.ds_sdk.core.schemas import dataspec


@dataspec('com.verily.activity.label')
@dataclasses.dataclass
class ActivityLabel(DataPoint):
    """Beam RowSchema for com.verily.activity.label."""
    class_label: str
    confidence: int
    intensity: Optional[int] = None
