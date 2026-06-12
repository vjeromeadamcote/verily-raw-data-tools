# Lint as: python3
# pylint: disable=invalid-name
"""Registers the schema for com.verily.owd_label with Beam."""

import dataclasses
from typing import Optional

from verily.ds_sdk.core.schemas import DataPoint
from verily.ds_sdk.core.schemas import dataspec


@dataspec('com.verily.owd_label')
@dataclasses.dataclass
class Owd_Label(DataPoint):
    """Beam RowSchema for com.verily.owd_label."""
    label: float
    confidence_level: Optional[float] = None
    smooth_label: Optional[float] = None
