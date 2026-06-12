# Lint as: python3
# pylint: disable=invalid-name
"""Registers the schema for com.verily.blood_glucose with Beam."""

import dataclasses

from verily.ds_sdk.core.schemas import DataPoint
from verily.ds_sdk.core.schemas import dataspec


@dataspec('com.verily.blood_glucose')
@dataclasses.dataclass
class Blood_Glucose(DataPoint):
    """Beam RowSchema for com.verily.blood_glucose."""
    glucose: float
