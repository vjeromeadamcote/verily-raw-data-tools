# Lint as: python3
# pylint: disable=invalid-name
"""Registers the schema for com.verily.blood_glucose_mmol with Beam."""

import dataclasses

from verily.ds_sdk.core.schemas import DataPoint
from verily.ds_sdk.core.schemas import dataspec


@dataspec('com.verily.blood_glucose_mmol')
@dataclasses.dataclass
class Blood_Glucose_Mmol(DataPoint):
    """Beam RowSchema for com.verily.blood_glucose_mmol."""
    glucose: float
