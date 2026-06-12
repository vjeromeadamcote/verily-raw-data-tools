# Lint as: python3
# pylint: disable=invalid-name
"""Registers the schema for com.verily.actigraphy_sleep with Beam."""

import dataclasses

from verily.ds_sdk.core.schemas import DataPoint
from verily.ds_sdk.core.schemas import dataspec


@dataspec('com.verily.actigraphy_sleep')
@dataclasses.dataclass
class Actigraphy_Sleep(DataPoint):
    """Beam RowSchema for com.verily.actigraphy_sleep."""
    coverage: float
    sleep_stage: str
