# Lint as: python3
# pylint: disable=invalid-name
"""Registers the schema for com.verily.sleep.stages.raw with Beam."""

import dataclasses

from verily.ds_sdk.core.schemas import DataPoint
from verily.ds_sdk.core.schemas import dataspec


@dataspec('com.verily.sleep.stages.raw')
@dataclasses.dataclass
class SleepStagesRaw(DataPoint):
    """Beam RowSchema for com.verily.sleep.stages.raw."""
    duration_millis: int
    stage: str
