# Lint as: python3
# pylint: disable=invalid-name
"""Registers the schema for com.verily.cam.subsystem_status with Beam."""

import dataclasses

from verily.ds_sdk.core.schemas import DataPoint
from verily.ds_sdk.core.schemas import dataspec


@dataspec('com.verily.cam.subsystem_status')
@dataclasses.dataclass
class CamSubsystem_Status(DataPoint):
    """Beam RowSchema for com.verily.cam.subsystem_status."""
    status: int
