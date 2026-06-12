# Lint as: python3
# pylint: disable=invalid-name
"""Registers the schema for com.verily.studywatch.telemetry with Beam."""

import dataclasses
from typing import Optional

from verily.ds_sdk.core.schemas import DataPoint
from verily.ds_sdk.core.schemas import dataspec


@dataspec('com.verily.studywatch.telemetry')
@dataclasses.dataclass
class StudywatchTelemetry(DataPoint):
    """Beam RowSchema for com.verily.studywatch.telemetry."""
    daily_ble_connections: Optional[int] = None
    daily_button_presses: Optional[int] = None
    daily_ecgs_completed: Optional[int] = None
    daily_on_wrist_time: Optional[int] = None
