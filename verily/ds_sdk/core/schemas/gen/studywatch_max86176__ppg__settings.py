# Lint as: python3
# pylint: disable=invalid-name
"""Registers the schema for com.verily.studywatch.max86176_ppg_settings with Beam."""  # pylint: disable=line-too-long

import dataclasses
from typing import Optional

from verily.ds_sdk.core.schemas import DataPoint
from verily.ds_sdk.core.schemas import dataspec


@dataspec('com.verily.studywatch.max86176_ppg_settings')
@dataclasses.dataclass
class StudywatchMax86176_Ppg_Settings(DataPoint):
    """Beam RowSchema for com.verily.studywatch.max86176_ppg_settings."""
    configuration_type: Optional[int] = None
    dac_offset_1: Optional[int] = None
    dac_offset_2: Optional[int] = None
    integration_time: Optional[int] = None
    led_current: Optional[int] = None
    sample_average: Optional[int] = None
