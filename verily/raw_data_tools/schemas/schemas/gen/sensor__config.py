# Lint as: python3
# pylint: disable=invalid-name
"""Registers the schema for com.verily.sensor_config with Beam."""

import dataclasses
from typing import Optional

from verily.raw_data_tools.schemas.schemas.shared_schemas import DataPoint
from verily.raw_data_tools.schemas.schemas.decorators import dataspec


@dataspec('com.verily.sensor_config')
@dataclasses.dataclass
class Sensor_Config(DataPoint):
    """Beam RowSchema for com.verily.sensor_config."""
    ambient_dac: Optional[int] = None
    cf_led: Optional[int] = None
    iled1: Optional[int] = None
    iled2: Optional[int] = None
    iled_coarse: Optional[int] = None
    iled_fine: Optional[int] = None
    iled_scale: Optional[int] = None
    led_sel: Optional[int] = None
    led_width: Optional[int] = None
    num_averages: Optional[int] = None
    num_pulses: Optional[int] = None
    range: Optional[int] = None
    rf_led: Optional[int] = None
    sampling_period: Optional[int] = None
    stage2_enable: Optional[bool] = None
    stage2_gain: Optional[int] = None
    tia_gain: Optional[int] = None
    type: Optional[str] = None
