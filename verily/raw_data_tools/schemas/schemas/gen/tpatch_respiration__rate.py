# Lint as: python3
# pylint: disable=invalid-name
"""Registers the schema for com.verily.tpatch.respiration_rate with Beam."""

import dataclasses
from typing import Optional

from verily.ds_sdk.core.schemas import DataPoint
from verily.ds_sdk.core.schemas import dataspec


@dataspec('com.verily.tpatch.respiration_rate')
@dataclasses.dataclass
class TpatchRespiration_Rate(DataPoint):
    """Beam RowSchema for com.verily.tpatch.respiration_rate."""
    movement_detected: Optional[str] = None
    on_body_detected: Optional[str] = None
    respiration_rate: Optional[float] = None
    x_axis_frequency_max_amplitude: Optional[float] = None
    x_axis_frequency_max_frequency_hz: Optional[float] = None
    x_axis_movement_excessive_value_g: Optional[float] = None
    x_axis_movement_median_value_g: Optional[float] = None
    x_axis_movement_movement_detected: Optional[str] = None
    x_axis_movement_on_body_detected: Optional[str] = None
    x_axis_movement_on_body_p95_g: Optional[float] = None
    x_axis_movement_on_body_threshold_g: Optional[float] = None
    x_axis_movement_threshold_g: Optional[float] = None
    y_axis_frequency_max_amplitude: Optional[float] = None
    y_axis_frequency_max_frequency_hz: Optional[float] = None
    y_axis_movement_excessive_value_g: Optional[float] = None
    y_axis_movement_median_value_g: Optional[float] = None
    y_axis_movement_movement_detected: Optional[str] = None
    y_axis_movement_on_body_detected: Optional[str] = None
    y_axis_movement_on_body_p95_g: Optional[float] = None
    y_axis_movement_on_body_threshold_g: Optional[float] = None
    y_axis_movement_threshold_g: Optional[float] = None
    z_axis_frequency_max_amplitude: Optional[float] = None
    z_axis_frequency_max_frequency_hz: Optional[float] = None
    z_axis_movement_excessive_value_g: Optional[float] = None
    z_axis_movement_median_value_g: Optional[float] = None
    z_axis_movement_movement_detected: Optional[str] = None
    z_axis_movement_on_body_detected: Optional[str] = None
    z_axis_movement_on_body_p95_g: Optional[float] = None
    z_axis_movement_on_body_threshold_g: Optional[float] = None
    z_axis_movement_threshold_g: Optional[float] = None
