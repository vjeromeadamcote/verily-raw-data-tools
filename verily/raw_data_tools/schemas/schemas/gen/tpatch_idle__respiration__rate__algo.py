# Lint as: python3
# pylint: disable=invalid-name
"""Registers the schema for com.verily.tpatch.idle_respiration_rate_algo with Beam."""  # pylint: disable=line-too-long

import dataclasses
from typing import Optional

from verily.raw_data_tools.schemas.schemas.shared_schemas import DataPoint
from verily.raw_data_tools.schemas.schemas.decorators import dataspec


@dataspec('com.verily.tpatch.idle_respiration_rate_algo')
@dataclasses.dataclass
class TpatchIdle_Respiration_Rate_Algo(DataPoint):
    """Beam RowSchema for com.verily.tpatch.idle_respiration_rate_algo."""
    movement_detected: Optional[str] = None
    on_body_detected: Optional[str] = None
    x_axis_movement_excessive_value_g: Optional[float] = None
    x_axis_movement_median_value_g: Optional[float] = None
    x_axis_movement_movement_detected: Optional[str] = None
    x_axis_movement_on_body_detected: Optional[str] = None
    x_axis_movement_on_body_p95_g: Optional[float] = None
    x_axis_movement_on_body_threshold_g: Optional[float] = None
    x_axis_movement_threshold_g: Optional[float] = None
    y_axis_movement_excessive_value_g: Optional[float] = None
    y_axis_movement_median_value_g: Optional[float] = None
    y_axis_movement_movement_detected: Optional[str] = None
    y_axis_movement_on_body_detected: Optional[str] = None
    y_axis_movement_on_body_p95_g: Optional[float] = None
    y_axis_movement_on_body_threshold_g: Optional[float] = None
    y_axis_movement_threshold_g: Optional[float] = None
    z_axis_movement_excessive_value_g: Optional[float] = None
    z_axis_movement_median_value_g: Optional[float] = None
    z_axis_movement_movement_detected: Optional[str] = None
    z_axis_movement_on_body_detected: Optional[str] = None
    z_axis_movement_on_body_p95_g: Optional[float] = None
    z_axis_movement_on_body_threshold_g: Optional[float] = None
    z_axis_movement_threshold_g: Optional[float] = None
