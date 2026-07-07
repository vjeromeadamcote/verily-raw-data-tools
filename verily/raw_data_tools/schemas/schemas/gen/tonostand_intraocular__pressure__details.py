# Lint as: python3
# pylint: disable=invalid-name
"""Registers the schema for com.verily.tonostand.intraocular_pressure_details with Beam."""  # pylint: disable=line-too-long

import dataclasses
from typing import List, Optional

from verily.raw_data_tools.schemas.schemas.shared_schemas import DataPoint
from verily.raw_data_tools.schemas.schemas.decorators import dataspec


@dataspec('com.verily.tonostand.intraocular_pressure_details')
@dataclasses.dataclass
class TonostandIntraocular_Pressure_Details(DataPoint):
    """Beam RowSchema for com.verily.tonostand.intraocular_pressure_details."""
    air_puff_start_time_micros: int
    axis_mode: int
    cornea_response_curve_time_offset: List[int]
    cornea_response_curve_voltage: List[float]
    eye_position_time_offset: List[int]
    eye_position_x: List[float]
    eye_position_y: List[float]
    eye_position_z: List[float]
    eye_pressure: float
    radial_mode: int
    session_id: int
    eye_location: Optional[str] = None
