# Lint as: python3
# pylint: disable=invalid-name
"""Registers the schema for com.verily.tonostand.intraocular_pressure with Beam."""  # pylint: disable=line-too-long

import dataclasses
from typing import Optional

from verily.raw_data_tools.schemas.schemas.shared_schemas import DataPoint
from verily.raw_data_tools.schemas.schemas.decorators import dataspec


@dataspec('com.verily.tonostand.intraocular_pressure')
@dataclasses.dataclass
class TonostandIntraocular_Pressure(DataPoint):
    """Beam RowSchema for com.verily.tonostand.intraocular_pressure."""
    average_eye_pressure: float
    session_id: int
    eye_location: Optional[str] = None
