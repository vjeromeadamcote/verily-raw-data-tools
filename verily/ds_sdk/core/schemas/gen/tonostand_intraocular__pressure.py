# Lint as: python3
# pylint: disable=invalid-name
"""Registers the schema for com.verily.tonostand.intraocular_pressure with Beam."""  # pylint: disable=line-too-long

import dataclasses
from typing import Optional

from verily.ds_sdk.core.schemas import DataPoint
from verily.ds_sdk.core.schemas import dataspec


@dataspec('com.verily.tonostand.intraocular_pressure')
@dataclasses.dataclass
class TonostandIntraocular_Pressure(DataPoint):
    """Beam RowSchema for com.verily.tonostand.intraocular_pressure."""
    average_eye_pressure: float
    session_id: int
    eye_location: Optional[str] = None
