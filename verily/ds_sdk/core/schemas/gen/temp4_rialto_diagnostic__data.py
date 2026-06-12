# Lint as: python3
# pylint: disable=invalid-name
"""Registers the schema for com.verily.temp4.rialto.diagnostic_data with Beam."""  # pylint: disable=line-too-long

import dataclasses
from typing import Optional

from verily.ds_sdk.core.schemas import DataPoint
from verily.ds_sdk.core.schemas import dataspec


@dataspec('com.verily.temp4.rialto.diagnostic_data')
@dataclasses.dataclass
class Temp4RialtoDiagnostic_Data(DataPoint):
    """Beam RowSchema for com.verily.temp4.rialto.diagnostic_data."""
    event: int
    temp: Optional[float] = None
