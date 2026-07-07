# Lint as: python3
# pylint: disable=invalid-name
"""Registers the schema for com.verily.pulse_ox with Beam."""

import dataclasses
from typing import Optional

from verily.raw_data_tools.schemas.schemas.shared_schemas import DataPoint
from verily.raw_data_tools.schemas.schemas.decorators import dataspec


@dataspec('com.verily.pulse_ox')
@dataclasses.dataclass
class Pulse_Ox(DataPoint):
    """Beam RowSchema for com.verily.pulse_ox."""
    pulse_ox: float
    perfusion_index: Optional[float] = None
