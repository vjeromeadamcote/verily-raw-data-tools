# Lint as: python3
# pylint: disable=invalid-name
"""Registers the schema for com.verily.dorimio.temp2.buffer with Beam."""

import dataclasses

from verily.raw_data_tools.schemas.schemas.shared_schemas import DataPoint
from verily.raw_data_tools.schemas.schemas.decorators import dataspec


@dataspec('com.verily.dorimio.temp2.buffer')
@dataclasses.dataclass
class DorimioTemp2Buffer(DataPoint):
    """Beam RowSchema for com.verily.dorimio.temp2.buffer."""
    value: bytes
