# Lint as: python3
# pylint: disable=invalid-name
"""Registers the schema for com.verily.dorimio.temp2.buffer with Beam."""

import dataclasses

from verily.ds_sdk.core.schemas import DataPoint
from verily.ds_sdk.core.schemas import dataspec


@dataspec('com.verily.dorimio.temp2.buffer')
@dataclasses.dataclass
class DorimioTemp2Buffer(DataPoint):
    """Beam RowSchema for com.verily.dorimio.temp2.buffer."""
    value: bytes
