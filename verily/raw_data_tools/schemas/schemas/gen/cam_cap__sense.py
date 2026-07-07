# Lint as: python3
# pylint: disable=invalid-name
"""Registers the schema for com.verily.cam.cap_sense with Beam."""

import dataclasses

from verily.raw_data_tools.schemas.schemas.shared_schemas import DataPoint
from verily.raw_data_tools.schemas.schemas.decorators import dataspec


@dataspec('com.verily.cam.cap_sense')
@dataclasses.dataclass
class CamCap_Sense(DataPoint):
    """Beam RowSchema for com.verily.cam.cap_sense."""
    cap_sense: int
