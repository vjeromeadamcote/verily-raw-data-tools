# Lint as: python3
# pylint: disable=invalid-name
"""Registers the schema for com.verily.galvanic_skin_response with Beam."""

import dataclasses

from verily.raw_data_tools.schemas.schemas.shared_schemas import DataPoint
from verily.raw_data_tools.schemas.schemas.decorators import dataspec


@dataspec('com.verily.galvanic_skin_response')
@dataclasses.dataclass
class Galvanic_Skin_Response(DataPoint):
    """Beam RowSchema for com.verily.galvanic_skin_response."""
    resistance: int
