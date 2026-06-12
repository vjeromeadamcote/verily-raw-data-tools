# Lint as: python3
# pylint: disable=invalid-name
"""Registers the schema for com.verily.galvanic_skin_response with Beam."""

import dataclasses

from verily.ds_sdk.core.schemas import DataPoint
from verily.ds_sdk.core.schemas import dataspec


@dataspec('com.verily.galvanic_skin_response')
@dataclasses.dataclass
class Galvanic_Skin_Response(DataPoint):
    """Beam RowSchema for com.verily.galvanic_skin_response."""
    resistance: int
