# Lint as: python3
# pylint: disable=invalid-name
"""Registers the schema for com.verily.tonostand.iris_code with Beam."""

import dataclasses
from typing import Optional

from verily.raw_data_tools.schemas.schemas.shared_schemas import DataPoint
from verily.raw_data_tools.schemas.schemas.decorators import dataspec


@dataspec('com.verily.tonostand.iris_code')
@dataclasses.dataclass
class TonostandIris_Code(DataPoint):
    """Beam RowSchema for com.verily.tonostand.iris_code."""
    iris_code: bytes
    iris_mask: bytes
    eye_location: Optional[str] = None
