# Lint as: python3
# pylint: disable=invalid-name
"""Registers the schema for com.verily.tonostand.iris_code with Beam."""

import dataclasses
from typing import Optional

from verily.ds_sdk.core.schemas import DataPoint
from verily.ds_sdk.core.schemas import dataspec


@dataspec('com.verily.tonostand.iris_code')
@dataclasses.dataclass
class TonostandIris_Code(DataPoint):
    """Beam RowSchema for com.verily.tonostand.iris_code."""
    iris_code: bytes
    iris_mask: bytes
    eye_location: Optional[str] = None
