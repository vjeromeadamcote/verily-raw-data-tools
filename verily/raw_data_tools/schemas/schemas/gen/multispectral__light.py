# Lint as: python3
# pylint: disable=invalid-name
"""Registers the schema for com.verily.multispectral_light with Beam."""

import dataclasses
from typing import Optional

from verily.ds_sdk.core.schemas import DataPoint
from verily.ds_sdk.core.schemas import dataspec


@dataspec('com.verily.multispectral_light')
@dataclasses.dataclass
class Multispectral_Light(DataPoint):
    """Beam RowSchema for com.verily.multispectral_light."""
    blue: Optional[int] = None
    degrees_from_normal: Optional[float] = None
    green: Optional[int] = None
    infrared_high: Optional[int] = None
    infrared_low: Optional[int] = None
    red: Optional[int] = None
    uv: Optional[int] = None
    white: Optional[int] = None
