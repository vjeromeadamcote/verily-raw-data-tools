# Lint as: python3
# pylint: disable=invalid-name
"""Registers the schema for com.verily.hypnos.resmed.raw with Beam."""

import dataclasses
from typing import Optional

from verily.ds_sdk.core.schemas import DataPoint
from verily.ds_sdk.core.schemas import dataspec


@dataspec('com.verily.hypnos.resmed.raw')
@dataclasses.dataclass
class HypnosResmedRaw(DataPoint):
    """Beam RowSchema for com.verily.hypnos.resmed.raw."""
    source_json: Optional[bytes] = None
