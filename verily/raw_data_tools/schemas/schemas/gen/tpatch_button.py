# Lint as: python3
# pylint: disable=invalid-name
"""Registers the schema for com.verily.tpatch.button with Beam."""

import dataclasses
from typing import Optional

from verily.ds_sdk.core.schemas import DataPoint
from verily.ds_sdk.core.schemas import dataspec


@dataspec('com.verily.tpatch.button')
@dataclasses.dataclass
class TpatchButton(DataPoint):
    """Beam RowSchema for com.verily.tpatch.button."""
    held_millis: Optional[int] = None
