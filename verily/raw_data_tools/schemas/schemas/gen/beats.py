# Lint as: python3
# pylint: disable=invalid-name
"""Registers the schema for com.verily.beats with Beam."""

import dataclasses

from verily.ds_sdk.core.schemas import DataPoint
from verily.ds_sdk.core.schemas import dataspec


@dataspec('com.verily.beats')
@dataclasses.dataclass
class Beats(DataPoint):
    """Beam RowSchema for com.verily.beats."""
