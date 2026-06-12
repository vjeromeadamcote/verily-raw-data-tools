# Lint as: python3
# pylint: disable=invalid-name
"""Registers the schema for com.verily.hello with Beam."""

import dataclasses

from verily.ds_sdk.core.schemas import DataPoint
from verily.ds_sdk.core.schemas import dataspec


@dataspec('com.verily.hello')
@dataclasses.dataclass
class Hello(DataPoint):
    """Beam RowSchema for com.verily.hello."""
