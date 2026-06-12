# Lint as: python3
# pylint: disable=invalid-name
"""Registers the schema for test.prober with Beam."""

import dataclasses

from verily.ds_sdk.core.schemas import DataPoint
from verily.ds_sdk.core.schemas import dataspec


@dataspec('test.prober')
@dataclasses.dataclass
class TestProber(DataPoint):
    """Beam RowSchema for test.prober."""
    prober_field: int
