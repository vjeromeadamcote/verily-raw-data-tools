# Lint as: python3
# pylint: disable=invalid-name
"""Registers the schema for test.prober with Beam."""

import dataclasses

from verily.raw_data_tools.schemas.schemas.shared_schemas import DataPoint
from verily.raw_data_tools.schemas.schemas.decorators import dataspec


@dataspec('test.prober')
@dataclasses.dataclass
class TestProber(DataPoint):
    """Beam RowSchema for test.prober."""
    prober_field: int
