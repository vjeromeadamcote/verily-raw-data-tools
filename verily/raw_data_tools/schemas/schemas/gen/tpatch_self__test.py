# Lint as: python3
# pylint: disable=invalid-name
"""Registers the schema for com.verily.tpatch.self_test with Beam."""

import dataclasses
from typing import Optional

from verily.raw_data_tools.schemas.schemas.shared_schemas import DataPoint
from verily.raw_data_tools.schemas.schemas.decorators import dataspec


@dataspec('com.verily.tpatch.self_test')
@dataclasses.dataclass
class TpatchSelf_Test(DataPoint):
    """Beam RowSchema for com.verily.tpatch.self_test."""
    atecc608_passed: Optional[str] = None
    bma456_passed: Optional[str] = None
    passed: Optional[str] = None
    tmp117_ambient_passed: Optional[str] = None
    tmp117_skin_passed: Optional[str] = None
