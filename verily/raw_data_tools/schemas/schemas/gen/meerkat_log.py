# Lint as: python3
# pylint: disable=invalid-name
"""Registers the schema for com.verily.meerkat.log with Beam."""

import dataclasses
from typing import Optional

from verily.raw_data_tools.schemas.schemas.shared_schemas import DataPoint
from verily.raw_data_tools.schemas.schemas.decorators import dataspec


@dataspec('com.verily.meerkat.log')
@dataclasses.dataclass
class MeerkatLog(DataPoint):
    """Beam RowSchema for com.verily.meerkat.log."""
    cap_id: Optional[str] = None
    log_description: Optional[str] = None
    log_type: Optional[int] = None
    log_type_detailed: Optional[int] = None
