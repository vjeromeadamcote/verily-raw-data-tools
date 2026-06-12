# Lint as: python3
# pylint: disable=invalid-name
"""Registers the schema for com.verily.meerkat.log with Beam."""

import dataclasses
from typing import Optional

from verily.ds_sdk.core.schemas import DataPoint
from verily.ds_sdk.core.schemas import dataspec


@dataspec('com.verily.meerkat.log')
@dataclasses.dataclass
class MeerkatLog(DataPoint):
    """Beam RowSchema for com.verily.meerkat.log."""
    cap_id: Optional[str] = None
    log_description: Optional[str] = None
    log_type: Optional[int] = None
    log_type_detailed: Optional[int] = None
