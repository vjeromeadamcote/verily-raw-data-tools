# Lint as: python3
# pylint: disable=invalid-name
"""Registers the schema for com.verily.pblite.provisioning with Beam."""

import dataclasses
from typing import Optional

from verily.ds_sdk.core.schemas import DataPoint
from verily.ds_sdk.core.schemas import dataspec


@dataspec('com.verily.pblite.provisioning')
@dataclasses.dataclass
class PbliteProvisioning(DataPoint):
    """Beam RowSchema for com.verily.pblite.provisioning."""
    firmware_version: Optional[str] = None
