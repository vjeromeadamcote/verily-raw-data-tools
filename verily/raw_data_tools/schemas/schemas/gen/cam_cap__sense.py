# Lint as: python3
# pylint: disable=invalid-name
"""Registers the schema for com.verily.cam.cap_sense with Beam."""

import dataclasses

from verily.ds_sdk.core.schemas import DataPoint
from verily.ds_sdk.core.schemas import dataspec


@dataspec('com.verily.cam.cap_sense')
@dataclasses.dataclass
class CamCap_Sense(DataPoint):
    """Beam RowSchema for com.verily.cam.cap_sense."""
    cap_sense: int
