# Lint as: python3
# pylint: disable=invalid-name
"""Registers the schema for com.verily.rialto.earlysense_compressed with Beam."""  # pylint: disable=line-too-long

import dataclasses

from verily.ds_sdk.core.schemas import DataPoint
from verily.ds_sdk.core.schemas import dataspec


@dataspec('com.verily.rialto.earlysense_compressed')
@dataclasses.dataclass
class RialtoEarlysense_Compressed(DataPoint):
    """Beam RowSchema for com.verily.rialto.earlysense_compressed."""
    value: bytes
