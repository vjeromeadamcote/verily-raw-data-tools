# Lint as: python3
# pylint: disable=invalid-name
"""Registers the schema for com.verily.rialto.earlysense_compressed with Beam."""  # pylint: disable=line-too-long

import dataclasses

from verily.raw_data_tools.schemas.schemas.shared_schemas import DataPoint
from verily.raw_data_tools.schemas.schemas.decorators import dataspec


@dataspec('com.verily.rialto.earlysense_compressed')
@dataclasses.dataclass
class RialtoEarlysense_Compressed(DataPoint):
    """Beam RowSchema for com.verily.rialto.earlysense_compressed."""
    value: bytes
