# Lint as: python3
# pylint: disable=invalid-name
"""Registers the schema for com.verily.tonostand.ocular_drug_delivery with Beam."""  # pylint: disable=line-too-long

import dataclasses
from typing import List, Optional

from verily.raw_data_tools.schemas.schemas.shared_schemas import DataPoint
from verily.raw_data_tools.schemas.schemas.decorators import dataspec


@dataspec('com.verily.tonostand.ocular_drug_delivery')
@dataclasses.dataclass
class TonostandOcular_Drug_Delivery(DataPoint):
    """Beam RowSchema for com.verily.tonostand.ocular_drug_delivery."""
    blink_detected: Optional[List[bool]] = None
    blink_volts: Optional[List[float]] = None
    droplet_volts: Optional[List[float]] = None
    eye_location: Optional[str] = None
    successful_drops: Optional[int] = None
    time_offsets: Optional[List[int]] = None
    total_drops: Optional[int] = None
