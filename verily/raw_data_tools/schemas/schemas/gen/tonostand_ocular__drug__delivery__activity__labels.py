# Lint as: python3
# pylint: disable=invalid-name
"""Registers the schema for com.verily.tonostand.ocular_drug_delivery_activity_labels with Beam."""  # pylint: disable=line-too-long

import dataclasses
from typing import Optional

from verily.raw_data_tools.schemas.schemas.shared_schemas import DataPoint
from verily.raw_data_tools.schemas.schemas.decorators import dataspec


@dataspec('com.verily.tonostand.ocular_drug_delivery_activity_labels')
@dataclasses.dataclass
class TonostandOcular_Drug_Delivery_Activity_Labels(DataPoint):
    """Beam RowSchema for com.verily.tonostand.ocular_drug_delivery_activity_labels."""  # pylint: disable=line-too-long
    light_switch_on: Optional[bool] = None
