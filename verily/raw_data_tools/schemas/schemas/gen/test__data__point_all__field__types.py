# Lint as: python3
# pylint: disable=invalid-name
"""Registers the schema for com.verily.test_data_point.all_field_types with Beam."""  # pylint: disable=line-too-long

import dataclasses
from typing import List

from verily.raw_data_tools.schemas.schemas.shared_schemas import DataPoint
from verily.raw_data_tools.schemas.schemas.decorators import dataspec


@dataspec('com.verily.test_data_point.all_field_types')
@dataclasses.dataclass
class Test_Data_PointAll_Field_Types(DataPoint):
    """Beam RowSchema for com.verily.test_data_point.all_field_types."""
    blob_field: bytes
    blob_list_field: List[bytes]
    bool_field: bool
    bool_list_field: List[bool]
    float_field: float
    float_list_field: List[float]
    int_field: int
    int_list_field: List[int]
    string_field: str
    string_list_field: List[str]
