# Lint as: python3
# pylint: disable=invalid-name
"""Registers the schema for com.verily.annotation.out_of_range_data with Beam."""  # pylint: disable=line-too-long

import dataclasses
from typing import Optional

from verily.ds_sdk.core.schemas import DataPoint
from verily.ds_sdk.core.schemas import dataspec


@dataspec('com.verily.annotation.out_of_range_data')
@dataclasses.dataclass
class AnnotationOut_Of_Range_Data(DataPoint):
    """Beam RowSchema for com.verily.annotation.out_of_range_data."""
    end_time_millis: Optional[int] = None
    field: Optional[str] = None
    supplemental_expected_maximum: Optional[float] = None
    supplemental_expected_minimum: Optional[float] = None
    supplemental_source_algorithm_name: Optional[str] = None
    supplemental_source_algorithm_version: Optional[str] = None
    supplemental_source_data_spec: Optional[str] = None
    supplemental_source_sensor_id: Optional[str] = None
