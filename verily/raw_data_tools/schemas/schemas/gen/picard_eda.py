# Lint as: python3
# pylint: disable=invalid-name
"""Registers the schema for com.verily.picard.eda with Beam."""

import dataclasses
from typing import List, Optional

from verily.ds_sdk.core.schemas import DataPoint
from verily.ds_sdk.core.schemas import dataspec


@dataspec('com.verily.picard.eda')
@dataclasses.dataclass
class PicardEda(DataPoint):
    """Beam RowSchema for com.verily.picard.eda."""
    abs_adc: Optional[List[int]] = None
    im_adc: Optional[List[int]] = None
    more_info: Optional[List[int]] = None
    on_wrist: Optional[bool] = None
    real_adc: Optional[List[int]] = None
    sampling_rate: Optional[int] = None
    sensor_id: Optional[int] = None
    true_timestamp_millis: Optional[int] = None
    true_timestamp_sample_index: Optional[int] = None
    z2_adc: Optional[List[int]] = None
