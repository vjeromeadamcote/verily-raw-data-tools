# Lint as: python3
# pylint: disable=invalid-name
"""Registers the schema for com.verily.studywatch.afib_detection_v2_result with Beam."""  # pylint: disable=line-too-long

import dataclasses
from typing import List, Optional

from apache_beam.utils.timestamp import Timestamp

from verily.ds_sdk.core.schemas import DataPoint
from verily.ds_sdk.core.schemas import dataspec


@dataspec('com.verily.studywatch.afib_detection_v2_result')
@dataclasses.dataclass
class StudywatchAfib_Detection_V2_Result(DataPoint):
    """Beam RowSchema for com.verily.studywatch.afib_detection_v2_result."""
    ibi_metrics_af_entropy: Optional[float] = None
    ibi_metrics_kurt_dibi: Optional[float] = None
    ibi_metrics_norm_ibi: Optional[float] = None
    ibi_metrics_nsr_entropy: Optional[float] = None
    ibi_metrics_sd_1: Optional[float] = None
    ibi_metrics_sd_2: Optional[float] = None
    ibi_metrics_std_dibi: Optional[float] = None
    ibi_metrics_std_ibi: Optional[float] = None
    peak_location_indices: Optional[List[int]] = None
    prediction: Optional[float] = None
    shannon_entropy: Optional[float] = None
    window_start_timestamp: Optional[Timestamp] = None
