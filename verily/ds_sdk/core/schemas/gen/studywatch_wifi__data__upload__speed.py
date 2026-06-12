# Lint as: python3
# pylint: disable=invalid-name
"""Registers the schema for com.verily.studywatch.wifi_data_upload_speed with Beam."""  # pylint: disable=line-too-long

import dataclasses
from typing import Optional

from verily.ds_sdk.core.schemas import DataPoint
from verily.ds_sdk.core.schemas import dataspec


@dataspec('com.verily.studywatch.wifi_data_upload_speed')
@dataclasses.dataclass
class StudywatchWifi_Data_Upload_Speed(DataPoint):
    """Beam RowSchema for com.verily.studywatch.wifi_data_upload_speed."""
    bytes_uploaded: Optional[int] = None
    speed_kbps: Optional[int] = None
    upload_duration_ms: Optional[int] = None
