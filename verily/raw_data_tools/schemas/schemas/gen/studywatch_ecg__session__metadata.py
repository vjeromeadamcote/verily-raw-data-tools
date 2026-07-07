# Lint as: python3
# pylint: disable=invalid-name
"""Registers the schema for com.verily.studywatch.ecg_session_metadata with Beam."""  # pylint: disable=line-too-long

import dataclasses
from typing import Optional

from verily.raw_data_tools.schemas.schemas.shared_schemas import DataPoint
from verily.raw_data_tools.schemas.schemas.decorators import dataspec


@dataspec('com.verily.studywatch.ecg_session_metadata')
@dataclasses.dataclass
class StudywatchEcg_Session_Metadata(DataPoint):
    """Beam RowSchema for com.verily.studywatch.ecg_session_metadata."""
    countdown_end_time: Optional[int] = None
    countdown_result: Optional[str] = None
    countdown_start_time: Optional[int] = None
    did_user_log_symptoms: Optional[bool] = None
    session_end_time: Optional[int] = None
    session_start_time: Optional[int] = None
    wrist_selection: Optional[str] = None
