# Lint as: python3
# pylint: disable=invalid-name
"""Registers the schema for com.verily.studywatch.user_tagged_event with Beam."""  # pylint: disable=line-too-long

import dataclasses
from typing import List, Optional

from apache_beam.utils.timestamp import Timestamp

from verily.raw_data_tools.schemas.schemas.shared_schemas import DataPoint
from verily.raw_data_tools.schemas.schemas.decorators import dataspec


@dataspec('com.verily.studywatch.user_tagged_event')
@dataclasses.dataclass
class StudywatchUser_Tagged_Event(DataPoint):
    """Beam RowSchema for com.verily.studywatch.user_tagged_event."""
    app_ended_timestamp: Timestamp
    app_started_timestamp: Timestamp
    event_category: str
    event_end_timestamp: Timestamp
    event_type: str
    is_auto_answered: Optional[bool] = None
    is_deleted: Optional[bool] = None
    question_prompt: Optional[List[str]] = None
    response: Optional[List[str]] = None
    responses: Optional[List[str]] = None
    survey_response_timestamp: Optional[List[Timestamp]] = None
