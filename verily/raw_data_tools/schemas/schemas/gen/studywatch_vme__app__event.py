# Lint as: python3
# pylint: disable=invalid-name
"""Registers the schema for com.verily.studywatch.vme_app_event with Beam."""

import dataclasses
from typing import List, Optional

from apache_beam.utils.timestamp import Timestamp

from verily.ds_sdk.core.schemas import DataPoint
from verily.ds_sdk.core.schemas import dataspec


@dataspec('com.verily.studywatch.vme_app_event')
@dataclasses.dataclass
class StudywatchVme_App_Event(DataPoint):
    """Beam RowSchema for com.verily.studywatch.vme_app_event."""
    type: str
    activities_end_timestamp: Optional[List[Timestamp]] = None
    activities_start_timestamp: Optional[List[Timestamp]] = None
    activities_status: Optional[List[str]] = None
    activities_type: Optional[List[str]] = None
    activities_user_rating: Optional[List[str]] = None
    activities_activity_num: Optional[List[int]] = None
    activities_num_post_activity_questions: Optional[List[int]] = None
    activities_num_pre_activity_questions: Optional[List[int]] = None
    activities_post_activity_questions_index: Optional[List[int]] = None
    activities_pre_activity_questions_index: Optional[List[int]] = None
    app_ended_timestamp: Optional[Timestamp] = None
    app_started_timestamp: Optional[Timestamp] = None
    first_user_interaction_timestamp: Optional[Timestamp] = None
    is_ended_early: Optional[bool] = None
    is_snoozed: Optional[bool] = None
    is_user_started: Optional[bool] = None
    likely_medication_status: Optional[str] = None
    num_completed_activities: Optional[int] = None
    num_expected_activities: Optional[int] = None
    question_prompt: Optional[List[str]] = None
    response: Optional[List[str]] = None
    responses: Optional[List[str]] = None
    survey_response_timestamp: Optional[List[Timestamp]] = None
