# Lint as: python3
# pylint: disable=invalid-name
"""Registers the schema for com.verily.studywatch.clinical_event with Beam."""

import dataclasses
from typing import Optional

from verily.ds_sdk.core.schemas import DataPoint
from verily.ds_sdk.core.schemas import dataspec


@dataspec('com.verily.studywatch.clinical_event')
@dataclasses.dataclass
class StudywatchClinical_Event(DataPoint):
    """Beam RowSchema for com.verily.studywatch.clinical_event."""
    event: Optional[str] = None
