# Lint as: python3
# pylint: disable=invalid-name
"""Registers the schema for com.verily.ziopatch.af_episode_characterization with Beam."""  # pylint: disable=line-too-long

import dataclasses
from typing import Optional

from verily.ds_sdk.core.schemas import DataPoint
from verily.ds_sdk.core.schemas import dataspec


@dataspec('com.verily.ziopatch.af_episode_characterization')
@dataclasses.dataclass
class ZiopatchAf_Episode_Characterization(DataPoint):
    """Beam RowSchema for com.verily.ziopatch.af_episode_characterization."""
    end_index: int
    event_type: str
    start_index: int
    total_samples: int
    avg_heart_rate: Optional[int] = None
    beats: Optional[int] = None
    bridge_type: Optional[str] = None
    duration: Optional[str] = None
    max_heart_rate: Optional[int] = None
    min_heart_rate: Optional[int] = None
