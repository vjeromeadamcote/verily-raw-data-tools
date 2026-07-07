# Lint as: python3
# pylint: disable=invalid-name
"""Registers the schema for com.verily.walking_suite with Beam."""

import dataclasses
from typing import Optional

from verily.raw_data_tools.schemas.schemas.shared_schemas import DataPoint
from verily.raw_data_tools.schemas.schemas.decorators import dataspec


@dataspec('com.verily.walking_suite')
@dataclasses.dataclass
class Walking_Suite(DataPoint):
    """Beam RowSchema for com.verily.walking_suite."""
    bout_cadence_25_pct: Optional[float] = None
    bout_cadence_5_pct: Optional[float] = None
    bout_cadence_75_pct: Optional[float] = None
    bout_cadence_95_pct: Optional[float] = None
    bout_cadence_mean: Optional[float] = None
    bout_cadence_median: Optional[float] = None
    bout_cadence_std: Optional[float] = None
    bout_duration_25_pct: Optional[float] = None
    bout_duration_5_pct: Optional[float] = None
    bout_duration_75_pct: Optional[float] = None
    bout_duration_95_pct: Optional[float] = None
    bout_duration_mean: Optional[float] = None
    bout_duration_median: Optional[float] = None
    bout_duration_std: Optional[float] = None
    cadence_25_pct: Optional[float] = None
    cadence_5_pct: Optional[float] = None
    cadence_75_pct: Optional[float] = None
    cadence_95_pct: Optional[float] = None
    cadence_mean: Optional[float] = None
    cadence_median: Optional[float] = None
    cadence_std: Optional[float] = None
    cm4_elasticnet_part3_minus_tremor: Optional[float] = None
    daily_ambulatory_minutes: Optional[float] = None
    daily_num_bout: Optional[float] = None
    daily_num_bout_15mn: Optional[float] = None
    daily_num_bout_1mn: Optional[float] = None
    daily_num_bout_2mn: Optional[float] = None
    daily_num_bout_30s_1mn: Optional[float] = None
    daily_num_bout_5mn: Optional[float] = None
    daily_representation_hours: Optional[float] = None
    daily_step_count: Optional[float] = None
    daily_top_15mn_cadence: Optional[float] = None
    daily_top_30mn_cadence: Optional[float] = None
    daily_top_60mn_cadence: Optional[float] = None
    frac_num_bout_15mn: Optional[float] = None
    frac_num_bout_1mn: Optional[float] = None
    frac_num_bout_2mn: Optional[float] = None
    frac_num_bout_30s_1mn: Optional[float] = None
    frac_num_bout_5mn: Optional[float] = None
    length: Optional[int] = None
    length_long_bout_cadence: Optional[int] = None
    length_valid_cadence: Optional[int] = None
    nwb_bout_duration_25_pct: Optional[float] = None
    nwb_bout_duration_5_pct: Optional[float] = None
    nwb_bout_duration_75_pct: Optional[float] = None
    nwb_bout_duration_95_pct: Optional[float] = None
    nwb_bout_duration_mean: Optional[float] = None
    nwb_bout_duration_median: Optional[float] = None
    nwb_bout_duration_std: Optional[float] = None
    nwb_daily_bout_gini: Optional[float] = None
    nwb_daily_bout_skewness: Optional[float] = None
    nwb_daily_bout_time_to_80: Optional[float] = None
    nwb_daily_non_ambulatory_minutes: Optional[float] = None
    nwb_daily_num_bout: Optional[float] = None
    nwb_daily_num_bout_15mn: Optional[float] = None
    nwb_daily_num_bout_1mn: Optional[float] = None
    nwb_daily_num_bout_2mn: Optional[float] = None
    nwb_daily_num_bout_30s_1mn: Optional[float] = None
    nwb_daily_num_bout_5mn: Optional[float] = None
    nwb_daily_representation_hours: Optional[float] = None
    nwb_frac_num_bout_15mn: Optional[float] = None
    nwb_frac_num_bout_1mn: Optional[float] = None
    nwb_frac_num_bout_2mn: Optional[float] = None
    nwb_frac_num_bout_30s_1mn: Optional[float] = None
    nwb_frac_num_bout_5mn: Optional[float] = None
    time_window_hours: Optional[float] = None
