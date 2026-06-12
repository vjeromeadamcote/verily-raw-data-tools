# Lint as: python3
"""NamedTuple classes (schemas) shared across all data types."""

import dataclasses
from enum import IntEnum
from typing import Optional, Set, TypeVar

from apache_beam.utils.timestamp import Timestamp


class _STATE_KEY(IntEnum):  # pylint: disable=invalid-name
    CREATED_USING_INIT = 0
    CREATED_USING_BUILDER = 1


@dataclasses.dataclass
class SensorStoreMetadata:
    sensor_store_write_time: Timestamp


@dataclasses.dataclass
class EchoMetadata:
    bucket_start: Timestamp
    bucket_write_time: Timestamp
    deleted_time: Optional[Timestamp]
    # TODO(b/213343867): This shouldn't be optional.
    snapshot_time: Optional[Timestamp]


@dataclasses.dataclass
class DataPointMetadata:
    """dataclass for store DataPoint metadata."""
    data_source_id: Optional[int]
    device_id: str
    participant_id: Optional[str]
    participant_namespace: Optional[int]
    echo_metadata: Optional[EchoMetadata]
    sensor_store_metadata: Optional[SensorStoreMetadata]
    annotation_labels: Set[str]
    _state_key: int = _STATE_KEY.CREATED_USING_INIT

    def __post_init__(self):
        if self._state_key == _STATE_KEY.CREATED_USING_INIT:
            raise ValueError(
                'DataPointMetadata cannot be created using __init__(). Please '
                'use `schemas.data_point_metadata_for_raw_data` or '
                '`schemas.data_point_metadata_for_derived_data` instead.')

    def __repr__(self) -> str:
        fields = [
            f'{k}={str(v)}' for k, v in self.__dict__.items()
            if k != '_state_key'
        ]
        return f'DataPointMetadata({", ".join(fields)})'


@dataclasses.dataclass
class DataPoint:
    data_point_metadata: DataPointMetadata
    measurement_timestamp_utc: Timestamp


DataPointType = TypeVar('DataPointType', bound=DataPoint)  # pylint: disable=invalid-name
