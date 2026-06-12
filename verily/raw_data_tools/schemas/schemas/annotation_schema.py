# Lint as: python3
"""NamedTuple classes (schemas) shared across all data types."""

import dataclasses
from typing import List, Optional

from apache_beam.utils.timestamp import Timestamp

# TODO(tanke): Model annotations as a DataPoint.
# Copying schema from derived_annotations BigQuery table schema for now.
# Note: Dropping user_id from the schema.


@dataclasses.dataclass
class MetricType:
    stream_item_type: Optional[int]
    derived_data_type: Optional[int]
    annotation_type: Optional[int]


@dataclasses.dataclass
class InputDataInfo:
    version_number: int
    version_name: str
    metric_type: MetricType


@dataclasses.dataclass
class AnnotationMetadata:
    device_id: str
    participant_id: Optional[str]
    participant_namespace: Optional[int]
    version_name: Optional[str]
    version_number: Optional[int]
    input_data_info: List[InputDataInfo]


@dataclasses.dataclass
class Annotation:
    annotation_label: str
    start_timestamp_utc: Timestamp
    end_timestamp_utc: Timestamp
    annotation_metadata: AnnotationMetadata
