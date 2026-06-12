"""Utility methods for data spec schemas."""

import dataclasses
from typing import List, Optional, Set, Type

from apache_beam.utils.timestamp import Timestamp
import pandas as pd

from verily.ds_sdk.core.schemas import shared_schemas

METADATA_FIELD = 'data_point_metadata'
TIMESTAMP_FIELD = 'measurement_timestamp_utc'


def get_class_name(data_spec_name: str) -> str:
    measurement_type = data_spec_name.replace('com.verily.', '')

    # data_spec.name -> Data_SpecName
    return ''.join([s.title() for s in measurement_type.split('.')])


def validate_required_fields(schema_class: Type[shared_schemas.DataPointType]):
    schema_properties = get_schema_fields(schema_class)

    if METADATA_FIELD not in schema_properties:
        raise ValueError(
            f'A {METADATA_FIELD} field of type '
            'verily.ds_sdk.core.schemas.shared_schemas.'
            f'{shared_schemas.DataPointMetadata.__name__} is required to write '
            f'data to SensorStore or BigQuery for type: {schema_class}')

    if TIMESTAMP_FIELD not in schema_properties:
        raise ValueError(
            f'A {TIMESTAMP_FIELD} field of type '
            f'apache_beam.utils.timestamp.{Timestamp.__name__} is required to '
            'write data to SensorStore or BigQuery for type: '
            f'{schema_class}.')


def get_schema_fields(
        schema_class: Type[shared_schemas.DataPointType]) -> Set[str]:
    if not dataclasses.is_dataclass(schema_class):
        raise ValueError('All DS SDK schemas must be a dataclass. '
                         f'{schema_class.__name__} was not a dataclass')
    return set(field.name for field in dataclasses.fields(schema_class))


def data_point_metadata_for_raw_data(
        data_source_id: Optional[int], device_id: str,
        participant_id: Optional[str], participant_namespace: Optional[int],
        echo_metadata: Optional[shared_schemas.EchoMetadata],
        sensor_store_metadata: Optional[shared_schemas.SensorStoreMetadata],
        annotation_labels: Set[str]) -> shared_schemas.DataPointMetadata:
    return shared_schemas.DataPointMetadata(
        data_source_id=data_source_id,
        device_id=device_id,
        participant_id=participant_id,
        participant_namespace=participant_namespace,
        echo_metadata=echo_metadata,
        sensor_store_metadata=sensor_store_metadata,
        annotation_labels=annotation_labels,
        _state_key=shared_schemas._STATE_KEY.CREATED_USING_BUILDER)  # pylint: disable=protected-access


def data_point_metadata_for_derived_data(
    input_data_points: List[shared_schemas.DataPointType]
) -> shared_schemas.DataPointMetadata:
    if not input_data_points:
        raise ValueError(
            'data_point_metadata_for_derived_data requires at least one input'
            ' data point. Received an empty list.')

    def get_min(
            left: Optional[shared_schemas.DataPointType],
            right: shared_schemas.DataPointType
    ) -> shared_schemas.DataPointType:
        if left is None:
            return right
        left_key = (left.measurement_timestamp_utc,
                    left.data_point_metadata.data_source_id)
        right_key = (right.measurement_timestamp_utc,
                     right.data_point_metadata.data_source_id)
        if left_key <= right_key:
            return left
        return right

    device_ids = set()
    participant_tuples = set()
    annotation_labels = set()
    sensor_store_write_times = set()
    min_point = None

    for point in input_data_points:
        # The min point's DataPointMetadata will be used to ensure the output
        # is deterministic
        min_point = get_min(min_point, point)

        device_ids.add(point.data_point_metadata.device_id)
        participant_tuples.add(
            (point.data_point_metadata.participant_id,
             point.data_point_metadata.participant_namespace))

        ss_meta = point.data_point_metadata.sensor_store_metadata
        if ss_meta is not None:
            sensor_store_write_times.add(ss_meta.sensor_store_write_time)

        annotation_labels.update(point.data_point_metadata.annotation_labels)

    if len(device_ids) > 1:
        raise RuntimeError(
            'data_point_metadata_for_derived_data encountered multiple Device'
            f'IDs while parsing the source data points: {sorted(device_ids)}')
    device_id = list(device_ids)[0]

    # Filters out (None, None) tuples
    participant_tuples = set(
        filter(lambda t: t != (None, None), participant_tuples))
    if len(participant_tuples) > 1:
        raise RuntimeError(
            'data_point_metadata_for_derived_data encountered multiple '
            'Participants while parsing the source data points: '
            f'{sorted(participant_tuples)}')
    elif len(participant_tuples) == 1:
        participant_id, participant_namespace = list(participant_tuples)[0]
    else:
        participant_id, participant_namespace = None, None

    sensor_store_metadata = None
    if len(sensor_store_write_times) > 0:
        sensor_store_metadata = shared_schemas.SensorStoreMetadata(
            max(sensor_store_write_times))

    return shared_schemas.DataPointMetadata(
        data_source_id=min_point.data_point_metadata.  # type: ignore
        data_source_id,
        device_id=device_id,
        participant_id=participant_id,
        participant_namespace=participant_namespace,
        echo_metadata=None,
        sensor_store_metadata=sensor_store_metadata,
        annotation_labels=annotation_labels,
        _state_key=shared_schemas._STATE_KEY.CREATED_USING_BUILDER)  # pylint: disable=protected-access


def data_point_metadata_for_derived_data_from_df(
        df: pd.DataFrame) -> shared_schemas.DataPointMetadata:
    data_point_metadata = df.attrs.get('data_point_metadata', None)
    if data_point_metadata is None:
        raise RuntimeError(
            'No value for `data_point_metadata` in DataFrame.attrs. This method'
            ' only works for DataFrames built using the ds_sdk API.')
    return data_point_metadata
