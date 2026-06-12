"""BigQuery Source implementation for DataPoints (Echo & Custom)."""

import copy
import dataclasses
import logging
from typing import Any, Dict, List, Optional, Set, Tuple, Type, Union
import uuid

import apache_beam as beam
from apache_beam.utils.timestamp import Timestamp
import typing_inspect

from verily.ds_sdk.core import conditions
from verily.ds_sdk.core import options
from verily.ds_sdk.core import query_runner
from verily.ds_sdk.core import schema_fetcher
from verily.ds_sdk.core import schemas
from verily.ds_sdk.core.gcp import bigquery_source_wrapper
from verily.ds_sdk.core.gcp import credentials
from verily.ds_sdk.core.io import cacheable_source
from verily.ds_sdk.core.io.bigquery import annotation_source
from verily.ds_sdk.core.io.bigquery import build_data_source_cache
from verily.ds_sdk.core.io.bigquery import build_row_filters
from verily.ds_sdk.core.io.bigquery import echo_dedupe
from verily.ds_sdk.core.io.bigquery import filter_by_annotation
from verily.ds_sdk.core.io.bigquery import incremental_query
from verily.ds_sdk.core.io.bigquery import participant_mappings
from verily.ds_sdk.core.io.bigquery.utils import cache_utils
from verily.ds_sdk.core.io.bigquery.utils import echo_utils
from verily.ds_sdk.core.sensorsuite import derived_data_sources
from verily.ds_sdk.core.utils import timestamps
from verily.ds_sdk.protos import types_pb2


def _parse_internal_echo_data_point_row(
    bigquery_row: Dict[str, Any],
    schema: Type,
    participant_info: Optional[participant_mappings.ParticipantInfo],
) -> schemas.DataPointType:  # type: ignore
    device_id = bigquery_row['DeviceID']
    measurement_timestamp = timestamps.parse_bigquery_timestamp(
        bigquery_row['DataPointTime']
    )

    data_source_id = bigquery_row['DataSourceID']

    echo_metadata = schemas.EchoMetadata(
        bucket_start=measurement_timestamp,
        # TODO(b/231162492): Rename bucket_write_time to
        # data_point_write_time.
        bucket_write_time=timestamps.parse_bigquery_timestamp(
            bigquery_row['DataPointWriteTime'], allow_null=True
        ),
        deleted_time=timestamps.parse_bigquery_timestamp(
            bigquery_row['DeletedTime'], allow_null=True
        ),
        snapshot_time=timestamps.parse_bigquery_timestamp(
            bigquery_row['SnapshotTime'], allow_null=True
        ),
    )
    is_deleted = bool(echo_metadata.deleted_time)

    participant_id = (
        None if participant_info is None else participant_info.participant_id
    )
    participant_namespace = (
        None
        if participant_info is None
        else participant_info.participant_namespace
    )

    data_point_metadata = schemas.data_point_metadata_for_raw_data(
        data_source_id=data_source_id,
        device_id=device_id,
        participant_id=participant_id,
        participant_namespace=participant_namespace,
        echo_metadata=echo_metadata,
        sensor_store_metadata=None,
        annotation_labels=set(),
    )

    data_point_dict = bigquery_row['DataPoint']
    data_point_dict['measurement_timestamp_utc'] = measurement_timestamp
    data_point_dict['data_point_metadata'] = data_point_metadata

    # Ensure all subfields that are timestamps are parsed correctly.
    schema_fields = dataclasses.fields(schema)
    for field in schema_fields:
        field_name = field.name
        field_type = field.type
        is_optional = typing_inspect.is_optional_type(field_type)
        field_val = data_point_dict.get(field_name, None)

        # Deleted DataPoints may have null values in required fields
        if field_val is None and not is_optional and not is_deleted:
            raise RuntimeError(
                f'Non-optional field {field_name} not set in table.'
            )
        if field_val is None or isinstance(field_val, Timestamp):
            # already converted to a timestamp no need to do anything.
            continue
        if is_optional:
            # If optional grab the inner type.
            field_type = typing_inspect.get_args(field_type)[0]
        if field_type == Timestamp:
            data_point_dict[field_name] = timestamps.parse_bigquery_timestamp(
                data_point_dict[field_name], is_optional
            )
        elif field_type in [List[Timestamp], Set[Timestamp]]:
            new_timestamps: Union[List[Any], Set[Any]] = []
            for timestamp in data_point_dict[field_name]:
                new_timestamps.append(  # type: ignore
                    timestamps.parse_bigquery_timestamp(timestamp)
                )
            if field_type == Set[Timestamp]:
                new_timestamps = set(new_timestamps)
            data_point_dict[field_name] = new_timestamps

    return schema(**data_point_dict)


def _parse_data_point_row(
    bigquery_row: Dict[str, Any],
    schema: Type,
    participant_info: Optional[participant_mappings.ParticipantInfo],
) -> Tuple[bytes, schemas.DataPointType]:
    device_id = bigquery_row['DeviceID']
    measurement_timestamp = timestamps.parse_bigquery_timestamp(
        bigquery_row['DataPointTime']
    )

    if bigquery_row['DataSource'] is not None:
        data_source = echo_utils.parse_data_source(bigquery_row['DataSource'])
    else:
        # The DataSource will only be null for custom DataPoint BQ tables, so we
        # set the DataSource as the default DataSource for derived DataPoints.
        data_source = derived_data_sources.get_base_data_source(device_id)

    data_source_id = cache_utils.hash_data_source_proto(data_source)

    echo_metadata = None
    # Legacy schema uses BucketWriteTime and new internal schema uses
    # DataPointWriteTime. If either is set, populate EchoMetadata
    # TODO(b/231162492): Rename bucket_write_time to
    # data_point_write_time.
    if (
        'BucketWriteTime' in bigquery_row
        or 'DataPointWriteTime' in bigquery_row
    ):
        bucket_write_time = bigquery_row.get(
            'BucketWriteTime', None
        ) or bigquery_row.get('DataPointWriteTime', None)
        bucket_start = (
            timestamps.parse_bigquery_timestamp(
                bigquery_row.get('BucketStart', None), allow_null=True
            )
            or measurement_timestamp
        )
        echo_metadata = schemas.EchoMetadata(
            bucket_start=bucket_start,
            bucket_write_time=timestamps.parse_bigquery_timestamp(
                bucket_write_time
            ),
            deleted_time=timestamps.parse_bigquery_timestamp(
                bigquery_row.get('DeletedTime', None), allow_null=True
            ),
            snapshot_time=timestamps.parse_bigquery_timestamp(
                bigquery_row.get('SnapshotTime', None), allow_null=True
            ),
        )
    is_deleted = echo_metadata and echo_metadata.deleted_time

    participant_id = (
        None if participant_info is None else participant_info.participant_id
    )
    participant_namespace = (
        None
        if participant_info is None
        else participant_info.participant_namespace
    )

    data_point_metadata = schemas.data_point_metadata_for_raw_data(
        data_source_id=data_source_id,
        device_id=device_id,
        participant_id=participant_id,
        participant_namespace=participant_namespace,
        echo_metadata=echo_metadata,
        sensor_store_metadata=None,
        annotation_labels=set(),
    )

    data_point_dict = bigquery_row['DataPoint']
    if 't' in data_point_dict:
        del data_point_dict['t']
    data_point_dict['measurement_timestamp_utc'] = measurement_timestamp
    data_point_dict['data_point_metadata'] = data_point_metadata

    # Ensure all subfields that are timestamps are parsed correctly.
    schema_fields = dataclasses.fields(schema)
    for field in schema_fields:
        field_name = field.name
        field_type = field.type
        is_optional = typing_inspect.is_optional_type(field_type)
        field_val = data_point_dict.get(field_name, None)

        # Deleted DataPoints may have null values in required fields
        if field_val is None and not is_optional and not is_deleted:
            raise RuntimeError(
                f'Non-optional field {field_name} not set in table.'
            )
        if field_val is None or isinstance(field_val, Timestamp):
            # already converted to a timestamp no need to do anything.
            continue
        if is_optional:
            # If optional grab the inner type.
            field_type = typing_inspect.get_args(field_type)[0]
        if field_type == Timestamp:
            data_point_dict[field_name] = timestamps.parse_bigquery_timestamp(
                data_point_dict[field_name], is_optional
            )
        elif field_type in [List[Timestamp], Set[Timestamp]]:
            new_timestamps: Union[List[Any], Set[Any]] = []
            for timestamp in data_point_dict[field_name]:
                new_timestamps.append(  # type: ignore
                    timestamps.parse_bigquery_timestamp(timestamp)
                )
            if field_type == Set[Timestamp]:
                new_timestamps = set(new_timestamps)
            data_point_dict[field_name] = new_timestamps

    # We need to convert the DataSource proto to bytes since Beam struggles
    # with encoding the Proto.
    return (data_source.SerializeToString(), schema(**data_point_dict))


class _ParseDataPointRowDoFn(beam.DoFn):
    """Parses Echo row into the tuple: (DataSource, DataPoint w/ metadata)."""

    def __init__(self, schema: Type, is_internal_echo_format: bool):
        super().__init__()
        self._schema = schema
        self._is_internal_echo_format = is_internal_echo_format

    def process(  # type: ignore[override]
        self,
        bigquery_row: Dict[str, Any],
        participant_mappings_dict: Dict[
            str, List[participant_mappings.ParticipantInfo]
        ],
    ):
        device_id = bigquery_row['DeviceID']
        measurement_timestamp = timestamps.parse_bigquery_timestamp(
            bigquery_row['DataPointTime']
        )

        participant_less_counter = beam.metrics.Metrics.counter(
            'data_points_source', 'points_with_no_participant_association'
        )

        participant_info = None
        if device_id in participant_mappings_dict:
            device_participant_associations = participant_mappings_dict[
                device_id
            ]

            if measurement_timestamp is not None:
                participant_info = (
                    participant_mappings.get_participant_info_at_timestamp(  # pylint: disable=line-too-long
                        device_id,
                        measurement_timestamp,
                        device_participant_associations,
                    )
                )
        if participant_info is None:
            participant_less_counter.inc()
            logging.warning(
                'no participant info found for data point row: %s', bigquery_row
            )
        if self._is_internal_echo_format:
            yield _parse_internal_echo_data_point_row(
                bigquery_row, self._schema, participant_info
            )
        else:
            yield _parse_data_point_row(
                bigquery_row, self._schema, participant_info
            )


class DataPointRowSource(cacheable_source.CacheablePTransform):
    """Reads from BigQuery and parses rows into DataPoints."""

    def __init__(
        self,
        *,
        data_point_table_id: str,
        participant_table_id: Optional[str],
        schema: Type,
        condition: Optional[conditions.Condition],
        source_options: options.BatchSourceOptions,
        annotation_inner_join_options: Optional[
            options.AnnotationInnerJoinOptions
        ],
        incremental_query_options: Optional[options.IncrementalQueryOptions],
        creds: credentials.DsSdkCredentials,
        env: str,
        billing_project: str,
        service_account: str,
        bigquery_location: str,
        data_source_mappings_table_id: Optional[str] = None,
        data_spec_name: Optional[str] = None,
        use_internal_echo: bool,
        use_dedupe_in_query: bool = False,
    ):
        """Creates a DataPointRowSource PTransform.

        Args:
          data_point_table_id: The BigQuery table reference in the form:
            `project.dataset.table`.
          participant_table_id: The table containing the participant id mappings
          in the form: `project.dataset.table`.
          schema: The DataPointType schema class to use for parsing the BigQuery
            rows.
          condition: Conditions to apply to the BigQuery table.
          source_options: The required options for building DS SDK sources.
          annotation_inner_join_options: Options for joining on annotations
            stored in Echo BigQuery.
          incremental_query_options: Options for building incremental BigQuery
            queries.
          creds: Credentials object used to generate user/project credentials.
          env: The environment to run in. Options are: qa, preprod, prod, &
            prod-batch.
          billing_project: The GCP project to bill resource usage to.
          service_account: The service account to auth with BigQuery. NOTE: This
            is only used on google3 / Flume.
          bigquery_location: The location the BigQuery table lives in (US | EU).
          data_source_mappings_table_id: The BigQuery table containing the
            DataSource mappings to build the DataSourceCache from.
          data_spec_name: The DataSpec name associated with
            `data_point_table_id`.
        """
        super().__init__(disable_cache=source_options.disable_cache)

        # Validate Inputs
        # NOTE: data points written by the DSSDK are still written in legacy
        # format (DataSources stored alongside DataPoints), so we need to keep
        # the use_internal_echo option here to handle reading in these tables.
        if use_internal_echo:
            if 'internal_' not in data_point_table_id:
                raise ValueError(
                    f'{data_point_table_id} is not an internal Echo table.'
                    'Consider turning off the `use_internal_echo`, or reach out'
                    ' to sensors-infra if you  believe this is a valid internal'
                    ' Echo table.'
                )
            if data_source_mappings_table_id is None:
                raise ValueError(
                    '`data_source_mappings_table_id` must be set when reading '
                    'from internal Echo tables.'
                )
            if data_spec_name is None:
                raise ValueError(
                    '`data_spec_name` must be set when reading from internal '
                    'Echo tables.'
                )

        # Prepare Inputs
        self._data_point_table_id = data_point_table_id
        self._participant_table_id = participant_table_id
        self._schema = schema
        self._source_options = source_options
        self._condition = condition
        self._incremental_query_options = incremental_query_options
        self._annotation_inner_join_options = annotation_inner_join_options
        self._creds = creds
        self._env = env
        self._billing_project = billing_project
        self._service_account = service_account
        self._bigquery_location = bigquery_location
        self._data_source_mappings_table_id = data_source_mappings_table_id
        self._data_spec_name = data_spec_name
        self._use_internal_echo = use_internal_echo

        # Flatten out the bundles options so they're easier to work with
        self._remove_duplicates = self._source_options.remove_duplicates
        self._cache_data_source = self._source_options.cache_data_source

        self._inner_join_on_annotations = False
        self._join_annotations_on_participant = True
        self._annotation_time_rounded_to_second = False
        self._annotation_labels = set()
        self._annotation_tables = set()
        if self._annotation_inner_join_options is not None:
            self._inner_join_on_annotations = True
            self._join_annotations_on_participant = (
                self._annotation_inner_join_options.join_on_participant
            )  # pylint: disable=line-too-long
            self._annotation_time_rounded_to_second = self._annotation_inner_join_options.annotation_time_rounded_to_second  # pylint: disable=line-too-long
            # Copy so we can mutate in this transform, and then pass the
            # original to the FilterByAnnotations transform.
            self._annotation_labels = copy.deepcopy(
                self._annotation_inner_join_options.annotation_labels
            )  # pylint: disable=line-too-long
            self._annotation_tables = copy.deepcopy(
                self._annotation_inner_join_options.annotation_tables
            )  # pylint: disable=line-too-long

        self._is_incremental = False
        if self._incremental_query_options is not None:
            self._is_incremental = True
            # Incremental queries create annotations that we need to inner join
            # on.
            self._inner_join_on_annotations = True

        cache_instance_objects = [
            self._data_point_table_id,
            self._condition,
            self._env,
            self._incremental_query_options,
        ]
        self._instance_key = hash(
            '::'.join([str(obj) for obj in cache_instance_objects])
        )
        self._dedupe_unique_identifer = None
        self._dedupe_column_to_keep_max_value = None
        if use_dedupe_in_query:
            self._dedupe_unique_identifer = 'DataSourceID, DataPointTime'
            self._dedupe_column_to_keep_max_value = 'DataPointWriteTime'

    def get_instance_key(self):
        return self._instance_key

    def get_row_schema(self):
        return self._schema

    def expand_fn(
        self, pcoll
    ) -> Tuple[
        beam.PCollection[schemas.DataPointType],
        beam.PCollection[Tuple[int, types_pb2.DataSource]],
    ]:
        pipeline = pcoll.pipeline
        is_streaming = pipeline.options.view_as(
            beam.options.pipeline_options.StandardOptions
        ).streaming

        if is_streaming:
            raise RuntimeError(
                'DataPointRowSource does not support streaming pipelines.'
            )

        if self._is_incremental:
            # Builds the incremental annotations table & adds the annotation
            # information to the annotation joining options.
            incremental_table_id, is_non_empty = self._build_incremental_table()
            if not is_non_empty:
                logging.warning(
                    'No data was found with the incremental query parameters, '
                    'nothing to do.'
                )
                return (
                    pcoll | 'Create empty data points' >> beam.Create([]),
                    pcoll | 'Create empty data source' >> beam.Create([]),
                )
            self._annotation_labels.add(
                incremental_query.INCREMENTAL_ANNOTATION_LABEL
            )
            self._annotation_tables.add(incremental_table_id)

        data_point_ibis_table = self._get_schema_fetcher().fetch_schema(
            self._data_point_table_id
        )
        annotation_sources = None
        if self._inner_join_on_annotations:
            annotation_sources = self._build_annotation_sources()
            row_filter_builder = (
                build_row_filters.BuildDataPointTableRowFilters(  # pylint: disable=line-too-long
                    self._data_point_table_id,
                    self._condition,
                    data_point_ibis_table,
                    annotation_sources,
                    self._creds,
                    self._billing_project,
                    self._bigquery_location,
                    self._dedupe_unique_identifer,
                    self._dedupe_column_to_keep_max_value,
                )
            )
        else:
            row_filter_builder = (
                build_row_filters.BuildDataPointTableRowFilters(  # pylint: disable=line-too-long
                    self._data_point_table_id,
                    self._condition,
                    data_point_ibis_table,
                    None,
                    self._creds,
                    self._billing_project,
                    self._bigquery_location,
                    self._dedupe_unique_identifer,
                    self._dedupe_column_to_keep_max_value,
                )
            )

        participant_mappings_dict: Union[Dict, beam.pvalue.AsDict] = {}
        if self._participant_table_id is not None:
            participant_mappings_dict = beam.pvalue.AsDict(
                pcoll
                | participant_mappings.BuildParticipantMappings(
                    self._participant_table_id,
                    self._billing_project,
                    self._service_account,
                    self._creds,
                    self._bigquery_location,
                )
            )

        # Internal Echo Parsing Logic
        dp_rows: beam.PCollection[schemas.DataPointType]
        if self._use_internal_echo:
            data_source_cache: beam.PCollection[
                Tuple[int, types_pb2.DataSource]
            ]
            data_source_cache = (
                pcoll
                | f'Build DataSource Cache - {self._data_point_table_id}'
                >> build_data_source_cache.BuildDataSourceCacheFromInternal(
                    bigquery_table_id=self.  # type: ignore
                    _data_source_mappings_table_id,
                    project_id=self._billing_project,
                    service_account=self._service_account,
                    creds=self._creds,
                    bigquery_location=self._bigquery_location,
                    data_spec_to_filter_for=self.  # type: ignore
                    _data_spec_name,
                )
            )

            dp_rows = (
                pcoll
                | f'Reading: {self._data_point_table_id}'
                >> bigquery_source_wrapper.GcpBigquerySourceWrapper(
                    row_filter_builder=row_filter_builder,
                    creds=self._creds,
                    project_id=self._billing_project,
                    service_account=self._service_account,
                )
                | f'Reshuffle table: {self._data_point_table_id}'
                >> beam.Reshuffle()
                | f'Parse table: {self._data_point_table_id}'
                >> beam.ParDo(
                    _ParseDataPointRowDoFn(
                        self._schema, self._use_internal_echo
                    ),
                    participant_mappings_dict,
                ).with_output_types(schemas.DataPointType)
            )

        # TODO(b/231162492): Remove option to source from external echo views
        # once internal Echo is stable.
        if not self._use_internal_echo:
            # External Echo Parsing Logic
            dp_rows_keyed_by_data_source = (
                pcoll
                | f'Reading: {self._data_point_table_id}'
                >> bigquery_source_wrapper.GcpBigquerySourceWrapper(
                    row_filter_builder=row_filter_builder,
                    creds=self._creds,
                    project_id=self._billing_project,
                    service_account=self._service_account,
                )
                | f'Reshuffle table: {self._data_point_table_id}'
                >> beam.Reshuffle()
                | f'Parse table: {self._data_point_table_id}'
                >> beam.ParDo(
                    _ParseDataPointRowDoFn(
                        self._schema, self._use_internal_echo
                    ),
                    participant_mappings_dict,
                ).with_output_types(Tuple[bytes, schemas.DataPointType])
            )

            data_source_cache = (
                dp_rows_keyed_by_data_source
                | f'Build DataSource Cache - {self._data_point_table_id}'
                >> build_data_source_cache.BuildDataSourceCache()
            )

            dp_rows = (
                dp_rows_keyed_by_data_source
                | f'Drop DataSource - {self._data_point_table_id}'
                >> beam.Values()  # pylint: disable=no-value-for-parameter
            )

        if annotation_sources is not None and annotation_sources:
            required_labels = set(self._annotation_labels)
            join_if = options.JoinIf.ANY

            if self._annotation_inner_join_options is not None:
                join_if = self._annotation_inner_join_options.join_if

            annotations = [
                pcoll | f'Querying: {ann.bigquery_table_id}' >> ann
                for ann in annotation_sources
            ] | beam.Flatten()
            dp_rows = (
                (dp_rows, annotations)
                | f'Apply annotation filtering for: {self._data_point_table_id}'
                >> filter_by_annotation.FilterByAnnotations(
                    required_annotation_labels=required_labels,
                    join_if=join_if,
                    join_on_participant=self._join_annotations_on_participant,
                    round_to_second=self._annotation_time_rounded_to_second,
                )
            )

        if self._condition is not None:
            # Apply conditions after joining with annotations. This is needed
            # because conditions.AnnotationCondition cannot be applied until
            # the data has been joined. All other conditions are actually
            # applied in the row_filter when fetching the data from BigQuery.
            dp_rows = (
                dp_rows
                | f'Apply conditions for: {self._data_point_table_id}'
                >> beam.Filter(
                    self._condition.data_point_row_condition,
                    data_source_cache=beam.pvalue.AsSingleton(
                        data_source_cache
                    ),
                )
            )

        if self._remove_duplicates and (
            self._dedupe_unique_identifer is None
            or self._dedupe_column_to_keep_max_value is None
        ):
            dp_rows = (
                dp_rows
                | f'Remove duplicates table: {self._data_point_table_id}'
                >> echo_dedupe.RemoveEchoDupes()
            )

        return dp_rows, data_source_cache

    def _build_incremental_table(self) -> Tuple[str, bool]:
        """Builds an incremental table modeled as Annotations."""
        dataset_name = echo_utils.get_temp_bigquery_dataset_for_location(
            self._bigquery_location
        )
        table_name = self._data_point_table_id.split('.')[-1]
        incremental_table_id = f'{self._billing_project}.{dataset_name}.incremental-{table_name}-{str(uuid.uuid4())}'  # pylint: disable=line-too-long

        raw_table = self._get_schema_fetcher().fetch_schema(
            self._data_point_table_id
        )

        if self._participant_table_id is None:
            raise ValueError('participant table required for incremental runs.')
        participant_mappings_table = self._get_schema_fetcher().fetch_schema(
            self._participant_table_id
        )

        creds, _ = self._creds.get_credentials()
        qr = query_runner.QueryRunner(
            self._billing_project, creds, self._bigquery_location
        )
        is_non_empty = incremental_query.create_incremental_table(
            incremental_table_id,
            self._incremental_query_options,  # type: ignore
            raw_table,
            participant_mappings_table,
            qr,
            self._use_internal_echo,
        )
        return incremental_table_id, is_non_empty

    def _build_annotation_sources(
        self,
    ) -> List[annotation_source.AnnotationRowSource]:
        """Builds Annotation sources used for inner joining on DataPoints."""
        annotation_cond = None
        for label in self._annotation_labels:
            if annotation_cond is None:
                annotation_cond = conditions.AnnotationCondition(label)
            else:
                annotation_cond |= conditions.AnnotationCondition(label)

        if self._condition is not None:
            if annotation_cond is None:
                annotation_cond = self._condition  # type: ignore
            else:
                annotation_cond &= self._condition

        annotations = []
        for table_id in self._annotation_tables:
            ann_source = annotation_source.AnnotationRowSource(
                bigquery_table_id=table_id,
                participant_table_id=self._participant_table_id,
                source_options=self._source_options,
                condition=annotation_cond,
                creds=self._creds,
                env=self._env,
                billing_project=self._billing_project,
                service_account=self._service_account,
                bigquery_location=self._bigquery_location,
            )
            annotations.append(ann_source)
        return annotations

    def _get_schema_fetcher(self) -> schema_fetcher.SchemaFetcher:
        """Returns a schema fetcher to fetch BigQuery schemas."""
        creds, _ = self._creds.get_credentials()
        return schema_fetcher.SchemaFetcher(
            self._billing_project, creds, self._bigquery_location
        )
