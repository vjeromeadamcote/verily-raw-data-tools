"""BigQuery Source implementation for Annotations (Echo & Custom)."""

import copy
import logging
from typing import Any, Dict, Iterable, List, Optional

import apache_beam as beam
from google.cloud import bigquery  # type: ignore
import ibis
import ibis_bigquery

from verily.ds_sdk.core import conditions
from verily.ds_sdk.core import options
from verily.ds_sdk.core import schema_fetcher
from verily.ds_sdk.core import schemas
from verily.ds_sdk.core.gcp import bigquery_source_wrapper
from verily.ds_sdk.core.gcp import credentials
from verily.ds_sdk.core.io import cacheable_source
from verily.ds_sdk.core.io.bigquery import build_row_filters
from verily.ds_sdk.core.io.bigquery import participant_mappings
from verily.ds_sdk.core.utils import timestamps
from verily.ds_sdk.protos import management_resources_pb2

_MATERIALIZE_QUERY = """
SELECT
  participants.ParticipantId AS participant_id,
  participants.ParticipantNamespace AS participant_namespace,
  ans.device_id,
  ans.start_timestamp_utc,
  ans.end_timestamp_utc,
  ans.annotation_label
FROM
  `{annotation_table}` ans
LEFT JOIN
  `{participant_table}` participants
ON
  ans.device_id = participants.DeviceId
  AND ans.start_timestamp_utc BETWEEN participants.StartTime
  AND participants.EndTime
"""


def _parse_annotation_row(bigquery_row, participant_info) -> schemas.Annotation:
    participant_namespace = bigquery_row.get('participant_namespace', None)
    if participant_namespace is not None:
        participant_namespace = management_resources_pb2.Participant.ParticipantNamespace.Value(  # pylint: disable=line-too-long
            participant_namespace)
    participant_id = bigquery_row.get('participant_id', None)

    device_id = bigquery_row.get('device_id')
    start_timestamp = timestamps.parse_bigquery_timestamp(
        bigquery_row.get('start_timestamp_utc'))
    end_timestamp = timestamps.parse_bigquery_timestamp(
        bigquery_row.get('end_timestamp_utc'))

    if participant_id is not None and participant_info is not None:
        if participant_info.participant_id != participant_id:
            raise ValueError(
                f'Annotation participant info doesn\'t match for {device_id}.')
    if participant_namespace is not None and participant_info is not None:
        if participant_info.participant_namespace != participant_namespace:
            raise ValueError(
                f'Annotation participant info doesn\'t match for {device_id}.')

    participant_id = (None if participant_info is None else
                      participant_info.participant_id)
    participant_namespace = (None if participant_info is None else
                             participant_info.participant_namespace)

    annotation_metadata = schemas.AnnotationMetadata(
        # This field is not optional, so we cannot default to None.
        device_id=device_id,
        participant_id=participant_id,
        participant_namespace=participant_namespace,
        version_name=bigquery_row.get('version_name', None),
        version_number=bigquery_row.get('version_number', None),
        input_data_info=[])

    return schemas.Annotation(
        # These fields are not optional, so we cannot default to None.
        annotation_label=bigquery_row.get('annotation_label'),
        start_timestamp_utc=start_timestamp,
        end_timestamp_utc=end_timestamp,
        annotation_metadata=annotation_metadata)


class _ParseBigQueryAnnotationRow(beam.DoFn):
    """Parses a BigQuery row returned from an annotations table."""

    def process(  # type: ignore[override]
        self, bigquery_row: Dict[str, Any], participant_mappings_dict: Dict[
            str, List[participant_mappings.ParticipantInfo]]
    ) -> Iterable[schemas.Annotation]:

        device_id = bigquery_row.get('device_id')
        start_timestamp = timestamps.parse_bigquery_timestamp(
            bigquery_row.get('start_timestamp_utc'))
        end_timestamp = timestamps.parse_bigquery_timestamp(
            bigquery_row.get('end_timestamp_utc'))

        participant_less_counter = beam.metrics.Metrics.counter(
            'annotations_source', 'points_with_no_participant_association')

        participant_info = None
        if device_id in participant_mappings_dict:
            device_participant_associations = participant_mappings_dict[
                device_id]

            participant_info = participant_mappings.get_participant_info_within_time_range(  # pylint: disable=line-too-long
                device_id, start_timestamp, end_timestamp,
                device_participant_associations)

        if participant_info is None:
            logging.warning('No participant info found for annotation: %s',
                            bigquery_row)
            participant_less_counter.inc()
            yield _parse_annotation_row(bigquery_row, None)

        else:
            for participant in participant_info:
                bq_row = copy.deepcopy(bigquery_row)
                bq_row['start_timestamp_utc'] = (
                    timestamps.beam_timestamp_to_pandas_timestamp(
                        max(start_timestamp, participant.start_timestamp)))
                bq_row['end_timestamp_utc'] = (
                    timestamps.beam_timestamp_to_pandas_timestamp(
                        min(end_timestamp, participant.end_timestamp)))

                yield _parse_annotation_row(bq_row, participant)


class AnnotationRowSource(cacheable_source.CacheablePTransform):
    """Reads from BigQuery and parses rows into schema-aware PCollections."""

    def __init__(
        self,
        *,
        bigquery_table_id: str,
        participant_table_id: Optional[str],
        source_options: options.BatchSourceOptions,
        condition: Optional[conditions.Condition],
        creds: credentials.DsSdkCredentials,
        env: str,
        billing_project: str,
        service_account: str,
        bigquery_location: str,
    ):
        """Creates a AnnotationRowSource PTransform.

    Args:
      bigquery_table_id: The full BigQuery table name in the format:
        `project.dataset.table`.
      participant_table_id: The table containing the participant id mappings in
        the form: `project.dataset.table`.
      registry: The registry (study) used to fetch credentials.
      source_options: The required options for building DS SDK sources.
      condition: Conditions to apply to the BigQuery table.
      creds: Credentials object used to generate user/project credentials.
      env: The environment to run in. Options are: qa, preprod, prod, &
        prod-batch.
      billing_project: The GCP project to bill resource usage to.
      service_account: The service account to auth with BigQuery. NOTE: This is
        only used on google3 / Flume.
      bigquery_location: The location to create the BigQuery client in.
    """
        super().__init__(disable_cache=source_options.disable_cache)

        self.bigquery_table_id = bigquery_table_id
        self._participant_table_id = participant_table_id
        self._source_options = source_options
        self._condition = condition
        self._creds = creds
        self._env = env
        self._billing_project = billing_project
        self._service_account = service_account
        self._bigquery_location = bigquery_location

        self._instance_key = hash(f'{bigquery_table_id}::{condition}::{env}')

    def get_raw_annotations(self) -> Iterable[schemas.Annotation]:
        """Returns the raw annotations w/o using beam.

    NOTE: This should only be used by flume pipelines for computing the row
    filter and will be removed once migrating to Dataflow.
    """
        annotation_ibis_table = self._get_schema_fetcher().fetch_schema(
            self.bigquery_table_id)

        creds, _ = self._creds.get_credentials()
        bq_client = bigquery.Client(credentials=creds,
                                    project=self._billing_project,
                                    location=self._bigquery_location)

        where_clause = _build_where_clause_from_condition(
            self._condition, annotation_ibis_table)
        query = _MATERIALIZE_QUERY.format(
            annotation_table=self.bigquery_table_id,
            participant_table=self._participant_table_id)
        if where_clause is not None:
            query += f' WHERE {where_clause}'

        logging.warning(
            'Executing annotations query to build data points filters.')
        rows = bq_client.query(query)

        annotations = []
        for row in rows:
            start_timestamp = timestamps.parse_bigquery_timestamp(
                row['start_timestamp_utc'])
            end_timestamp = timestamps.parse_bigquery_timestamp(
                row['end_timestamp_utc'])
            annotations.append(
                schemas.Annotation(
                    annotation_label=row['annotation_label'],
                    start_timestamp_utc=start_timestamp,
                    end_timestamp_utc=end_timestamp,
                    annotation_metadata=schemas.AnnotationMetadata(
                        device_id=row['device_id'],
                        participant_id=row['participant_id'],
                        participant_namespace=row['participant_namespace'],
                        version_name=None,
                        version_number=None,
                        input_data_info=[])))

        return annotations

    def get_instance_key(self):
        return self._instance_key

    def get_row_schema(self):
        return schemas.Annotation

    def expand_fn(self, pcoll):
        pipeline = pcoll.pipeline
        is_streaming = (pipeline.options.view_as(
            beam.options.pipeline_options.StandardOptions).streaming)

        if is_streaming:
            raise RuntimeError(
                'AnnotationRowSource does not support streaming pipelines.')

        annotation_ibis_table = self._get_schema_fetcher().fetch_schema(
            self.bigquery_table_id)
        row_filter_builder = build_row_filters.BuildAnnotationTableRowFilters(
            self.bigquery_table_id, self._condition, annotation_ibis_table,
            self._creds, self._billing_project, self._bigquery_location)

        participant_mappings_dict = {}
        if self._participant_table_id is not None:
            participant_mappings_dict = beam.pvalue.AsDict(
                pcoll | participant_mappings.BuildParticipantMappings(
                    self._participant_table_id, self._billing_project, self.
                    _service_account, self._creds, self._bigquery_location))

        annotation_rows = (
            pcoll | f'Reading: {self.bigquery_table_id}' >>
            bigquery_source_wrapper.GcpBigquerySourceWrapper(
                row_filter_builder=row_filter_builder,
                creds=self._creds,
                project_id=self._billing_project,
                service_account=self._service_account) |
            f'Reshuffle table: {self.bigquery_table_id}' >> beam.Reshuffle() |
            f'Parse table: {self.bigquery_table_id}' >> beam.ParDo(
                _ParseBigQueryAnnotationRow(),
                participant_mappings_dict=participant_mappings_dict))

        return annotation_rows

    def _get_schema_fetcher(self) -> schema_fetcher.SchemaFetcher:
        """Returns a schema fetcher to fetch BigQuery schemas."""
        creds, _ = self._creds.get_credentials()
        return schema_fetcher.SchemaFetcher(self._billing_project, creds,
                                            self._bigquery_location)


def _build_where_clause_from_condition(
        condition: Optional[conditions.Condition],
        ibis_table: ibis.expr.types.TableExpr) -> Optional[str]:
    row_filter = None
    if condition is not None:
        cond = condition.annotations_condition(
            ibis_table, include_annotation_conditions=True)
        if cond is not None:
            query = ibis_bigquery.compile(ibis_table[cond])
            # The first index will be the actual condition we want to filter on.
            row_filter = query.split('WHERE', 1)[1]
    return row_filter
