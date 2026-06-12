"""Wrapper around the BigQuery source for GCP usage.

This wrapper is used to ensure google3 and GCP have the same interface for
calling the BigQuery source. This makes it easier to maintain a copy in
google3 and GitHub.
"""

import logging
from typing import Any, Optional, Tuple

import apache_beam as beam
from apache_beam.io.gcp import bigquery_tools
from apache_beam.io.iobase import SourceBundle
from apache_beam.options.pipeline_options import PipelineOptions
import google.cloud.bigquery_storage_v1 as bq_storage


class GcpBigquerySourceWrapper(beam.PTransform):
    """Wrapper PTransform for calling BigQuery source from GCP / Dataflow."""

    def __init__(
        self,
        *,
        # TODO(tanke): Refactor imports to allow us to set proper type
        # annotations
        row_filter_builder: Any,  # PTransforms defined in build_row_filters.py
        creds: Any,  # DsSdkCredentials defined in credentials.py
        project_id: str,
        service_account: str,
    ):
        super().__init__()
        self.row_filter_builder = row_filter_builder
        self.creds = creds

        # Project and service_account are not needed for the GCP source.
        del project_id, service_account

    def expand(self, pcol):
        table, row_filter = self.row_filter_builder.materialize(
        )  # type:Tuple[str, Optional[str]]
        table = table.replace('.', ':', 1)
        return (
            pcol |
            f'Querying table: {table} row_filter: {row_filter}' >> beam.io.Read(
                # Beam doesn't expose the row_restriction as part of the
                # public API so we have to use the internal source...
                _CustomBigQueryStorageSource(
                    method=beam.io.gcp.bigquery.ReadFromBigQuery.Method.
                    DIRECT_READ,
                    table=table,
                    row_restriction=row_filter,
                    pipeline_options=PipelineOptions())))


class _CustomBigQueryStorageSource(
        beam.io.gcp.bigquery._CustomBigQueryStorageSource):  # pylint: disable=protected-access
    """Custom fork of beam.io.gcp.bigquery._CustomBigQueryStorageSource.

    This is an exact copy other than the create_read_session timeout.

    The main reason to fork this is to allow us to specify a timeout when
    creating a read session. If this works well we should consider exposing this
    in apache beam directly.
    """

    def split(
        self,
        desired_bundle_size,
        start_position=None,  # pylint: disable=unused-argument
        stop_position=None):  # pylint: disable=unused-argument
        if self.split_result is None:  # pylint: disable=access-member-before-definition
            bq = bigquery_tools.BigQueryWrapper(
                temp_table_ref=(self.temp_table if self.temp_table else None))

            if self.query is not None:
                self._setup_temporary_dataset(bq)
                self.table_reference = self._execute_query(bq)

            requested_session = bq_storage.types.ReadSession()
            requested_session.table = 'projects/{}/datasets/{}/tables/{}'.format(  # pylint: disable=line-too-long,consider-using-f-string
                self.table_reference.projectId, self.table_reference.datasetId,
                self.table_reference.tableId)

            if self.use_native_datetime:
                requested_session.data_format = bq_storage.types.DataFormat.ARROW  # pylint: disable=line-too-long
                requested_session.read_options\
                  .arrow_serialization_options.buffer_compression = \
                  bq_storage.types.ArrowSerializationOptions.CompressionCodec.LZ4_FRAME  # pylint: disable=line-too-long
            else:
                requested_session.data_format = bq_storage.types.DataFormat.AVRO

            if self.selected_fields is not None:
                requested_session.read_options.selected_fields = self.selected_fields  # pylint: disable=line-too-long
            if self.row_restriction is not None:
                requested_session.read_options.row_restriction = self.row_restriction  # pylint: disable=line-too-long

            storage_client = bq_storage.BigQueryReadClient()
            stream_count = 0
            if desired_bundle_size > 0:
                table_size = self._get_table_size(bq, self.table_reference)
                stream_count = min(int(table_size / desired_bundle_size),
                                   _CustomBigQueryStorageSource.MAX_SPLIT_COUNT)
            stream_count = max(stream_count,
                               _CustomBigQueryStorageSource.MIN_SPLIT_COUNT)

            parent = 'projects/{}'.format(self.table_reference.projectId)  # pylint: disable=consider-using-f-string
            read_session = storage_client.create_read_session(
                parent=parent,
                read_session=requested_session,
                max_stream_count=stream_count,
                timeout=60 * 60)  # Set timeout to one hour for large studies
            logging.info(
                'Sent BigQuery Storage API CreateReadSession request: \n %s \n'
                'Received response \n %s.', requested_session, read_session)

            self.split_result = [
                beam.io.gcp.bigquery._CustomBigQueryStorageStreamSource(  # pylint: disable=protected-access
                    stream.name, self.use_native_datetime)
                for stream in read_session.streams
            ]

        for source in self.split_result:
            yield SourceBundle(weight=1.0,
                               source=source,
                               start_position=None,
                               stop_position=None)
