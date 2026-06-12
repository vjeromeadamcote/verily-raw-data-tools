"""Main SensorsIO for reading and writing sensor data."""

import logging
from typing import Dict, List, Optional, Type, Union
import uuid

import apache_beam as beam
from apache_beam.runners.interactive import interactive_runner
import apache_beam.runners.interactive.interactive_beam as ib
from google.cloud import bigquery  # type: ignore
import pandas as pd

from verily.ds_sdk.core import conditions
from verily.ds_sdk.core import data_filters
from verily.ds_sdk.core import options
from verily.ds_sdk.core import schemas
from verily.ds_sdk.core import sensorsuite
from verily.ds_sdk.core import studies
from verily.ds_sdk.core.docker import worker_image
from verily.ds_sdk.core.gcp import credentials
from verily.ds_sdk.core.io import sensor_store
from verily.ds_sdk.core.io.bigquery import annotation_source
from verily.ds_sdk.core.io.bigquery import annotations_sink
from verily.ds_sdk.core.io.bigquery import build_data_source_cache
from verily.ds_sdk.core.io.bigquery import data_points_sink
from verily.ds_sdk.core.io.bigquery import data_points_source
from verily.ds_sdk.core.io.bigquery import mock_source
from verily.ds_sdk.core.io.data_source_cache import DataSourceCache
from verily.ds_sdk.core.io.pubsub import sensor_store_source
from verily.ds_sdk.core.transforms import PrintPcol
from verily.ds_sdk.core.utils import data_specs as data_specs_util
from verily.ds_sdk.core.utils import dataflow_utils
from verily.ds_sdk.core.utils import runner_utils

_SDK_TEMP_BUCKET_SUFFIX = '-ds-sdk-temp'

# Set log level so users can see relevent information.
logging.getLogger().setLevel(logging.INFO)


class _DataSourceCacheStateHandler:
    """Manages the state of the DataSource cache."""

    def __init__(self):
        # Batch run attributes
        self._pcolls = []
        self._pvalue_needs_refresh = True
        self._pvalue = None
        # Streaming run attributes
        self._redis_endpoint = None  # host:port

    def get_streaming_cache(self):
        """Returns a DataSourceCache reference backed by Redis."""
        if self._redis_endpoint is None:
            raise ValueError(
                'Streaming DataSource Cache was never instantiated!'
            )
        return DataSourceCache({}, self._redis_endpoint)

    def get_batch_cache(self) -> DataSourceCache:
        """Flattens the pcolls into a single pvalue / Dict side input."""
        # If the cache hasn't been modified, we can avoid creating another
        # pvalue.
        if self._pvalue_needs_refresh:
            if len(self._pcolls) == 0:
                raise ValueError(
                    'Batch DataSource Cache was never instantiated!'
                )
            elif len(self._pcolls) == 1:
                pcol = self._pcolls[0]
            else:
                pcol = (
                    self._pcolls
                    | f'Flatten DataSources {uuid.uuid4().hex}'
                    >> beam.Flatten()
                    | f'Merge Caches {uuid.uuid4().hex}'
                    >> build_data_source_cache.MergeDataSourceCaches()
                )

            self._pvalue = beam.pvalue.AsSingleton(pcol)
            self._pvalue_needs_refresh = False
        return self._pvalue

    def add_pcoll(self, pcoll: beam.PCollection) -> None:
        """Adds a pcoll to the cache and signals a need for a cache refresh."""
        self._pcolls.append(pcoll)
        self._pvalue_needs_refresh = True

    def set_redis_connection_info(self, redis_endpoint: str):
        """Updates the redis cache connection info."""
        if not self._redis_endpoint:
            self._redis_endpoint = redis_endpoint
        elif self._redis_endpoint != redis_endpoint:
            raise RuntimeError(
                'Streaming DataSource Cache Handler only supports a single '
                f'redis connection. Got {self._redis_endpoint} & '
                f'{redis_endpoint}'
            )


class SensorsIO:
    """Main object for reading and writing sensor data with Beam."""

    def __init__(
        self,
        *,
        registry: str,
        runner: Union[str, beam.runners.PipelineRunner],
        env: str,
        gcp_project: Optional[str] = None,
        service_account: Optional[str] = None,
        temp_gcs_bucket: Optional[str] = None,
        dataflow_options: Optional[options.DataflowOptions] = None,
        streaming: bool = False,
        bigquery_location: str = 'US',
        study_info: Optional[studies.StudyInfo] = None,
    ):
        self._registry = registry
        self._env = env
        self._streaming = streaming
        self._bigquery_location = bigquery_location
        self._incremental_options = None

        self._runner = runner
        self._is_interactive_runner = False
        if runner_utils.is_interactive_runner(runner):
            self._runner = interactive_runner.InteractiveRunner()
            self._is_interactive_runner = True

        pipeline_options = {}
        self.is_dataflow_job = False
        if runner_utils.is_dataflow_runner(runner):
            self.is_dataflow_job = True
            if dataflow_options is None:
                raise ValueError(
                    'If running your job on dataflow you must provide '
                    '`dataflow_options`'
                )

            if not dataflow_options.job_name:
                dataflow_options.job_name = (
                    f'ds_sdk_dataflow_{str(uuid.uuid4())}'
                )

            pipeline_options = dataflow_options.to_pipeline_options()
            if 'project' in pipeline_options:
                raise ValueError(
                    '`project` was present in '
                    'DataflowOptions.additional_options, project can be passed '
                    'directly to the SensorsIO object using: `gcp_project`.'
                )
            if 'service_account_email' in pipeline_options:
                raise ValueError(
                    '`service_account_email` was present in '
                    'DataflowOptions.additional_options, service_account_email '
                    'can be passed directly to the SensorsIO object using: '
                    '`service_account`.'
                )
            if 'temp_location' in pipeline_options:
                raise ValueError(
                    '`temp_location` was present in '
                    'DataflowOptions.additional_options, temp_location can be '
                    'passed directly to the SensorsIO object using: '
                    '`temp_gcs_bucket`.'
                )
            if 'streaming' in pipeline_options:
                raise ValueError(
                    '`streaming` was present in '
                    'DataflowOptions.additional_options, streaming can be '
                    'passed directly to the SensorsIO object using: '
                    '`streaming`.'
                )
            if worker_image.should_create_worker_image():
                pipeline_options['sdk_container_image'] = (
                    worker_image.build_docker_image(dataflow_options.job_name)
                )
        self.dataflow_options = dataflow_options

        if study_info is None:
            study_info = studies.get_study_info(registry, self._env)

        if study_info.gcp_project is None:
            raise ValueError(
                'Unable to determine gcp_project for registry: '
                f'{registry} and env: {self._env}'
            )

        self._study_info = study_info

        # Fetch values from the registry's study config for unset optional
        # values that are required by Beam.
        if gcp_project is None:
            self._gcp_project = study_info.gcp_project
        else:
            self._gcp_project = gcp_project

        if service_account is None:
            full_service_account = study_info.cloud_service_account
            if full_service_account is None:
                raise ValueError(
                    'Unable to determine service account for registry: '
                    f'{registry} and env: {self._env}'
                )
            self._service_account = full_service_account.replace(
                'projects/-/serviceAccounts/', ''
            )
        else:
            self._service_account = service_account

        if temp_gcs_bucket is None:
            gcp_project = study_info.gcp_project
            self._temp_gcs_bucket = (
                f'gs://{gcp_project}{_SDK_TEMP_BUCKET_SUFFIX}'
            )
        else:
            self._temp_gcs_bucket = temp_gcs_bucket
        if self._temp_gcs_bucket.endswith('/'):
            self._temp_gcs_bucket = self._temp_gcs_bucket[:-1]
        self._creds = credentials.DsSdkCredentials(
            runner, self._service_account, self._gcp_project
        )

        pipeline_options.update(
            {
                'project': self._gcp_project,
                'temp_location': self._temp_gcs_bucket + '/dataflow_temp',
                'service_account_email': self._service_account,
                'streaming': self._streaming,
            }
        )

        if not self.is_dataflow_job:
            self.pipeline_options = (
                beam.options.pipeline_options.PipelineOptions(
                    **pipeline_options
                )
            )
        else:
            self.pipeline_options = (
                beam.options.pipeline_options.GoogleCloudOptions(
                    **pipeline_options
                )
            )

        self._p = beam.Pipeline(
            runner=self._runner, options=self.pipeline_options
        )
        self._data_source_cache_handler = _DataSourceCacheStateHandler()

    def run(self, wait_until_finish: bool = True):
        result = self._p.run()
        if wait_until_finish:
            result.wait_until_finish()
        return result

    def echo_data_point_rows(
        self,
        *,
        data_spec_name: str,
        source_options: options.BatchSourceOptions,
        condition: Optional[conditions.Condition],
        annotation_inner_join_options: Optional[
            options.AnnotationInnerJoinOptions
        ],
        incremental_query_options: Optional[options.IncrementalQueryOptions],
        data_filter_list: Optional[List[data_filters.DataFilter]] = None,
        use_dedupe_in_query: bool = False,
    ):
        """Returns rows for a given data spec in Echo.

        Args:
        data_spec_name: The data spec that is being queried
            (e.g. com.verily.imu).
        source_options: Options for building the Echo source.
        condition: The condition to apply to data being read in.
        annotation_inner_join_options: Options for joining data points with
            annotations.
        incremental_query_options: Options for reading data based on when it was
            written (incrementally).
        use_dedupe_in_query: whether to use BigQuery query to dedupe. If False,
            the dedupe will be done after the query with Beam workers.
        """
        if self._streaming:
            raise ValueError(
                'echo_data_point_rows can only be used in batch mode.'
            )
        if incremental_query_options is not None:
            if (
                self._incremental_options is not None
                and incremental_query_options != self._incremental_options
            ):
                raise ValueError(
                    'Same incremental options must be used for every query to '
                    'Echo.'
                )

            if incremental_query_options.state_file_path is not None:
                incremental_query_options.update_from_state_file(
                    self._registry, self._creds, self._gcp_project
                )
            # Only run this before self._incremental_options is set. That
            # ensures we're not setting duplicate labels.
            if self.is_dataflow_job and self._incremental_options is None:
                incremental_query_labels = []
                # TODO make sure all these are formatted correctly
                if incremental_query_options.write_start_time is not None:
                    safe_start_label = dataflow_utils.escape_dataflow_job_labels(  # pylint: disable=line-too-long
                        incremental_query_options.write_start_time.isoformat()
                    )
                    incremental_query_labels.append(
                        f'write_start_time={safe_start_label}'
                    )
                if incremental_query_options.write_end_time is not None:
                    safe_end_label = dataflow_utils.escape_dataflow_job_labels(
                        incremental_query_options.write_end_time.isoformat()
                    )
                    incremental_query_labels.append(
                        f'write_end_time={safe_end_label}'
                    )
                safe_query_mode = dataflow_utils.escape_dataflow_job_labels(
                    incremental_query_options.incremental_query_mode.name
                )
                incremental_query_labels.append(
                    f'incremental_query_mode={safe_query_mode}'
                )
                labels = (
                    []
                    if self.pipeline_options.labels is None
                    else self.pipeline_options.labels
                )
                self.pipeline_options.labels = labels + incremental_query_labels
            self._incremental_options = incremental_query_options  # type: ignore  # pylint: disable=line-too-long

        row_schema = schemas.DATA_SPEC_NAME_TO_SCHEMA_CLASS[data_spec_name]

        escaped_data_spec_name = data_specs_util.to_echo_name(data_spec_name)
        if self._study_info.gcp_project is None:
            raise ValueError(
                'Unable to determine gcp_project for study: '
                f'{self._study_info}'
            )
        if self._study_info.internal_echo_dataset is None:
            raise ValueError(
                'Unable to determine internal_echo_dataset for study: '
                f'{self._study_info}'
            )

        data_point_table_id = f'{self._study_info.gcp_project}.{self._study_info.internal_echo_dataset}.{escaped_data_spec_name}'  # pylint: disable=line-too-long
        data_source_mappings_table_id = f'{self._study_info.gcp_project}.{self._study_info.internal_echo_dataset}.data_source_mappings'  # pylint: disable=line-too-long
        participant_table_id = None
        if source_options.join_on_participant:
            participant_table_id = studies.get_participant_table_for_study(
                self._study_info
            )

        source = None
        if source_options.use_mock_source:
            source = mock_source.MockRowSource(
                row_schema=row_schema, condition=condition, for_annotation=False
            )
        else:
            source = data_points_source.DataPointRowSource(
                data_point_table_id=data_point_table_id,
                participant_table_id=participant_table_id,
                schema=row_schema,
                condition=condition,
                source_options=source_options,
                annotation_inner_join_options=annotation_inner_join_options,
                incremental_query_options=incremental_query_options,
                creds=self._creds,
                env=self._env,
                billing_project=self._gcp_project,
                service_account=self._service_account,
                bigquery_location=self._bigquery_location,
                data_source_mappings_table_id=data_source_mappings_table_id,
                data_spec_name=data_spec_name,
                use_internal_echo=True,
                use_dedupe_in_query=use_dedupe_in_query,
            )

        echo_rows, data_source_cache = (
            self._p  #
            | f'Fetch Rows - {data_spec_name}' >> source
        )  # type: ignore[operator]
        self._data_source_cache_handler.add_pcoll(data_source_cache)

        if data_filter_list is not None and len(data_filter_list) > 0:
            data_filter_do_fn = data_filters.MergedDataFilters(
                data_filter_list=data_filter_list
            )

            echo_rows = (
                echo_rows
                | f'Apply data filters to {data_spec_name}'
                >> beam.ParDo(
                    data_filter_do_fn,
                    data_source_cache=self.get_data_source_dict(),
                ).with_output_types(row_schema)
            )

        return echo_rows

    def custom_data_point_rows(
        self,
        *,
        data_point_table_id: str,
        row_schema: Type[schemas.DataPointType],
        source_options: options.BatchSourceOptions,
        condition: Optional[conditions.Condition],
        annotation_inner_join_options: Optional[
            options.AnnotationInnerJoinOptions
        ],
    ):
        """Returns rows from a BigQuery table that written by the SDK BQ sink.

        Args:
        data_point_table_id: The bigquery table containing the custom
            DataPoints.
        row_schema: The NamedTuple row schema that the data corresponds to.
        source_options: Options for building the DataPoints source.
        condition: The condition to apply to data being read in.
        annotation_inner_join_options: Options for joining data points with
            annotations.
        """
        if self._streaming:
            raise ValueError(
                'custom_data_point_rows can only be used in batch mode.'
            )
        participant_table_id = None
        if source_options.join_on_participant:
            participant_table_id = studies.get_participant_table_for_study(
                self._study_info
            )
        if source_options.remove_duplicates:
            logging.warning(
                '`custom_data_point_rows` does not support removing duplicates.'
                'BatchSourceOptions.remove_duplicates has been overridden to '
                'False.'
            )
            source_options.remove_duplicates = False

        source = None
        if source_options.use_mock_source:
            source = mock_source.MockRowSource(
                row_schema=row_schema, condition=condition, for_annotation=False
            )
        else:
            source = data_points_source.DataPointRowSource(
                data_point_table_id=data_point_table_id,
                participant_table_id=participant_table_id,
                schema=row_schema,
                condition=condition,
                source_options=source_options,
                annotation_inner_join_options=annotation_inner_join_options,
                incremental_query_options=None,
                creds=self._creds,
                env=self._env,
                billing_project=self._gcp_project,
                service_account=self._service_account,
                bigquery_location=self._bigquery_location,
                use_internal_echo=False,
            )

        data_point_rows, data_source_cache = (
            self._p
            | f'Fetch Rows - {data_point_table_id}'  # type: ignore[operator]
            >> source
        )
        self._data_source_cache_handler.add_pcoll(data_source_cache)

        return data_point_rows

    def sensor_store_streaming_rows(
        self,
        *,
        data_spec_names: List[str],
        streaming_options: options.StreamingSourceOptions,
        condition: Optional[conditions.Condition],
        pubsub_message_window_into: beam.WindowInto,
        api_key: str,
        request_retry_timeout: pd.Timedelta = pd.Timedelta('1h'),
    ) -> Dict[str, beam.PCollection[schemas.DataPointType]]:
        """Returns rows from a sensor store pub/sub topic.

        Args:
        data_spec_names: The data specs to read from SensorStore.
        streaming_options: Options for building the streaming SensorStore
            source.
        condition: The condition to apply to data being read in.
        pubsub_message_window_into: The WindowInto to apply to PubSub requests
            before sending the SensorStore requests. This is used to group
            concurrent upload notifications into a single read request / Beam
            window.
        api_key: The API key that is attached to the requests sent to
            SensorStore.
            NOTE: This API key must be in the same project as the ServiceAccount
            you are using.
        request_retry_timeout: How long we should retry retryable errors before
            giving up. Defaults to one hour.
        """
        data_point_rows = (
            self._p
            | f'Streaming SensorStore Source: {data_spec_names}'
            >> sensor_store_source.StreamingSensorStoreSource(
                data_spec_names=data_spec_names,
                source_options=streaming_options,
                pubsub_message_window_into=pubsub_message_window_into,
                condition=condition,
                registry=self._registry,
                creds=self._creds,
                env=self._env,
                api_key=api_key,
                request_retry_timeout=request_retry_timeout,
            )
        )
        if streaming_options.redis_endpoint is None:
            raise ValueError(
                '`streaming_options.redis_endpoint` must be provided for '
                'streaming pipelines'
            )
        self._data_source_cache_handler.set_redis_connection_info(
            streaming_options.redis_endpoint
        )

        return data_point_rows

    def annotation_rows(
        self,
        *,
        bigquery_table: str,
        source_options: options.BatchSourceOptions,
        condition: Optional[conditions.Condition],
    ):
        """Returns annotations stored in Echo / Custom Annotations tables.

        Args:
        bigquery_table: The custom BigQuery table to read the annotations from
            in the form: `project.dataset.table`.
        source_options: Options for building the Annotation source.
        condition: The condition to apply to data being read in.
        """
        if self._streaming:
            raise ValueError('annotation_rows can only be used in batch mode.')

        if not source_options.join_on_participant:
            logging.warning(
                'annotation_rows requires joining on participants.'
                'BatchSourceOptions.join_on_participants has been overridden '
                'to True.'
            )
            source_options.join_on_participant = True
        participant_table_id = studies.get_participant_table_for_study(
            self._study_info
        )

        source = None
        if source_options.use_mock_source:
            source = mock_source.MockRowSource(
                row_schema=schemas.Annotation,
                condition=condition,
                for_annotation=True,
            )
        else:
            source = annotation_source.AnnotationRowSource(
                bigquery_table_id=bigquery_table,
                participant_table_id=participant_table_id,
                source_options=source_options,
                condition=condition,
                creds=self._creds,
                env=self._env,
                billing_project=self._gcp_project,
                service_account=self._service_account,
                bigquery_location=self._bigquery_location,
            )

        annotation_rows = (
            self._p | 'Fetch Echo Annotation Rows - '
            f'{bigquery_table}'  # type: ignore[operator]
             >> source
        )

        return annotation_rows

    def write_to_sensor_store(
        self,
        schema: Type[schemas.DataPointType],
        algorithm_name: str,
        algorithm_version: str,
        overwrite_key_generator: sensorsuite.OverwriteKeyGeneratorType,
        api_key: str,
        global_qps_limit: int = 200,
        request_retry_timeout: pd.Timedelta = pd.Timedelta('1h'),
    ) -> beam.PTransform:
        """Returns a PTransform that can be used to write data to SensorStore.

        Args:
        schema: The schema that is being written to SensorStore, this schema
            must be annotated with the @dataspec annotation.
        algorithm_name: The name of the algorithm that generated the data point.
        algorithm_version: The version of the algorithm that generated the data
            point.
        overwrite_key_generator: A subclass of OverwriteKeyGenerator. This is
            the function that will be used to generate an overwrite key for each
            element.
        api_key: The API key that is attached to the requests sent to
            SensorStore.
            NOTE: This API key must be in the same project as the ServiceAccount
            you are using.
        global_qps_limit: The max QPS that should be sent to SensorStore across
            all workers. Defaults to 200.
        request_retry_timeout: How long we should retry retryable errors before
            giving up. Defaults to one hour.
        """
        dataflow_job_name = (
            self.dataflow_options.job_name
            if self.dataflow_options is not None
            else None
        )
        # TODO: find an easy way to combine dataflow region and BQ location
        dataflow_region = (
            self.dataflow_options.region
            if self.dataflow_options is not None
            else None
        )
        return sensor_store.SensorStoreSink(
            schema=schema,
            algorithm_name=algorithm_name,
            algorithm_version=algorithm_version,
            overwrite_key_generator=overwrite_key_generator,
            data_source_cache=self.get_data_source_dict(),
            env=self._env,
            creds=self._creds,
            study=self._registry,
            api_key=api_key,
            request_retry_timeout=request_retry_timeout,
            fail_fast=self._streaming,
            log_process_time_metrics=self._streaming,
            incremental_options=self._incremental_options,
            billing_project=self._gcp_project,
            dataflow_job_name=dataflow_job_name,
            streaming=self._streaming,
            global_qps_limit=global_qps_limit,
            dataflow_region=dataflow_region,
        )

    def write_data_points_to_big_query(
        self,
        table_id: str,
        schema: Type[schemas.DataPointType],
        write_disposition: str = bigquery.WriteDisposition.WRITE_TRUNCATE,
    ) -> beam.PTransform:
        """Returns a PTransform that can be used to write data to BigQuery.

        Data written by this PTransform will have the same schema as the Echo
        BigQuery tables.

        Args:
        table_id: The table_id to write data points to. Of the format:
            {project_id}.{data_set}.{table_name}
        schema: The beam schema that will be written to BigQuery.
        write_disposition: to specify whether to write append, write empty,
        or write truncate. Defaults to write truncate. Of the format:
            bigquery.WriteDisposition.{WRITE_TYPE}
        """
        return data_points_sink.WriteDataPointsToBigQuery(
            table_id=table_id,
            project_id=self._gcp_project,
            schema=schema,
            creds=self._creds,
            temp_gcs_bucket=self._temp_gcs_bucket,
            data_source_cache=self.get_data_source_dict(),
            write_disposition=write_disposition,
            streaming=self._streaming,
            bigquery_location=self._bigquery_location,
        )

    def write_annotations_to_bigquery(self, table_id: str) -> beam.PTransform:
        """Returns a PTransform used to write annotations to BigQuery."""
        return annotations_sink.WriteAnnotationsToBigQuery(
            table_id=table_id,
            creds=self._creds,
            temp_gcs_bucket=self._temp_gcs_bucket,
            bigquery_location=self._bigquery_location,
        )

    def get_data_source_dict(self) -> DataSourceCache:
        """Returns the DataSource cache reference."""
        if self._streaming:
            return self._data_source_cache_handler.get_streaming_cache()
        return self._data_source_cache_handler.get_batch_cache()

    def inspect(self, pcol):
        return pcol | PrintPcol()

    def collect(self, *args, **kwargs):
        if not self._is_interactive_runner:
            raise ValueError(
                '`collect` method only supported for interactive runner.'
            )
        if 'pcoll' in kwargs:
            pcoll = kwargs['pcoll']
            # Moving into args below
            del kwargs['pcoll']
        else:
            pcoll = args[0]

        if pcoll.element_type == pd.DataFrame or (
            'union_types' in dir(pcoll.element_type)
            and pd.DataFrame in pcoll.element_type.union_types
        ):
            # Beam has issues materializing a pcoll of DataFrames. To handle
            # this, we convert to a tuple and then drop the key from the
            # materialized output.
            pcoll = pcoll | beam.Map(lambda x: (1, x))
            output = ib.collect(*(pcoll,), **kwargs)
            return list(output.to_numpy()[:, 1])

        return ib.collect(*(pcoll,), **kwargs)

    def show(self, *args, **kwargs):
        if not self._is_interactive_runner:
            raise ValueError(
                '`show` method only supported for interactive runner.'
            )
        return ib.show(*args, **kwargs)

    def show_graph(self):
        if not self._is_interactive_runner:
            raise ValueError(
                '`show` method only supported for interactive runner.'
            )
        return ib.show_graph(self._p)
