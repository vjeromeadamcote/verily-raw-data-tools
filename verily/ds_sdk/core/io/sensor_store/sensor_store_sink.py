"""Sink for writing data to SensorStore."""

import logging
from typing import Iterable, Optional, Tuple, Type

import apache_beam as beam
from apache_beam.utils.timestamp import Timestamp
import pandas as pd

from verily.ds_sdk.core import options
from verily.ds_sdk.core import schemas
from verily.ds_sdk.core.gcp import credentials
from verily.ds_sdk.core.gcp import throttler
from verily.ds_sdk.core.io.data_source_cache import DataSourceCache
from verily.ds_sdk.core.sensorsuite import overwrite_keys
from verily.ds_sdk.core.sensorsuite import sensor_store_client
from verily.ds_sdk.core.utils import timestamps


def _log_processing_duration(elem_timestamp_param: Timestamp) -> None:
    try:
        process_start_timestamp = timestamps.beam_timestamp_to_pandas_timestamp(
            elem_timestamp_param)
    except OverflowError:
        # This case happens when running in batch mode and may happen if a user
        # defines a custom TimestampParam upstream.
        logging.error(
            'Unable to generate process duration metrics. If this is a '
            'production streaming pipeline, please reach out to sensors-infra '
            'for triaging.')
        return

    process_time_delta = pd.Timestamp.now(tz='UTC') - process_start_timestamp
    logging.info('pipeline process latency=%f (s)',
                 process_time_delta.total_seconds())


def _dump_errors(elem: Tuple[int, Iterable[Tuple[str, str]]]):
    _, errors = elem
    errors = list(errors)
    if errors:
        for error in errors:
            device_id, message = error
            logging.error('device_id: %s SensorStore write failed with: %s',
                          device_id, message)
        raise ValueError(
            'SensorStore writes failed. All errors have been dumped to the '
            'logs.')


def _update_state(error_count: int,
                  inc_options: options.IncrementalQueryOptions, registry: str,
                  creds: credentials.DsSdkCredentials, billing_project: str):
    if error_count == 0:
        inc_options.write_state_file(registry, creds, billing_project)


class SensorStoreSink(beam.PTransform):
    """PTransform for writing DataPointType objects to SensorStore.

  Args:
    schema: The schema that is being written to SensorStore, this schema must be
      annotated with the @dataspec annotation.
    algorithm_name: The name of the algorithm that generated the data point.
    algorithm_version: The version of the algorithm that generated the data
      point.
    overwrite_key_generator: A subclass of OverwriteKeyGenerator. This is the
      function that will be used to generate an overwrite key for each element.
    data_source_cache: The cache used to retrieve data sources.
    env: The enviroment to write data to.
    creds: The credentials to attach to write requests.
    study: The study to write data to.
    request_retry_timeout: How long to retry retryable errors before giving up.
    fail_fast: If True sensor store errors will be raised immediately. If False
      errors will be collected and an error will be raised once all write have
      been attempted.
  """

    def __init__(
        self,
        *,
        schema: Type[schemas.DataPointType],
        algorithm_name: str,
        algorithm_version: str,
        overwrite_key_generator: overwrite_keys.OverwriteKeyGeneratorType,
        data_source_cache: DataSourceCache,
        env: str,
        creds: credentials.DsSdkCredentials,
        api_key: str,
        study: str,
        request_retry_timeout: pd.Timedelta,
        fail_fast: bool,
        log_process_time_metrics: bool,
        incremental_options: Optional[options.IncrementalQueryOptions],
        billing_project: str,
        dataflow_job_name: Optional[str],
        streaming: bool,
        global_qps_limit: int,
        dataflow_region: Optional[str],
    ):
        super().__init__()
        if not hasattr(schema, 'data_spec_from_decorator'):
            # TODO(dyke): We should add a guide for how to go from writing to BQ
            # to writing to SensorStore. i.e. create data spec, add annotations,
            # etc..
            raise ValueError(
                f'schema: {schema.__name__} was not annotations with the '
                '`@dataspec` annotation. Before writing to SensorStore please '
                'annotation your schema with what dataspec it represents.')
        self._data_spec_name = schema.data_spec_from_decorator  # type: ignore
        self._schema = schema
        self._algorithm_name = algorithm_name
        self._algorithm_version = algorithm_version
        self._overwrite_key_generator = overwrite_key_generator
        self._data_source_cache = data_source_cache
        self._env = env
        self._creds = creds
        self._api_key = api_key
        self._study = study
        self._request_retry_timeout = request_retry_timeout
        self._fail_fast = fail_fast
        self._log_process_time_metrics = log_process_time_metrics
        self._incremental_options = incremental_options
        self._billing_project = billing_project
        self._dataflow_job_name = dataflow_job_name
        self._streaming = streaming
        self._global_qps_limit = global_qps_limit
        self._dataflow_region = dataflow_region

    def expand(self, pcol: beam.PCollection[schemas.DataPointType]):
        ss_writes = (pcol | beam.ParDo(
            _KeyByOverwriteKeyAndDevice(self._overwrite_key_generator,
                                        self._algorithm_name,
                                        self._algorithm_version),
            self._data_source_cache) | beam.GroupByKey() | beam.ParDo(
                _WriteToSensorStore(
                    data_spec_name=self._data_spec_name,
                    algorithm_name=self._algorithm_name,
                    algorithm_version=self._algorithm_version,
                    env=self._env,
                    creds=self._creds,
                    api_key=self._api_key,
                    study=self._study,
                    request_retry_timeout=self._request_retry_timeout,
                    fail_fast=self._fail_fast,
                    log_process_time_metrics=self._log_process_time_metrics,
                    dataflow_job_name=self._dataflow_job_name,
                    project_id=self._billing_project,
                    streaming=self._streaming,
                    global_qps_limit=self._global_qps_limit,
                    dataflow_region=self._dataflow_region),
                self._data_source_cache))

        if self._incremental_options is not None:
            _ = ss_writes | beam.combiners.Count.Globally() | beam.Map(
                _update_state,
                inc_options=self._incremental_options,
                registry=self._study,
                creds=self._creds,
                billing_project=self._billing_project)

        if not self._fail_fast:
            ss_writes = (ss_writes | 'Group errors' >> beam.GroupByKey() |
                         'Dump Errors' >> beam.Map(_dump_errors))

        return ss_writes


class _KeyByOverwriteKeyAndDevice(beam.DoFn):
    """DoFn for keying data points by overwrite key and device ID."""

    def __init__(
        self,
        overwrite_key_generator: overwrite_keys.OverwriteKeyGeneratorType,
        algorithm_name: str,
        algorithm_version: str,
    ):
        super().__init__()
        self._overwrite_key_generator = overwrite_key_generator
        self._algorithm_name = algorithm_name
        self._algorithm_version = algorithm_version

    def process(  # type: ignore[override]
        self,
        data_point: schemas.DataPointType,
        data_source_cache: DataSourceCache,
    ) -> Iterable[Tuple[Tuple[overwrite_keys.OverwriteKey, str],
                        schemas.DataPointType]]:
        data_source = data_source_cache.get(
            data_point.data_point_metadata.data_source_id, None)
        overwrite_key = self._overwrite_key_generator.generate_overwrite_key(
            data_point, self._algorithm_name, self._algorithm_version,
            data_source)
        device_id = data_point.data_point_metadata.device_id
        yield ((overwrite_key, device_id), data_point)


class _WriteToSensorStore(beam.DoFn):
    """DoFn for writing groups of data points to sensor store."""

    def __init__(
        self,
        *,
        env: str,
        creds: credentials.DsSdkCredentials,
        api_key: str,
        study: str,
        data_spec_name: str,
        algorithm_name: str,
        algorithm_version: str,
        request_retry_timeout: pd.Timedelta,
        fail_fast: bool,
        log_process_time_metrics: bool,
        dataflow_job_name: Optional[str],
        project_id: str,
        streaming: bool,
        global_qps_limit: int,
        dataflow_region: Optional[str],
    ):
        super().__init__()
        self._sensor_store_client = None
        self.env = env
        self.creds = creds
        self.api_key = api_key
        self.request_retry_timeout = request_retry_timeout
        self.study = study
        self.data_spec_name = data_spec_name
        self.algorithm_name = algorithm_name
        self.algorithm_version = algorithm_version
        self.fail_fast = fail_fast
        self.log_process_time_metrics = log_process_time_metrics
        self.dataflow_job_name = dataflow_job_name
        self.project_id = project_id
        self.streaming = streaming
        self.global_qps_limit = global_qps_limit
        self.dataflow_region = dataflow_region

    def setup(self):
        super().setup()
        self._sensor_store_client = sensor_store_client.SensorStoreClient(
            self.env, self.creds, self.api_key, self.request_retry_timeout)
        self._throttler = throttler.Throttler(self.global_qps_limit, self.creds,
                                              self.dataflow_job_name,
                                              self.project_id,
                                              self.dataflow_region)

    def process(  # type: ignore[override]
        self,
        elem: Tuple[Tuple[overwrite_keys.OverwriteKey, str],
                    Iterable[schemas.DataPointType]],
        data_source_cache: DataSourceCache,
        timestamp=beam.DoFn.TimestampParam
    ) -> Iterable[Tuple[int, Tuple[str, str]]]:
        key, data_points = elem
        overwrite_key, _ = key
        errors = []
        try:
            # Only throttle for batch pipelines
            if not self.streaming:
                self._throttler.throttle()  # type: ignore
            self._sensor_store_client.overwrite_data_point_batches(  # type: ignore # pylint: disable=line-too-long
                self.data_spec_name, self.algorithm_name,
                self.algorithm_version, data_points, data_source_cache,
                overwrite_key, self.study)
            if self.log_process_time_metrics:
                _log_processing_duration(timestamp)
        except sensor_store_client.SensorStoreError as e:
            if self.fail_fast:
                raise ValueError(
                    f'SensorStore writes failed for device: {e.device_id}'
                ) from e
            else:
                errors.append(((1), (e.device_id, e.message)))
        return errors
