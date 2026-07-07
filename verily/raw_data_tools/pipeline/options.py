"""Options for querying data with sensors_io.py"""

import argparse
import dataclasses
import enum
import json
import logging
import os
import sys
from typing import Any, Dict, Optional, Set, Tuple

from google.cloud import storage  # type: ignore
import pandas as pd

from verily.raw_data_tools.pipeline import credentials
from verily.raw_data_tools.pipeline import dataflow_utils


class JoinIf(enum.Enum):
    """Enum used to specify annotation joining logic."""

    # Any annotation labels must be present for the DataPoint to be joined.
    ANY = 1
    # All annotation labels must be present for the DataPoint to be joined.
    ALL = 2


@dataclasses.dataclass
class AnnotationInnerJoinOptions:
    """Options for joining data points with annotations."""

    # The set of annotation labels to inner join on.
    annotation_labels: Set[str] = dataclasses.field(default_factory=set)
    # The set of BigQuery table ids that contain the annotations.
    annotation_tables: Set[str] = dataclasses.field(default_factory=set)
    # If ANY: DataPoint will be joined if any of the annotation_labels are
    # present
    # If ALL: DataPoint will be joined if all of the annotation_labels are
    # present
    join_if: JoinIf = JoinIf.ANY
    # If True, will include participant fields in key when joining data.
    join_on_participant: bool = True
    # If True, will round the annotation start time down to the nearest second
    # and the end time up to the nearest second. This is useful for joining
    # annotations with data points that have timestamps that are rounded to the
    # nearest second.
    annotation_time_rounded_to_second: bool = False


class TimestampPart(enum.Enum):
    """Enum used for truncating timestamps from BigQuery."""
    SECOND = 1
    MINUTE = 2
    HOUR = 3
    DAY = 4

    def to_ibis_timestamp_unit(self) -> str:
        if self == TimestampPart.SECOND:
            return 's'
        if self == TimestampPart.MINUTE:
            return 'm'
        elif self == TimestampPart.HOUR:
            return 'h'
        elif self == TimestampPart.DAY:
            return 'D'
        raise ValueError(f'No ibis unit mapping for: {self}')

    def to_ibis_add_arguments(self) -> Tuple[int, str]:
        if self == self.DAY:
            # Ibis doesn't support adding one day so add 24 hours instead.
            return (24, TimestampPart.HOUR.to_ibis_timestamp_unit())
        return (1, self.to_ibis_timestamp_unit())


class IncrementQueryMode(enum.Enum):
    """Enum used to determine how an incremental query should be run."""

    # For this mode based on the DataPointQueryOptions.write_start_time and
    # DataPointQueryOptions.write_end_time we will find the min and max
    # measurement times for each user, device and return all data between the
    # timestamps
    # [DataPointQueryOptions.write_start_time, DataPointQueryOptions.write_end_time). pylint: disable=line-too-long
    # If DataPointQueryOptions.incremental_timestamp_part is set, we will floor
    # the min measurement time and ceiling the max measurement timestamp for
    # each
    # user and device.
    TRUNCATE_INTERVALS = 1
    # In EXPORT mode we will only return newly written data between:
    # [DataPointQueryOptions.write_start_time, DataPointQueryOptions.write_end_time). pylint: disable=line-too-long
    EXPORT = 2
    # For use with DataPointQueryOptions.incremental_timestamp_part. Rather than
    # taking the floor and ceiling of the timestamp part, will add/subtract the
    # entire timestamp part interval to the measurement timestamp for
    # each user and device. This will add extra overlap and can create
    # partial data beyond the time interval that needs to be considered.
    EXPAND_INTERVALS = 3


@dataclasses.dataclass
class IncrementalQueryOptions:
    """Options for joining querying data points by write time."""

    # The below fields control incremental runs that read from BigQuery.
    # Controls which incremental query mode to use.
    incremental_query_mode: IncrementQueryMode = IncrementQueryMode.TRUNCATE_INTERVALS  # pylint: disable=line-too-long
    # The start time to start the incremental query at. See IncrementQueryMode
    # documentation for how this is used per query mode.
    write_start_time: Optional[pd.Timestamp] = None
    # The end time to end the incremental query at. See IncrementQueryMode
    # documentation for how this is used per query mode.
    write_end_time: Optional[pd.Timestamp] = None
    # See: IncrementQueryMode.ALGO, documentation.
    incremental_timestamp_part: TimestampPart = TimestampPart.HOUR
    # Path where to read and write incremental state files to.
    # The state file is a file that indicates what the last time range for the
    # previous incremental runs were. This can be used so the next incremental
    # job picks up where the last one left off.
    #
    # One only of state_file_path and (write_start_time, write_end_time) should
    # be set.
    #
    # The state file is a JSON file named after the registry that the job is
    # running over. i.e. baseline.json
    # The JSON contents will look like the following:
    # { 'last_write_end_time': '2020-01-01' }
    # If no state file is present the job will assume no data has ever been
    # written for the data. The state file will be updated if a job completes
    # sucessfully.
    # TODO(dyke): it would be nice if this could keep track of internvals
    # instead of just the lastest, but that would require significant changes.
    state_file_path: Optional[str] = None
    # If no state file is present the max time to look back for the initial run.
    max_look_back: pd.Timedelta = pd.Timedelta('3d')

    def __post_init__(self):
        if ((self.write_start_time is not None or
             self.write_end_time is not None) and self.state_file_path):
            raise ValueError(
                'Only one of (write_start_time, write_end_time) and '
                'state_file_path should be set.')
        if self.write_start_time is not None and self.write_end_time is None:
            self.write_end_time = pd.Timestamp.now(tz='UTC')

    def _update_state_from_gcs(self, registry_file: str,
                               creds: credentials.RawDataToolsCredentials,
                               billing_project: str):
        if self.state_file_path is None:
            raise ValueError(
                'State file path was `None` cannot update state file')
        path = self.state_file_path.replace('gs://', '')
        if not path:
            raise ValueError(
                f'Invalid GCS path for state file: {self.state_file_path}')
        split_path = path.split('/', 1)
        bucket_name = split_path[0]
        blob_path = '/' + registry_file
        if len(split_path) > 1:
            blob_path = split_path[1] + blob_path

        # Read file from GCS
        creds, _ = creds.get_credentials()
        gcs_client = storage.Client(project=billing_project, credentials=creds)
        bucket = gcs_client.bucket(bucket_name)
        state_file_blob = bucket.get_blob(blob_path)
        if not state_file_blob:
            # state file doesn't exist
            logging.error(
                'No state file was found. Assuming this is the first run and '
                'running for the previous time delta: %s', self.max_look_back)
            self.write_end_time = pd.Timestamp.now(tz='UTC')
            self.write_start_time = self.write_end_time - self.max_look_back
            return
        state_file = json.loads(state_file_blob.download_as_bytes())
        last_end_time = state_file['last_write_end_time']
        self.write_start_time = pd.Timestamp(last_end_time)
        self.write_end_time = pd.Timestamp.now(tz='UTC')

    def _update_state_from_local(self, registry_file):
        full_path = os.path.join(self.state_file_path, registry_file)
        try:
            with open(full_path, 'r', encoding='utf-8') as f:
                state_file_json = json.load(f)
                last_end_time = state_file_json['last_write_end_time']
                self.write_start_time = pd.Timestamp(last_end_time)
                self.write_end_time = pd.Timestamp.now(tz='UTC')
        except FileNotFoundError:
            # state file doesn't exist
            logging.error(
                'No state file was found. Assuming this is the first run '
                'and running for the previous time delta: %s',
                self.max_look_back)
            self.write_end_time = pd.Timestamp.now(tz='UTC')
            self.write_start_time = self.write_end_time - self.max_look_back
            return

    def update_from_state_file(self, registry: str,
                               creds: credentials.RawDataToolsCredentials,
                               billing_project: str):
        if self.state_file_path is None:
            raise ValueError(
                'State file path was `None` cannot update state file')
        registry_file = f'{registry}.json'
        if self.state_file_path.startswith('gs://'):
            self._update_state_from_gcs(registry_file, creds, billing_project)
        else:
            self._update_state_from_local(registry_file)

    def write_state_file(self, registry: str,
                         creds: credentials.RawDataToolsCredentials,
                         billing_project: str):
        if self.state_file_path is None:
            logging.warning(
                'No incremental state file. Cannot commit progress.')
            return
        # Ensure Incremental options are up to date from last run.
        self.update_from_state_file(registry, creds, billing_project)
        registry_file = f'{registry}.json'
        to_write = {'last_write_end_time': self.write_end_time}
        if self.state_file_path.startswith('gs://'):
            path = self.state_file_path.replace('gs://', '')
            if not path:
                raise ValueError(
                    f'Invalid GCS path for state file: {self.state_file_path}')
            split_path = path.split('/', 1)
            bucket_name = split_path[0]
            blob_path = registry_file
            if len(split_path) > 1:
                blob_path = split_path[0] + blob_path

            # Read file from GCS
            creds, _ = creds.get_credentials()
            gcs_client = storage.Client(project=billing_project,
                                        credentials=creds)
            bucket = gcs_client.bucket(bucket_name)

            blob = bucket.blob(blob_path)
            blob.upload_from_string(json.dumps(to_write, default=str))
        else:
            full_path = os.path.join(self.state_file_path, registry_file)
            with open(full_path, mode='w', encoding='utf-8') as f:
                json.dump(to_write, f, default=str)


@dataclasses.dataclass
class StreamingSourceOptions:
    """Options for building SensorSuite Beam Sources for Batch pipelines."""
    # Whether or not the DataSources should be cached.
    cache_data_source: bool = True
    # The PubSub topic to read from. Cannot set if `subscription` is set.
    topic: Optional[str] = None
    # The PubSub subscription to read from. Cannot set if `topic` is set.
    subscription: Optional[str] = None
    # The time duration used for padding / splitting up SensorStore read
    # requests.
    ss_request_split_duration: Optional[pd.Timedelta] = pd.Timedelta('1h')
    # Redis Endpoint for storing the DataSource cache. format = host:port
    redis_endpoint: Optional[str] = None
    # Whether or not the points read from SensorStore should remained grouped.
    # This upstream grouping can help speed up streaming pipelines that need
    # points to be grouped downstream, but requires the user's pipeline to
    # handle the batch vs streaming logic changes.
    group_returned_points: bool = False

    def __post_init__(self):
        if self.topic is None and self.subscription is None:
            raise ValueError('Either `topic` or `subscription` must be set in '
                             'StreaminingSourceOptions.')
        if self.topic is not None and self.subscription is not None:
            raise ValueError('Cannot set both `topic` and `subscription` in '
                             'StreaminingSourceOptions.')


@dataclasses.dataclass
class BatchSourceOptions:
    """Options for building SensorSuite Beam Sources for Batch pipelines."""

    # Whether or not mock data should be generated using the source's schema.
    use_mock_source: bool = False
    # Whether or not the DataSources should be cached.
    cache_data_source: bool = True
    # Whether or not the rows should be cached locally.
    # TODO(tanke): Enable this once there are tests for the CacheableSource
    # transform.
    disable_cache: bool = True
    # Whether or not to remove duplicate rows.
    remove_duplicates: bool = True
    # Whether or not to try to populate participant information
    join_on_participant: bool = True


@dataclasses.dataclass
class DataflowOptions:
    """Options to provide to the DataflowRunner."""

    # The name of the job to launch on Dataflow.
    job_name: str
    # The region to launch the Dataflow job in.
    region: str = 'us-central1'
    # Any additional options to pass to the pipeline options on dataflow.
    additional_options: Dict[str, Any] = dataclasses.field(default_factory=dict)

    def to_pipeline_options(self) -> Dict[str, Any]:
        opts = self.additional_options
        opts['job_name'] = self.job_name
        opts['region'] = self.region

        try:
            from importlib.metadata import version as get_version
            rdt_version = get_version('verily-raw-data-tools')
            rdt_version = dataflow_utils.escape_dataflow_job_labels(
                rdt_version)
            version_label = f'raw_data_tools_version={rdt_version}'
            if 'labels' in opts:
                opts['labels'].append(version_label)
            else:
                opts['labels'] = [version_label]
        except Exception:
            logging.info(
                'Unable to read package version, version label will not '
                'be attached.')

        # TODO(b/248274745): see if this can be removed if we can decrease the
        # disk space required by the DS SDK.
        parser = argparse.ArgumentParser()
        parser.add_argument('--disk_size_gb',
                            help='Disk size to use for dataflow workers.',
                            required=False,
                            type=int,
                            default=50)

        parsed_args, _ = parser.parse_known_args(sys.argv)

        disk_size = parsed_args.disk_size_gb

        if disk_size < 50:
            logging.warning(
                'specified disk size was < 50GB. Bumping to 50GB to ensure '
                'workers can start succesfully')
            disk_size = 50

        opts['disk_size_gb'] = disk_size

        return opts
