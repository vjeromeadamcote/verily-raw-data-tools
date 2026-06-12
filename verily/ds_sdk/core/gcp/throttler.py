"""Global throttler for throttling across a dataflow pipeline."""

import logging
import math
import multiprocessing
import random
import time
from typing import Optional

from google.cloud import dataflow_v1beta3
import pandas as pd

from verily.ds_sdk.core.gcp import credentials

# The number of calls to `throttle()` before we refresh the throttling
# variables.
NUM_THROTTLES_UNTIL_REFRESH = 50


class Throttler:
    """Throttler for rate limiting across workers in dataflow.

    This throttler should be create in the `setup()` method of a `DoFn` class.
    """

    def __init__(
        self,
        global_limit_per_second: float,
        ds_sdk_creds: credentials.DsSdkCredentials,
        dataflow_job_name: Optional[str],
        dataflow_project_id: Optional[str],
        dataflow_region: Optional[str],
    ):
        self._creds = ds_sdk_creds
        self.dataflow_job_name = dataflow_job_name
        self.project_id = dataflow_project_id
        self.global_limit_per_second = global_limit_per_second
        self.dataflow_region = dataflow_region
        self._set_throttling_vars()

    def throttle(self):
        wait_time = self._get_wait_time()
        if wait_time > 0:
            logging.info('throttling for: %f seconds', wait_time)
            time.sleep(wait_time)
        self._last_throttle_finish_time = pd.Timestamp.now(tz='UTC')
        self.num_calls += 1
        # If the number of calls to throttle exceeds our refresh limit reset
        # the throttling vars. This is done to account for scaling events which
        # could increase / decrease the number of workers. If the number of
        # workers increases or decreases we need to reset the time_between_calls
        # to remain as close to our global QPS limit as possible.
        if self.num_calls >= NUM_THROTTLES_UNTIL_REFRESH:
            self._set_throttling_vars()

    def _get_wait_time(self) -> float:
        if self._last_throttle_finish_time is None:
            return self._initial_sleep_time.total_seconds()
        now = pd.Timestamp.now(tz='UTC')
        time_diff = now - self._last_throttle_finish_time
        if time_diff > self._time_between_calls:
            return 0
        return (self._time_between_calls - time_diff).total_seconds()

    def _set_throttling_vars(self):
        if (self.dataflow_job_name is not None and
                self.project_id is not None and
                self.dataflow_region is not None):
            num_workers = None
            last_error = None
            for _ in range(5):
                try:
                    num_workers = _get_dataflow_shard_count(
                        self.dataflow_job_name, self.project_id, self._creds,
                        self.dataflow_region)
                    break
                except Exception as e:  # pylint: disable=broad-except
                    # Catch all exceptions to be extra safe.
                    logging.error(
                        'Failed to fetch number of workers. error: %s',  # pylint: disable=line-too-long
                        e)
                    last_error = e
            if num_workers is None:
                logging.error(
                    'Failed to fetch number of workers after 5 tries. Defaulting to 1 worker. error: %s',  # pylint: disable=line-too-long
                    last_error)
                num_workers = 1
        else:
            num_workers = 1
        self._local_limit_per_second = (self.global_limit_per_second /
                                        num_workers)
        self._last_throttle_finish_time: Optional[pd.Timestamp] = None
        self._time_between_calls = pd.Timedelta(
            math.ceil(1000 / self._local_limit_per_second), 'milliseconds')
        # Time to wait on the initial request. This is a random float
        # between 0 and 2x the interval between calls. This helps better
        # distribute the initial load.
        self._initial_sleep_time = pd.Timedelta(
            random.random() * self._time_between_calls * 2, 'milliseconds')
        self.num_calls = 0


def _get_dataflow_shard_count(
    dataflow_job_name: str,
    project_id: str,
    ds_sdk_creds: credentials.DsSdkCredentials,
    dataflow_region: str,
) -> int:
    """Returns the number of shards for the dataflow job.

    This is equal to the number of workers * cpu cores per worker.
    """
    creds, _ = ds_sdk_creds.get_credentials()
    dataflow_jobs_client = dataflow_v1beta3.JobsV1Beta3Client(credentials=creds)
    jobs_request = dataflow_v1beta3.ListJobsRequest(
        filter=dataflow_v1beta3.ListJobsRequest.Filter.ACTIVE,
        project_id=project_id,
        location=dataflow_region)
    jobs_page_result = dataflow_jobs_client.list_jobs(request=jobs_request)
    job_id = None
    for result in jobs_page_result:
        if result.name == dataflow_job_name:
            job_id = result.id
            break
    if job_id is None:
        raise ValueError(
            f'Unable to find dataflow job for job name: {dataflow_job_name} in'
            f' project: {project_id}')

    messages_client = dataflow_v1beta3.MessagesV1Beta3Client(credentials=creds)
    messages_request = dataflow_v1beta3.ListJobMessagesRequest(
        job_id=job_id,
        project_id=project_id,
        location=dataflow_region,
        minimum_importance=dataflow_v1beta3.JobMessageImportance.
        JOB_MESSAGE_BASIC)
    messages_page_result = messages_client.list_job_messages(
        request=messages_request)
    num_workers: Optional[int] = None
    for result in messages_page_result.pages:
        if result.autoscaling_events:
            # Autoscaling events are sorted by increasing timestamp so grab the
            # last event that recorded the current number of workers.
            for event in reversed(result.autoscaling_events):
                if (hasattr(event, 'current_num_workers') and
                        event.current_num_workers != 0):
                    num_workers = event.current_num_workers
                    break
    if num_workers is None:
        raise ValueError(f'Unable to find num workers for job id: {job_id} in'
                         f' project: {project_id}')

    num_shards = num_workers * multiprocessing.cpu_count()
    logging.info('Found %d workers and %d cores. Total shards=%d', num_workers,
                 multiprocessing.cpu_count(), num_shards)

    return num_shards
