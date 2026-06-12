"""Tests for throttler.py"""

import unittest
from unittest import mock

import pandas as pd

from verily.ds_sdk.core.gcp import throttler


class ThrottlerTest(unittest.TestCase):

    def test_throttler(self):
        one_qps_throttler = throttler.Throttler(global_limit_per_second=1,
                                                ds_sdk_creds=None,
                                                dataflow_job_name=None,
                                                dataflow_project_id=None,
                                                dataflow_region=None)
        start_time = pd.Timestamp.now(tz='UTC')
        # Three calls to the throttler with a 1 QPS limit should always take
        # longer than 2 seconds.
        # - The first call will be throttled between 0 and 2 seconds
        # - The second and third call will each be throttled for 1 second
        for _ in range(3):
            one_qps_throttler.throttle()
        end_time = pd.Timestamp.now(tz='UTC')
        total_time = end_time - start_time
        self.assertGreater(total_time.total_seconds(), 2)

    @mock.patch('verily.ds_sdk.core.gcp.throttler._get_dataflow_shard_count')
    def test_failed_to_fetch_dataflow_info_default(
            self, shard_count_mock: mock.MagicMock):

        shard_count_mock.side_effect = ValueError('test error')

        one_qps_throttler = throttler.Throttler(
            global_limit_per_second=1,
            ds_sdk_creds=None,
            dataflow_job_name='job_name',
            dataflow_project_id='project_id',
            dataflow_region='region')

        start_time = pd.Timestamp.now(tz='UTC')
        # Three calls to the throttler with a 1 QPS limit should always take
        # longer than 2 seconds.
        # - The first call will be throttled between 0 and 2 seconds
        # - The second and third call will each be throttled for 1 second
        for _ in range(3):
            one_qps_throttler.throttle()
        end_time = pd.Timestamp.now(tz='UTC')
        total_time = end_time - start_time
        self.assertGreater(total_time.total_seconds(), 2)
        self.assertEqual(shard_count_mock.call_count, 5)

    @mock.patch('verily.ds_sdk.core.gcp.throttler._get_dataflow_shard_count')
    def test_failed_to_fetch_dataflow_info_retries(
            self, shard_count_mock: mock.MagicMock):

        shard_count_mock.side_effect = [ValueError('test error'), 3]

        one_qps_throttler = throttler.Throttler(
            global_limit_per_second=1,
            ds_sdk_creds=None,
            dataflow_job_name='job_name',
            dataflow_project_id='project_id',
            dataflow_region='region')

        start_time = pd.Timestamp.now(tz='UTC')
        # Three calls to the throttler with a 1 QPS across 3 workers should
        # always take longer than 6 seconds.
        # - The first call will be throttled between 0 and 2 seconds
        # - The second and third call will each be throttled for 3 second
        # NOTE: we want to valid this with three workers to ensure it is not
        # taking the default value if we fail to fetch the number of workers 5
        # times.
        for _ in range(3):
            one_qps_throttler.throttle()
        end_time = pd.Timestamp.now(tz='UTC')
        total_time = end_time - start_time
        self.assertGreater(total_time.total_seconds(), 6)
        self.assertEqual(shard_count_mock.call_count, 2)


if __name__ == '__main__':
    unittest.main()
