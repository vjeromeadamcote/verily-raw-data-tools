"""Tests for verily.raw_data_tools.pipeline.dataflow_utils."""

import unittest

from verily.raw_data_tools.pipeline import dataflow_utils


class EscapeLabelsTest(unittest.TestCase):

    def test_lowercase_and_replace(self):
        self.assertEqual(
            'hello_world',
            dataflow_utils.escape_dataflow_job_labels('Hello World'))

    def test_special_characters(self):
        self.assertEqual(
            'foo_bar_baz',
            dataflow_utils.escape_dataflow_job_labels('foo.bar!baz'))

    def test_truncate_at_63(self):
        long_label = 'a' * 100
        result = dataflow_utils.escape_dataflow_job_labels(long_label)
        self.assertEqual(63, len(result))

    def test_hyphens_preserved(self):
        self.assertEqual(
            'my-job-label',
            dataflow_utils.escape_dataflow_job_labels('my-job-label'))

    def test_underscores_preserved(self):
        self.assertEqual(
            'my_job_label',
            dataflow_utils.escape_dataflow_job_labels('my_job_label'))

    def test_empty_string(self):
        self.assertEqual(
            '', dataflow_utils.escape_dataflow_job_labels(''))


class DataflowUrlsTest(unittest.TestCase):

    def test_get_dataflow_url(self):
        url = dataflow_utils.get_dataflow_url('job123', 'us-central1', 'proj')
        self.assertEqual(
            'https://console.cloud.google.com/dataflow/jobs'
            '/us-central1/job123?project=proj', url)

    def test_get_dataflow_metrics_url(self):
        url = dataflow_utils.get_dataflow_metrics_url(
            'job123', 'us-central1', 'proj')
        self.assertEqual(
            'https://console.cloud.google.com/dataflow/jobs'
            '/us-central1/job123/metrics?project=proj', url)


if __name__ == '__main__':
    unittest.main()
