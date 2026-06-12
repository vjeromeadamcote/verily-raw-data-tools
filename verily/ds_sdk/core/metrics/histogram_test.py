# Lint as: python3
"""Tests for histogram."""

import unittest

import apache_beam as beam
from apache_beam.testing import test_pipeline

from verily.ds_sdk.core.metrics import histogram


class HistogramTest(unittest.TestCase):

    def test_linear_histogram(self):

        def increment(elem):
            linear_histogram = histogram.LinearHistogram(
                'namespace', 'name', 10, 1)

            linear_histogram.update(elem)

        p = test_pipeline.TestPipeline()

        _ = p | beam.Create([1, 3, 3, 5, 100, 200]) | beam.Map(increment)

        results = p.run()

        metrics = results.metrics().query(
            beam.metrics.MetricsFilter().with_namespace(
                'namespace'))['counters']
        expected = {
            'name_1_2': 1,
            'name_3_4': 2,
            'name_5_6': 1,
            'name_10': 2,
        }
        got = {}
        for metric in metrics:
            metric_name = metric.key.metric.name
            got[metric_name] = metric.committed
        self.assertDictEqual(expected, got)


if __name__ == '__main__':
    unittest.main()
