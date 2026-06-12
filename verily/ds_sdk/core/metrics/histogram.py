# Lint as: python3
"""Beam metrics for tracking histograms"""

import dataclasses
from typing import List, Optional

import apache_beam as beam


@dataclasses.dataclass(frozen=True)
class Bucket:
    bucket_start: int
    bucket_end: Optional[int]

    def __str__(self):
        if self.bucket_end is None:
            return f'{self.bucket_start}'
        return f'{self.bucket_start}_{self.bucket_end}'


class HistogramBase:
    """Base class for all histogram based metrics."""

    def __init__(self, namespace: str, name: str, buckets: List[Bucket]):
        self.namespace = namespace
        self.name = name
        self._buckets = {}
        for bucket in buckets:
            self._buckets[bucket] = beam.metrics.Metrics.counter(
                self.namespace, f'{self.name}_{bucket}')

    def update(self, value: int):
        if value < 0:
            raise ValueError('Values for histograms must be positive.')
        bucket = self._get_bucket(value)
        c = self._buckets[bucket]
        c.inc()

    def _get_bucket(self, value: int) -> Bucket:
        for bucket in self._buckets:
            if value >= bucket.bucket_start:
                if bucket.bucket_end is None or value < bucket.bucket_end:
                    return bucket
        raise ValueError(
            'No bucket found for value: {value}. bucket {self._buckets}')


class LinearHistogram(HistogramBase):
    """Histogram metric with equal size buckets"""

    def __init__(self, namespace: str, name: str, num_buckets: int,
                 bucket_step: int):
        buckets = []
        previous_bucket = 0
        for _ in range(num_buckets):
            bucket_end = previous_bucket + bucket_step
            buckets.append(
                Bucket(bucket_start=previous_bucket, bucket_end=bucket_end))
            previous_bucket = bucket_end
        buckets.append(Bucket(bucket_start=previous_bucket, bucket_end=None))
        super().__init__(namespace, name, buckets)
