"""Tests for echo_dedupe."""

import copy
from typing import Optional
import unittest

import apache_beam as beam
from apache_beam.testing.test_pipeline import TestPipeline
from apache_beam.testing.util import assert_that
from apache_beam.testing.util import equal_to
from apache_beam.utils.timestamp import Timestamp
import pandas as pd

from verily.ds_sdk.core import schemas
from verily.ds_sdk.core.io.bigquery import echo_dedupe


def build_timestamp(timestamp_str: str) -> Timestamp:
    return Timestamp.from_utc_datetime(pd.Timestamp(timestamp_str, tz='UTC'))


def build_dp_metadata(
        data_source_id: int, bucket_write_time: Timestamp,
        bucket_start: Timestamp,
        deleted_time: Optional[Timestamp]) -> schemas.DataPointMetadata:
    echo_metadata = schemas.EchoMetadata(
        bucket_start=bucket_start,
        bucket_write_time=bucket_write_time,
        deleted_time=deleted_time,
        snapshot_time=build_timestamp('2020-01-01 12:00:00'),
    )
    return schemas.data_point_metadata_for_raw_data(
        data_source_id=data_source_id,
        device_id='C2Q12345',
        participant_id='12345',
        participant_namespace=1,
        echo_metadata=echo_metadata,
        sensor_store_metadata=None,
        annotation_labels=set())


class EchoDedupeTest(unittest.TestCase):

    def test_remove_dupes(self):

        # identical_dps1 should not be deduped.
        identical_dps1 = schemas.Pressure(
            data_point_metadata=build_dp_metadata(
                data_source_id=12345,
                bucket_write_time=build_timestamp('2020-01-01 12:00:00'),
                bucket_start=build_timestamp('2020-01-01 12:00:00'),
                deleted_time=None),
            measurement_timestamp_utc=build_timestamp('2020-01-01 12:00:00'),
            pressure=1)

        # identical_dps2 should be deduped as an identical point.
        identical_dps2 = copy.deepcopy(identical_dps1)
        # stale_dps should be deduped because new_dps has the same bucket start
        # time, and a newer bucket write time.
        stale_dps = schemas.Pressure(data_point_metadata=build_dp_metadata(
            data_source_id=12345,
            bucket_write_time=build_timestamp('2020-01-02 12:00:00 UTC'),
            bucket_start=build_timestamp('2020-01-02 12:00:00 UTC'),
            deleted_time=None),
                                     measurement_timestamp_utc=build_timestamp(
                                         '2020-01-02 12:00:00 UTC'),
                                     pressure=1)
        # stale_dps_different_data_source should *not* deduped because it has a
        # different DataSource.name, so it's in a separate bundle.
        stale_dps_different_data_source = schemas.Pressure(
            data_point_metadata=build_dp_metadata(
                data_source_id=54321,
                bucket_write_time=build_timestamp('2020-01-02 12:00:00 UTC'),
                bucket_start=build_timestamp('2020-01-02 12:00:00 UTC'),
                deleted_time=None),
            measurement_timestamp_utc=build_timestamp(
                '2020-01-02 12:00:00 UTC'),
            pressure=1)
        # new_dps should not be deduped (newest write time for bundle).
        new_dps = schemas.Pressure(data_point_metadata=build_dp_metadata(
            data_source_id=12345,
            bucket_write_time=build_timestamp('2020-01-03 12:00:00 UTC'),
            bucket_start=build_timestamp('2020-01-02 12:00:00 UTC'),
            deleted_time=None),
                                   measurement_timestamp_utc=build_timestamp(
                                       '2020-01-03 12:00:00 UTC'),
                                   pressure=1)
        # unique_dps should not be deduped.
        dp_metadata = build_dp_metadata(
            data_source_id=12345,
            bucket_write_time=build_timestamp('2020-01-04 12:00:00 UTC'),
            bucket_start=build_timestamp('2020-01-04 12:00:00 UTC'),
            deleted_time=None)
        unique_dps = schemas.Pressure(data_point_metadata=dp_metadata,
                                      measurement_timestamp_utc=build_timestamp(
                                          '2020-01-04 12:00:00 UTC'),
                                      pressure=1)
        # deleted_dps should be deduped because it has a DeletedTime
        # (overwritten).
        deleted_dps = schemas.Pressure(
            data_point_metadata=build_dp_metadata(
                data_source_id=12345,
                bucket_write_time=build_timestamp('2020-01-05 12:00:00 UTC'),
                bucket_start=build_timestamp('2020-01-05 12:00:00 UTC'),
                deleted_time=build_timestamp('2020-01-01 12:00:00 UTC')),
            measurement_timestamp_utc=build_timestamp(
                '2020-01-05 12:00:00 UTC'),
            pressure=1)
        # replaced_dps should be deduped because replaced_deleted_dps point was
        # written at the same measurement time, with a newer timestamp.
        replaced_dps = schemas.Pressure(
            data_point_metadata=build_dp_metadata(
                data_source_id=12345,
                bucket_write_time=build_timestamp('2020-01-06 12:00:00 UTC'),
                bucket_start=build_timestamp('2020-01-06 12:00:00 UTC'),
                deleted_time=None),
            measurement_timestamp_utc=build_timestamp(
                '2020-01-06 12:00:00 UTC'),
            pressure=1)
        # replaced_deleted_dps should be deduped because it has a DeletedTime
        # (overwritten). So we end up with no data points on 1/6/20.
        replaced_deleted_dps = schemas.Pressure(
            data_point_metadata=build_dp_metadata(
                data_source_id=12345,
                bucket_write_time=build_timestamp('2020-01-07 12:00:00 UTC'),
                bucket_start=build_timestamp('2020-01-06 12:00:00 UTC'),
                deleted_time=build_timestamp('2020-01-01 12:00:00 UTC')),
            measurement_timestamp_utc=build_timestamp(
                '2020-01-06 12:00:00 UTC'),
            pressure=1)

        with TestPipeline() as p:

            output = (p | beam.Create([
                identical_dps1, identical_dps2, stale_dps, new_dps, unique_dps,
                stale_dps_different_data_source, deleted_dps, replaced_dps,
                replaced_deleted_dps
            ]) | echo_dedupe.RemoveEchoDupes())

            assert_that(
                output,
                equal_to([
                    identical_dps1, new_dps, unique_dps,
                    stale_dps_different_data_source
                ]))

    def test_non_identical_dupes_fail(self):
        dps = schemas.Pressure(data_point_metadata=build_dp_metadata(
            data_source_id=12345,
            bucket_write_time=build_timestamp('2020-01-01 12:00:00 UTC'),
            bucket_start=build_timestamp('2020-01-01 12:00:00 UTC'),
            deleted_time=None),
                               measurement_timestamp_utc=build_timestamp(
                                   '2020-01-01 12:00:00 UTC'),
                               pressure=1)
        non_identical_dps = schemas.Pressure(
            data_point_metadata=build_dp_metadata(
                data_source_id=12345,
                bucket_write_time=build_timestamp('2020-01-01 12:00:00 UTC'),
                bucket_start=build_timestamp('2020-01-01 12:00:00 UTC'),
                deleted_time=None),
            measurement_timestamp_utc=build_timestamp(
                '2020-01-01 12:00:00 UTC'),
            pressure=2)

        with self.assertRaises(RuntimeError):
            with TestPipeline() as p:
                _ = (p | beam.Create([dps, non_identical_dps]) |
                     echo_dedupe.RemoveEchoDupes())


if __name__ == '__main__':
    unittest.main()
