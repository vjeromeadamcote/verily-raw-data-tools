"""Tests for incremental_query."""

import unittest

import ibis_bigquery
import pandas as pd

from verily.ds_sdk.core import options
from verily.ds_sdk.core.io.bigquery import incremental_query
from verily.ds_sdk.core.testing import ibis_testing_utils


class IncrementalQueryTest(unittest.TestCase):

    def setUp(self):
        super().setUp()
        self._ibis_table = ibis_testing_utils.data_points_table('table_id')
        self._internal_ibis_table = ibis_testing_utils.internal_data_points_table('internal_table_id')  # pylint: disable=line-too-long
        self._participants_table = ibis_testing_utils.paricipant_mapping_table(
            'participants')

    def test_query_in_annotation_format(self):
        inc_options = options.IncrementalQueryOptions(
            write_start_time=pd.Timestamp('2010-01-01'),
            write_end_time=pd.Timestamp('2010-01-02'))
        query = incremental_query._buid_incremental_query(  # pylint: disable=protected-access
            inc_options,
            self._ibis_table,
            self._participants_table,
            False,
        )

        expected_query = """\
WITH t0 AS (
  SELECT t2.*, t4.`ParticipantId`
  FROM table_id t2
    LEFT OUTER JOIN participants t4
      ON (t2.`DeviceID` = t4.`DeviceId`) AND
         (t2.`DataPointTime` >= t4.`StartTime`) AND
         (t2.`DataPointTime` < t4.`EndTime`)
)
SELECT DISTINCT t1.`ParticipantId` AS `user_id`, t1.`DeviceID` AS `device_id`,
       'incremental' AS `annotation_label`,
       TIMESTAMP_TRUNC(t1.`DataPointTime`, HOUR) AS `start_timestamp_utc`,
       TIMESTAMP_ADD(TIMESTAMP_TRUNC(t1.`DataPointTime`, HOUR), INTERVAL 1 HOUR) AS `end_timestamp_utc`
FROM t0 t1
WHERE (t1.`BucketWriteTime` >= TIMESTAMP '2010-01-01 00:00:00') AND
      (t1.`BucketWriteTime` < TIMESTAMP '2010-01-02 00:00:00')"""
        self.assertEqual(ibis_bigquery.compile(query), expected_query)

    def test_query_internal_table_in_annotation_format(self):
        inc_options = options.IncrementalQueryOptions(
            write_start_time=pd.Timestamp('2010-01-01'),
            write_end_time=pd.Timestamp('2010-01-02'))
        query = incremental_query._buid_incremental_query(  # pylint: disable=protected-access
            inc_options,
            self._internal_ibis_table,
            self._participants_table,
            True,
        )

        expected_query = """\
WITH t0 AS (
  SELECT t2.*, t4.`ParticipantId`
  FROM internal_table_id t2
    LEFT OUTER JOIN participants t4
      ON (t2.`DeviceID` = t4.`DeviceId`) AND
         (t2.`DataPointTime` >= t4.`StartTime`) AND
         (t2.`DataPointTime` < t4.`EndTime`)
)
SELECT DISTINCT t1.`ParticipantId` AS `user_id`, t1.`DeviceID` AS `device_id`,
       'incremental' AS `annotation_label`,
       TIMESTAMP_TRUNC(t1.`DataPointTime`, HOUR) AS `start_timestamp_utc`,
       TIMESTAMP_ADD(TIMESTAMP_TRUNC(t1.`DataPointTime`, HOUR), INTERVAL 1 HOUR) AS `end_timestamp_utc`
FROM t0 t1
WHERE (t1.`DataPointWriteTime` >= TIMESTAMP '2010-01-01 00:00:00') AND
      (t1.`DataPointWriteTime` < TIMESTAMP '2010-01-02 00:00:00')"""
        print(ibis_bigquery.compile(query))
        self.assertEqual(ibis_bigquery.compile(query), expected_query)

    def test_query_hour_rounding(self):
        inc_options = options.IncrementalQueryOptions(
            write_start_time=pd.Timestamp('2010-01-01'),
            write_end_time=pd.Timestamp('2010-01-02'),
            incremental_timestamp_part=options.TimestampPart.DAY)
        query = incremental_query._buid_incremental_query(  # pylint: disable=protected-access
            inc_options, self._ibis_table, self._participants_table, False)

        expected_query = """\
WITH t0 AS (
  SELECT t2.*, t4.`ParticipantId`
  FROM table_id t2
    LEFT OUTER JOIN participants t4
      ON (t2.`DeviceID` = t4.`DeviceId`) AND
         (t2.`DataPointTime` >= t4.`StartTime`) AND
         (t2.`DataPointTime` < t4.`EndTime`)
)
SELECT DISTINCT t1.`ParticipantId` AS `user_id`, t1.`DeviceID` AS `device_id`,
       'incremental' AS `annotation_label`,
       TIMESTAMP_TRUNC(t1.`DataPointTime`, HOUR) AS `start_timestamp_utc`,
       TIMESTAMP_ADD(TIMESTAMP_TRUNC(t1.`DataPointTime`, HOUR), INTERVAL 24 HOUR) AS `end_timestamp_utc`
FROM t0 t1
WHERE (t1.`BucketWriteTime` >= TIMESTAMP '2010-01-01 00:00:00') AND
      (t1.`BucketWriteTime` < TIMESTAMP '2010-01-02 00:00:00')"""

        self.assertEqual(ibis_bigquery.compile(query), expected_query)

    def test_query_no_start_time(self):
        inc_options = options.IncrementalQueryOptions(
            write_end_time=pd.Timestamp('2010-01-02'))
        query = incremental_query._buid_incremental_query(  # pylint: disable=protected-access
            inc_options, self._ibis_table, self._participants_table, False)

        expected_query = """\
WITH t0 AS (
  SELECT t2.*, t4.`ParticipantId`
  FROM table_id t2
    LEFT OUTER JOIN participants t4
      ON (t2.`DeviceID` = t4.`DeviceId`) AND
         (t2.`DataPointTime` >= t4.`StartTime`) AND
         (t2.`DataPointTime` < t4.`EndTime`)
)
SELECT DISTINCT t1.`ParticipantId` AS `user_id`, t1.`DeviceID` AS `device_id`,
       'incremental' AS `annotation_label`,
       TIMESTAMP_TRUNC(t1.`DataPointTime`, HOUR) AS `start_timestamp_utc`,
       TIMESTAMP_ADD(TIMESTAMP_TRUNC(t1.`DataPointTime`, HOUR), INTERVAL 1 HOUR) AS `end_timestamp_utc`
FROM t0 t1
WHERE (t1.`BucketWriteTime` >= TIMESTAMP '1677-09-21 00:00:00') AND
      (t1.`BucketWriteTime` < TIMESTAMP '2010-01-02 00:00:00')"""

        self.assertEqual(ibis_bigquery.compile(query), expected_query)

    def test_query_export_mode(self):
        inc_options = options.IncrementalQueryOptions(
            write_start_time=pd.Timestamp('2010-01-01'),
            write_end_time=pd.Timestamp('2010-01-02'),
            incremental_query_mode=options.IncrementQueryMode.EXPORT)
        query = incremental_query._buid_incremental_query(  # pylint: disable=protected-access
            inc_options, self._ibis_table, self._participants_table, False)

        expected_query = """\
WITH t0 AS (
  SELECT t2.*, t4.`ParticipantId`
  FROM table_id t2
    LEFT OUTER JOIN participants t4
      ON (t2.`DeviceID` = t4.`DeviceId`) AND
         (t2.`DataPointTime` >= t4.`StartTime`) AND
         (t2.`DataPointTime` < t4.`EndTime`)
)
SELECT DISTINCT t1.`ParticipantId` AS `user_id`, t1.`DeviceID` AS `device_id`,
       'incremental' AS `annotation_label`,
       t1.`DataPointTime` AS `start_timestamp_utc`,
       t1.`DataPointTime` AS `end_timestamp_utc`
FROM t0 t1
WHERE (t1.`BucketWriteTime` >= TIMESTAMP '2010-01-01 00:00:00') AND
      (t1.`BucketWriteTime` < TIMESTAMP '2010-01-02 00:00:00')"""

        self.assertEqual(ibis_bigquery.compile(query), expected_query)

    def test_query_expand_intervals(self):
        inc_options = options.IncrementalQueryOptions(
            write_start_time=pd.Timestamp('2010-01-01'),
            write_end_time=pd.Timestamp('2010-01-02'),
            incremental_timestamp_part=options.TimestampPart.DAY,
            incremental_query_mode=options.IncrementQueryMode.EXPAND_INTERVALS)
        query = incremental_query._buid_incremental_query(  # pylint: disable=protected-access
            inc_options, self._ibis_table, self._participants_table, False)

        expected_query = """\
WITH t0 AS (
  SELECT t2.*, t4.`ParticipantId`
  FROM table_id t2
    LEFT OUTER JOIN participants t4
      ON (t2.`DeviceID` = t4.`DeviceId`) AND
         (t2.`DataPointTime` >= t4.`StartTime`) AND
         (t2.`DataPointTime` < t4.`EndTime`)
)
SELECT DISTINCT t1.`ParticipantId` AS `user_id`, t1.`DeviceID` AS `device_id`,
       'incremental' AS `annotation_label`,
       TIMESTAMP_TRUNC(TIMESTAMP_SUB(t1.`DataPointTime`, INTERVAL 24 HOUR), HOUR) AS `start_timestamp_utc`,
       TIMESTAMP_ADD(TIMESTAMP_ADD(TIMESTAMP_TRUNC(TIMESTAMP_SUB(t1.`DataPointTime`, INTERVAL 24 HOUR), HOUR), INTERVAL 48 HOUR), INTERVAL 2 HOUR) AS `end_timestamp_utc`
FROM t0 t1
WHERE (t1.`BucketWriteTime` >= TIMESTAMP '2010-01-01 00:00:00') AND
      (t1.`BucketWriteTime` < TIMESTAMP '2010-01-02 00:00:00')"""

        self.assertEqual(ibis_bigquery.compile(query), expected_query)


if __name__ == '__main__':
    unittest.main()
