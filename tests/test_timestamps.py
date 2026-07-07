"""Tests for verily.raw_data_tools.utils.timestamps.

Ported from verily.ds_sdk.core.utils.timestamps_test.
"""

import datetime
import unittest

from apache_beam.utils.timestamp import Timestamp
import pandas as pd
import pytz

from verily.raw_data_tools.utils import timestamps


class TimestampsTest(unittest.TestCase):

    def test_parse_bigquery_timestamp_datetime(self):
        timestamp = pd.Timestamp('2020-01-01', tz=pytz.utc)
        want = Timestamp.from_utc_datetime(timestamp)
        got = timestamps.parse_bigquery_timestamp(timestamp)
        self.assertEqual(want, got)

    def test_parse_bigquery_timestamp_datetime_no_tz(self):
        timestamp = pd.Timestamp('2020-01-01')
        want = Timestamp.from_utc_datetime(
            pd.Timestamp('2020-01-01', tz=pytz.utc))
        got = timestamps.parse_bigquery_timestamp(timestamp)
        self.assertEqual(want, got)

    def test_parse_bigquery_timestamp_micros(self):
        timestamp = pd.Timestamp('2020-01-01', tz=pytz.utc)
        micros = int(timestamp.timestamp() * 1000000)
        want = Timestamp.from_utc_datetime(timestamp)
        got = timestamps.parse_bigquery_timestamp(micros)
        self.assertEqual(want, got)

    def test_parse_bigquery_timestamp_micros_overflow(self):
        max_bq_timestamp_micros = 9223372036854776
        want = Timestamp.from_utc_datetime(
            pd.Timestamp.max.tz_localize(pytz.utc))
        got = timestamps.parse_bigquery_timestamp(max_bq_timestamp_micros)
        self.assertEqual(want, got)

    def test_parse_bigquery_timestamp_none(self):
        got = timestamps.parse_bigquery_timestamp(None, allow_null=True)
        self.assertIsNone(got)

    def test_parse_bigquery_timestamp_none_not_allowed(self):
        with self.assertRaises(ValueError):
            timestamps.parse_bigquery_timestamp(None)

    def test_datetime_to_beam_timestamp_non_utc_timezone(self):
        timestamp = datetime.datetime.fromtimestamp(
            123123, tz=pytz.timezone('US/Eastern'))
        with self.assertRaisesRegex(
                ValueError,
                'timestamp had a non UTC timezone set: US/Eastern'):
            timestamps.datetime_to_beam_timestamp(timestamp)

    def test_millis_to_utc_datetime(self):
        self.assertEqual(
            timestamps.millis_to_utc_datetime(1641049200000),
            datetime.datetime(2022, 1, 1, 15, 0, 0, 0,
                              tzinfo=datetime.timezone.utc))
        self.assertEqual(
            timestamps.millis_to_utc_datetime(1672660800000),
            datetime.datetime(2023, 1, 2, 12, 0, 0, 0,
                              tzinfo=datetime.timezone.utc))

    def test_round_down_to_nearest_second(self):
        timestamp = pd.Timestamp('2020-01-01 12:15:30.123456', tz=pytz.utc)
        want = pd.Timestamp('2020-01-01 12:15:30', tz=pytz.utc)
        got = timestamps.round_down_to_nearest_second(timestamp)
        self.assertEqual(want, got)

    def test_round_up_to_nearest_second(self):
        timestamp = pd.Timestamp('2020-01-01 12:15:30.123456', tz=pytz.utc)
        want = pd.Timestamp('2020-01-01 12:15:31', tz=pytz.utc)
        got = timestamps.round_up_to_nearest_second(timestamp)
        self.assertEqual(want, got)


if __name__ == '__main__':
    unittest.main()
