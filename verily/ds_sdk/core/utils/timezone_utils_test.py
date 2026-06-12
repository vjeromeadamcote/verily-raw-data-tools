"""Tests for ds_sdk.sdk.core.utils.timezone_utils_test."""

import unittest

import pandas as pd
import pytz

import verily.ds_sdk.core.utils.timezone_utils as timezone_utils


class TimezoneUtilsTest(unittest.TestCase):

    def test_get_date_in_timezone(self):
        date_time = pd.to_datetime('2019-09-30 00:10:10.125 UTC')
        dt_millis = date_time.value // 10**6
        utc_timestamp = pd.Timestamp(dt_millis, unit='ms', tz='UTC')

        valid_timezone = 'US/Pacific'
        expected = '2019-09-29'
        result = timezone_utils.get_date_in_timezone(utc_timestamp,
                                                     valid_timezone)
        self.assertEqual(expected, result)

        invalid_timezone = 'Area/51'
        expected = '2019-09-30'
        result = timezone_utils.get_date_in_timezone(utc_timestamp,
                                                     invalid_timezone)
        self.assertEqual(expected, result)

        valid_pos_offset = '+05:00'
        expected = '2019-09-30'
        result = timezone_utils.get_date_in_timezone(utc_timestamp,
                                                     valid_pos_offset)
        self.assertEqual(expected, result)

        valid_neg_offset = '-07:00'
        expected = '2019-09-29'
        result = timezone_utils.get_date_in_timezone(utc_timestamp,
                                                     valid_neg_offset)
        self.assertEqual(expected, result)

        # Invalid values default to UTC.
        invalid_pos_hour_offset = '30:00'
        expected = '2019-09-30'
        result = timezone_utils.get_date_in_timezone(utc_timestamp,
                                                     invalid_pos_hour_offset)
        self.assertEqual(expected, result)

        invalid_neg_hour_offset = '-80:00'
        expected = '2019-09-30'
        result = timezone_utils.get_date_in_timezone(utc_timestamp,
                                                     invalid_neg_hour_offset)
        self.assertEqual(expected, result)

        pos_minute_offset = '00:15'
        expected = '2019-09-30'
        result = timezone_utils.get_date_in_timezone(utc_timestamp,
                                                     pos_minute_offset)
        self.assertEqual(expected, result)

        neg_minute_offset = '-00:15'
        expected = '2019-09-29'
        result = timezone_utils.get_date_in_timezone(utc_timestamp,
                                                     neg_minute_offset)
        self.assertEqual(expected, result)

        invalid_pos_min_offset = '00:80'
        expected = '2019-09-30'
        result = timezone_utils.get_date_in_timezone(utc_timestamp,
                                                     invalid_pos_min_offset)
        self.assertEqual(expected, result)

        invalid_neg_min_offset = '-00:80'
        expected = '2019-09-30'
        result = timezone_utils.get_date_in_timezone(utc_timestamp,
                                                     invalid_neg_min_offset)
        self.assertEqual(expected, result)

        valid_no_offset = '00:00'
        expected = '2019-09-30'
        result = timezone_utils.get_date_in_timezone(utc_timestamp,
                                                     valid_no_offset)
        self.assertEqual(expected, result)

        empty_str = ''
        expected = '2019-09-30'
        result = timezone_utils.get_date_in_timezone(utc_timestamp, empty_str)
        self.assertEqual(expected, result)

    def test_convert_timezone(self):
        utc_timestamp = pd.Timestamp('2019-10-07 10:00:00', tz='UTC')
        named_local_tz = 'US/Pacific'
        offset_local_tz = '-07:00'
        expected = 'US/Pacific'
        got = timezone_utils.convert_timezone(utc_timestamp, named_local_tz)
        self.assertEqual(expected, got)

        expected = pytz.FixedOffset(-420)
        got = timezone_utils.convert_timezone(utc_timestamp, offset_local_tz)
        self.assertEqual(expected, got)

        invalid_named_tz = 'Narnia'
        invalid_offset_tz = '-30:00'
        # Invalid timezones default to UTC.
        expected = 'UTC'
        got = timezone_utils.convert_timezone(utc_timestamp, invalid_named_tz)
        self.assertEqual(expected, got)
        got = timezone_utils.convert_timezone(utc_timestamp, invalid_offset_tz)
        self.assertEqual(expected, got)

    def test_convert_timestamp_timezone(self):
        utc_timestamp = pd.Timestamp('2019-10-07 10:00:00', tz='UTC')
        named_local_tz = 'US/Pacific'
        offset_local_tz = '-07:00'
        expected = pd.Timestamp('2019-10-07 03:00:00', tz='US/Pacific')

        got = timezone_utils.convert_timestamp_timezone(utc_timestamp,
                                                        named_local_tz)
        self.assertEqual(expected, got)
        got = timezone_utils.convert_timestamp_timezone(utc_timestamp,
                                                        offset_local_tz)
        self.assertEqual(expected, got)

        invalid_named_tz = 'Narnia'
        invalid_offset_tz = '-30:00'
        # Invalid timezones default to UTC.
        got = timezone_utils.convert_timestamp_timezone(utc_timestamp,
                                                        invalid_named_tz)
        self.assertEqual(utc_timestamp, got)
        got = timezone_utils.convert_timestamp_timezone(utc_timestamp,
                                                        invalid_offset_tz)
        self.assertEqual(utc_timestamp, got)

    def test_time_string_to_int(self):
        valid_time_string = '2019-10-07 07:51:37.857 UTC'
        expected = 1570434697857
        result = timezone_utils.time_string_to_int(valid_time_string)
        self.assertEqual(expected, result)

        valid_time_string_no_tz = '2019-10-07 07:51:37.857'
        expected = 1570434697857
        result = timezone_utils.time_string_to_int(valid_time_string_no_tz)
        self.assertEqual(expected, result)

        invalid_time_string_date = '2019-30-30 07:51:37.857 UTC'
        with self.assertRaises(ValueError):
            timezone_utils.time_string_to_int(invalid_time_string_date)

        invalid_time_string_fmt = '2019-10-07 07:51:37.857 BAD FMT'
        with self.assertRaises(ValueError):
            timezone_utils.time_string_to_int(invalid_time_string_fmt)

        invalid_time_string_fmt = 'UTC 2019-10-07 07:51:37.857'
        with self.assertRaises(ValueError):
            timezone_utils.time_string_to_int(invalid_time_string_fmt)

    def test_timestamp_to_ms(self):
        valid_pd_dt = pd.to_datetime('2019-10-07')
        expected = 1570406400000
        result = timezone_utils.timestamp_to_ms(valid_pd_dt)
        self.assertEqual(expected, result)

        invalid_dt = pd.to_datetime('')
        with self.assertRaises(ValueError):
            timezone_utils.timestamp_to_ms(invalid_dt)


if __name__ == '__main__':
    unittest.main()
