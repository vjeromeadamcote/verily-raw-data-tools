"""Tests for verily.raw_data_tools.utils.timezone_utils.

Ported from verily.ds_sdk.core.utils.timezone_utils_test.
"""

import datetime
import unittest

import pandas as pd
import pytz

from verily.raw_data_tools.utils import timezone_utils


class TimezoneUtilsTest(unittest.TestCase):

    def test_get_date_in_timezone(self):
        date_time = pd.to_datetime('2019-09-30 00:10:10.125 UTC')
        dt_millis = date_time.value // 10**6
        utc_timestamp = pd.Timestamp(dt_millis, unit='ms', tz='UTC')

        self.assertEqual(
            '2019-09-29',
            timezone_utils.get_date_in_timezone(utc_timestamp, 'US/Pacific'))
        self.assertEqual(
            '2019-09-30',
            timezone_utils.get_date_in_timezone(utc_timestamp, 'Area/51'))
        self.assertEqual(
            '2019-09-30',
            timezone_utils.get_date_in_timezone(utc_timestamp, '+05:00'))
        self.assertEqual(
            '2019-09-29',
            timezone_utils.get_date_in_timezone(utc_timestamp, '-07:00'))
        self.assertEqual(
            '2019-09-30',
            timezone_utils.get_date_in_timezone(utc_timestamp, '30:00'))
        self.assertEqual(
            '2019-09-30',
            timezone_utils.get_date_in_timezone(utc_timestamp, '-80:00'))
        self.assertEqual(
            '2019-09-30',
            timezone_utils.get_date_in_timezone(utc_timestamp, '00:15'))
        self.assertEqual(
            '2019-09-29',
            timezone_utils.get_date_in_timezone(utc_timestamp, '-00:15'))
        self.assertEqual(
            '2019-09-30',
            timezone_utils.get_date_in_timezone(utc_timestamp, '00:80'))
        self.assertEqual(
            '2019-09-30',
            timezone_utils.get_date_in_timezone(utc_timestamp, '-00:80'))
        self.assertEqual(
            '2019-09-30',
            timezone_utils.get_date_in_timezone(utc_timestamp, '00:00'))
        self.assertEqual(
            '2019-09-30',
            timezone_utils.get_date_in_timezone(utc_timestamp, ''))

    def test_convert_timezone_named(self):
        utc_timestamp = pd.Timestamp('2019-10-07 10:00:00', tz='UTC')
        self.assertEqual(
            'US/Pacific',
            timezone_utils.convert_timezone(utc_timestamp, 'US/Pacific'))

    def test_convert_timezone_offset(self):
        utc_timestamp = pd.Timestamp('2019-10-07 10:00:00', tz='UTC')
        result = timezone_utils.convert_timezone(utc_timestamp, '-07:00')
        result_offset = result.utcoffset(None)
        expected_offset = datetime.timedelta(hours=-7)
        self.assertEqual(expected_offset, result_offset)

    def test_convert_timezone_invalid(self):
        utc_timestamp = pd.Timestamp('2019-10-07 10:00:00', tz='UTC')
        self.assertEqual(
            'UTC',
            timezone_utils.convert_timezone(utc_timestamp, 'Narnia'))
        self.assertEqual(
            'UTC',
            timezone_utils.convert_timezone(utc_timestamp, '-30:00'))

    def test_convert_timestamp_timezone(self):
        utc_timestamp = pd.Timestamp('2019-10-07 10:00:00', tz='UTC')
        expected = pd.Timestamp('2019-10-07 03:00:00', tz='US/Pacific')

        got = timezone_utils.convert_timestamp_timezone(
            utc_timestamp, 'US/Pacific')
        self.assertEqual(expected, got)
        got = timezone_utils.convert_timestamp_timezone(
            utc_timestamp, '-07:00')
        self.assertEqual(expected, got)

        # Invalid timezones default to UTC.
        got = timezone_utils.convert_timestamp_timezone(
            utc_timestamp, 'Narnia')
        self.assertEqual(utc_timestamp, got)
        got = timezone_utils.convert_timestamp_timezone(
            utc_timestamp, '-30:00')
        self.assertEqual(utc_timestamp, got)

    def test_time_string_to_int(self):
        self.assertEqual(
            1570434697857,
            timezone_utils.time_string_to_int('2019-10-07 07:51:37.857 UTC'))
        self.assertEqual(
            1570434697857,
            timezone_utils.time_string_to_int('2019-10-07 07:51:37.857'))

        with self.assertRaises(ValueError):
            timezone_utils.time_string_to_int('2019-30-30 07:51:37.857 UTC')
        with self.assertRaises(ValueError):
            timezone_utils.time_string_to_int(
                '2019-10-07 07:51:37.857 BAD FMT')
        with self.assertRaises(ValueError):
            timezone_utils.time_string_to_int('UTC 2019-10-07 07:51:37.857')

    def test_timestamp_to_ms(self):
        self.assertEqual(
            1570406400000,
            timezone_utils.timestamp_to_ms(pd.to_datetime('2019-10-07')))
        with self.assertRaises(ValueError):
            timezone_utils.timestamp_to_ms(pd.to_datetime(''))


if __name__ == '__main__':
    unittest.main()
