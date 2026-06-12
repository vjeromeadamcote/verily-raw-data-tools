"""Tests for rialto_timezone_fix.py."""

import unittest

import apache_beam as beam
from apache_beam.testing.test_pipeline import TestPipeline
from apache_beam.testing.util import assert_that
from apache_beam.testing.util import equal_to
import pandas as pd

from verily.ds_sdk.contrib import rialto_timezone_fix
from verily.ds_sdk.core.utils import timestamps
from verily.ds_sdk.protos import types_pb2


def build_data_source(device_id: str, timezone: str):
    return types_pb2.DataSource(device=types_pb2.Device(
        serial_number=device_id, time_zone_name=timezone))


class RialtoTimezoneFixTest(unittest.TestCase):

    def test_build_timezone_map(self):
        input_data = [
            (timestamps.datetime_to_beam_timestamp(
                pd.Timestamp('2020-01-01', tz='UTC')),
             build_data_source('device1', 'US/Pacific')),
            (timestamps.datetime_to_beam_timestamp(
                pd.Timestamp('2020-01-01', tz='UTC')),
             build_data_source('device1', 'US/Pacific')),
            (timestamps.datetime_to_beam_timestamp(
                pd.Timestamp('2020-01-01', tz='UTC')),
             build_data_source('device1', 'US/Eastern')),
            (timestamps.datetime_to_beam_timestamp(
                pd.Timestamp('2020-01-01',
                             tz='UTC')), build_data_source('device2',
                                                           '-05:00')),
            (timestamps.datetime_to_beam_timestamp(
                pd.Timestamp('2020-01-01',
                             tz='UTC')), build_data_source('device2',
                                                           '-05:00')),
            (timestamps.datetime_to_beam_timestamp(
                pd.Timestamp('2020-01-01',
                             tz='UTC')), build_data_source('device2',
                                                           '-06:00')),
        ]
        expected_map = rialto_timezone_fix.UtcOffsetMap()
        expected_map.add_utc_offset('device1', -28800.0)
        expected_map.add_utc_offset('device2', -18000.0)

        with TestPipeline() as p:
            output = (p | beam.Create(input_data) |
                      rialto_timezone_fix.BuildMostCommonUtcOffsetMap())

            assert_that(output, equal_to([expected_map]))

    def test_build_timezone_map_readable(self):
        input_data = [
            (timestamps.datetime_to_beam_timestamp(
                pd.Timestamp('2020-01-01', tz='UTC')),
             build_data_source('device1', 'US/Pacific')),
            (timestamps.datetime_to_beam_timestamp(
                pd.Timestamp('2020-01-01', tz='UTC')),
             build_data_source('device1', 'US/Pacific')),
            (timestamps.datetime_to_beam_timestamp(
                pd.Timestamp('2020-01-01', tz='UTC')),
             build_data_source('device1', 'US/Eastern')),
            (timestamps.datetime_to_beam_timestamp(
                pd.Timestamp('2020-01-01',
                             tz='UTC')), build_data_source('device2',
                                                           '-05:00')),
            (timestamps.datetime_to_beam_timestamp(
                pd.Timestamp('2020-01-01',
                             tz='UTC')), build_data_source('device2',
                                                           '-05:00')),
            (timestamps.datetime_to_beam_timestamp(
                pd.Timestamp('2020-01-01',
                             tz='UTC')), build_data_source('device2',
                                                           '-06:00')),
        ]
        expected_map = rialto_timezone_fix.UtcOffsetMap()
        expected_map.add_utc_offset('device1', 'America/Los_Angeles')
        expected_map.add_utc_offset('device2', 'America/New_York')

        with TestPipeline() as p:
            output = (p | beam.Create(input_data) |
                      rialto_timezone_fix.BuildMostCommonUtcOffsetMap(
                          return_readable_timezone=True))

            assert_that(output, equal_to([expected_map]))

    def test_invalid_timezone_readable(self):
        input_data = [
            (timestamps.datetime_to_beam_timestamp(
                pd.Timestamp('2020-01-01',
                             tz='UTC')), build_data_source('device1',
                                                           '-07:30')),
            (timestamps.datetime_to_beam_timestamp(
                pd.Timestamp('2020-01-01',
                             tz='UTC')), build_data_source('device1',
                                                           '-07:30')),
        ]

        error_msg_regex = 'did not have a readable timezone mapping.'
        with self.assertRaisesRegex(ValueError, error_msg_regex):
            with TestPipeline() as p:
                _ = (p | beam.Create(input_data) |
                     rialto_timezone_fix.BuildMostCommonUtcOffsetMap(
                         return_readable_timezone=True))


if __name__ == '__main__':
    unittest.main()
