"""Tests for participant_mappings."""

import unittest
from unittest import mock

import apache_beam as beam
from apache_beam.testing.test_pipeline import TestPipeline
from apache_beam.testing.util import assert_that
from apache_beam.testing.util import equal_to
import pandas as pd

from verily.ds_sdk.core.io.bigquery import participant_mappings
from verily.ds_sdk.core.utils import timestamps


class FakeDsSdkCredentials:

    def get_credentials(self):
        return (mock.MagicMock(), None)


class ParticipantMappingsTest(unittest.TestCase):

    def setUp(self) -> None:
        super().setUp()
        self._participant_info_list = [
            participant_mappings.ParticipantInfo(
                'device_id', 'participant_id_1', 1,
                timestamps.datetime_to_beam_timestamp(
                    pd.Timestamp('2020-01-01')),
                timestamps.datetime_to_beam_timestamp(
                    pd.Timestamp('2020-01-03'))),
            participant_mappings.ParticipantInfo(
                'device_id', 'participant_id_2', 1,
                timestamps.datetime_to_beam_timestamp(
                    pd.Timestamp('2020-01-03')),
                timestamps.datetime_to_beam_timestamp(
                    pd.Timestamp('2020-01-05'))),
            participant_mappings.ParticipantInfo(
                'device_id', 'participant_id_3', 1,
                timestamps.datetime_to_beam_timestamp(
                    pd.Timestamp('2020-01-05')),
                timestamps.datetime_to_beam_timestamp(
                    pd.Timestamp('2020-01-10'))),
            participant_mappings.ParticipantInfo(
                'device_id', 'participant_id_4', 1,
                timestamps.datetime_to_beam_timestamp(
                    pd.Timestamp('2022-01-01 00:00:00')),
                timestamps.datetime_to_beam_timestamp(
                    pd.Timestamp('2022-01-02 00:00:00'))),
            participant_mappings.ParticipantInfo(
                'device_id', 'participant_id_4', 1,
                timestamps.datetime_to_beam_timestamp(
                    pd.Timestamp('2022-01-02 00:00:01')),
                timestamps.datetime_to_beam_timestamp(
                    pd.Timestamp('2022-01-04 00:00:00'))),
        ]

    def test_get_participant_info_at_timestamp(self):
        # Tests start time inclusive
        got = participant_mappings.get_participant_info_at_timestamp(
            'device_id',
            timestamps.datetime_to_beam_timestamp(pd.Timestamp('2020-01-01')),
            self._participant_info_list)
        self.assertEqual(got, self._participant_info_list[0])

        # Tests simple case
        got = participant_mappings.get_participant_info_at_timestamp(
            'device_id',
            timestamps.datetime_to_beam_timestamp(pd.Timestamp('2020-01-04')),
            self._participant_info_list)
        self.assertEqual(got, self._participant_info_list[1])

        # Tests end time exclusive
        got = participant_mappings.get_participant_info_at_timestamp(
            'device_id',
            timestamps.datetime_to_beam_timestamp(pd.Timestamp('2020-01-05')),
            self._participant_info_list)
        self.assertEqual(got, self._participant_info_list[2])

        # Tests empty associations return None
        got = participant_mappings.get_participant_info_at_timestamp(
            'device_id',
            timestamps.datetime_to_beam_timestamp(pd.Timestamp('2020-01-05')),
            # No assocaitions passed.
            [])
        self.assertIsNone(got)

        # Tests missing associations @ timestamp return None
        got = participant_mappings.get_participant_info_at_timestamp(
            'device_id',
            # No association at this time.
            timestamps.datetime_to_beam_timestamp(pd.Timestamp('2020-02-05')),
            self._participant_info_list)
        self.assertIsNone(got)

    def test_get_participant_info_at_timestamp_throws_error(self):
        # Tests the case where device ids don't match participant table.
        with self.assertRaises(ValueError):
            participant_mappings.get_participant_info_at_timestamp(
                'mismatched_device_id',
                timestamps.datetime_to_beam_timestamp(
                    pd.Timestamp('2020-01-05')), self._participant_info_list)

    def test_get_participant_info_within_time_range(self):
        # Tests start time before participant interval
        #          |p_start------------p_end|
        #   |start----------end|
        got = participant_mappings.get_participant_info_within_time_range(
            'device_id',
            timestamps.datetime_to_beam_timestamp(pd.Timestamp('2019-12-20')),
            timestamps.datetime_to_beam_timestamp(pd.Timestamp('2020-01-02')),
            self._participant_info_list)
        self.assertEqual(got, self._participant_info_list[0:1])

        # Tests intervals match exactly
        #   |p_start------p_end|
        #   |start----------end|
        got = participant_mappings.get_participant_info_within_time_range(
            'device_id',
            timestamps.datetime_to_beam_timestamp(pd.Timestamp('2020-01-03')),
            timestamps.datetime_to_beam_timestamp(pd.Timestamp('2020-01-05')),
            self._participant_info_list)
        self.assertEqual(got, self._participant_info_list[1:2])

        # Tests interval contained
        #   |p_start-----------------------p_end|
        #            |start----------end|
        got = participant_mappings.get_participant_info_within_time_range(
            'device_id',
            timestamps.datetime_to_beam_timestamp(pd.Timestamp('2020-01-06')),
            timestamps.datetime_to_beam_timestamp(pd.Timestamp('2020-01-07')),
            self._participant_info_list)
        self.assertEqual(got, self._participant_info_list[2:3])

        # Tests end time after participant interval
        #   |p_start--------p_end|
        #             |start----------end|
        got = participant_mappings.get_participant_info_within_time_range(
            'device_id',
            timestamps.datetime_to_beam_timestamp(pd.Timestamp('2020-01-07')),
            timestamps.datetime_to_beam_timestamp(pd.Timestamp('2020-01-20')),
            self._participant_info_list)
        self.assertEqual(got, self._participant_info_list[2:3])

        # Tests no overlap
        #   |p_start----p_end|
        #                       |start------end|
        got = participant_mappings.get_participant_info_within_time_range(
            'device_id',
            # No association at this time.
            timestamps.datetime_to_beam_timestamp(pd.Timestamp('2020-01-10')),
            timestamps.datetime_to_beam_timestamp(pd.Timestamp('2020-01-11')),
            self._participant_info_list)
        self.assertIsNone(got)

        got = participant_mappings.get_participant_info_within_time_range(
            'device_id',
            # Crosses a small gap in participant associations that should be
            # ignored.
            timestamps.datetime_to_beam_timestamp(pd.Timestamp('2022-01-01')),
            timestamps.datetime_to_beam_timestamp(pd.Timestamp('2022-01-11')),
            self._participant_info_list)
        self.assertEqual(got, self._participant_info_list[4:5])

    def test_get_participant_info_within_time_range_multiple_participants(self):
        # Tests multiple non-overlapping participants, same time range
        #  |p_start----p_end||p_start----p_end||p_start----p_end|
        #  |start--------------------------------------------end|
        got = participant_mappings.get_participant_info_within_time_range(
            'device_id',
            # Crosses a small gap in participant associations that should be
            # ignored.
            timestamps.datetime_to_beam_timestamp(pd.Timestamp('2020-01-01')),
            timestamps.datetime_to_beam_timestamp(pd.Timestamp('2020-01-11')),
            self._participant_info_list)
        self.assertEqual(got, self._participant_info_list[:3])

    def test_get_participant_info_within_time_range_throws_error(self):
        # Tests the case where device ids don't match participant table.
        with self.assertRaises(ValueError):
            participant_mappings.get_participant_info_within_time_range(
                'mismatched_device_id',
                timestamps.datetime_to_beam_timestamp(
                    pd.Timestamp('2020-01-06')),
                timestamps.datetime_to_beam_timestamp(
                    pd.Timestamp('2020-01-07')), self._participant_info_list)

        # Tests interval spans multiple overlapping participant_associations
        #   |p_start--------p_end|
        #                    |p_start--------p_end|
        #        |start----------------end|
        with self.assertRaises(ValueError):
            overlapping_participant_list = self._participant_info_list + [
                participant_mappings.ParticipantInfo(
                    'device_id', 'participant_id_3', 1,
                    timestamps.datetime_to_beam_timestamp(
                        pd.Timestamp('2020-01-04')),
                    timestamps.datetime_to_beam_timestamp(
                        pd.Timestamp('2020-01-05')))
            ]

            participant_mappings.get_participant_info_within_time_range(
                'device_id',
                timestamps.datetime_to_beam_timestamp(
                    pd.Timestamp('2020-01-03')),
                timestamps.datetime_to_beam_timestamp(
                    pd.Timestamp('2020-01-05')), overlapping_participant_list)

    @mock.patch(
        'verily.ds_sdk.core.gcp.bigquery_source_wrapper.GcpBigquerySourceWrapper'  # pylint: disable=line-too-long
    )
    def test_build_participant_mappings(self, bq_source_mock):
        bq_source_mock.side_effect = [
            beam.Create([
                {
                    'DeviceId': 'device_id',
                    'ParticipantId': 'participant_id',
                    'ParticipantNamespace': 'GAIA',
                    'StartTime': pd.Timestamp('2020-02-01'),
                    'EndTime': pd.Timestamp('2020-02-02'),
                },
                {
                    'DeviceId': 'device_id',
                    'ParticipantId': 'participant_id',
                    'ParticipantNamespace': 'GAIA',
                    'StartTime': pd.Timestamp('2020-02-03'),
                    'EndTime': pd.Timestamp('2020-02-04'),
                },
                {
                    'DeviceId': 'device_id',
                    'ParticipantId': 'participant_id',
                    'ParticipantNamespace': 'GAIA',
                    'StartTime': pd.Timestamp('2020-02-04'),
                    'EndTime': pd.Timestamp('2020-02-05'),
                },
                {
                    'DeviceId': 'device_id',
                    'ParticipantId': 'participant_id2',
                    'ParticipantNamespace': 'GAIA',
                    'StartTime': pd.Timestamp('2020-02-02'),
                    'EndTime': pd.Timestamp('2020-02-03'),
                },
            ])
        ]

        expected_participant_mappings = ('device_id', [
            participant_mappings.ParticipantInfo(
                'device_id', 'participant_id', 1,
                timestamps.datetime_to_beam_timestamp(
                    pd.Timestamp('2020-02-01')),
                timestamps.datetime_to_beam_timestamp(
                    pd.Timestamp('2020-02-02'))),
            participant_mappings.ParticipantInfo(
                'device_id', 'participant_id', 1,
                timestamps.datetime_to_beam_timestamp(
                    pd.Timestamp('2020-02-03')),
                timestamps.datetime_to_beam_timestamp(
                    pd.Timestamp('2020-02-05'))),
            participant_mappings.ParticipantInfo(
                'device_id', 'participant_id2', 1,
                timestamps.datetime_to_beam_timestamp(
                    pd.Timestamp('2020-02-02')),
                timestamps.datetime_to_beam_timestamp(
                    pd.Timestamp('2020-02-03')))
        ])

        with TestPipeline() as p:
            got_participant_mappings = (
                p | participant_mappings.BuildParticipantMappings(
                    'bigquery_table_id', 'project_id', 'service_account',
                    FakeDsSdkCredentials(), 'US'))

            assert_that(got_participant_mappings,
                        equal_to([expected_participant_mappings]))


if __name__ == '__main__':
    unittest.main()
