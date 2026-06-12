"""Tests for data_points_source."""

import copy
from typing import Any, Dict
import unittest
from unittest import mock

import apache_beam as beam
from apache_beam.testing.test_pipeline import TestPipeline
from apache_beam.testing.util import assert_that
from apache_beam.testing.util import equal_to
import pandas as pd

from verily.ds_sdk.core import options
from verily.ds_sdk.core import schemas
from verily.ds_sdk.core.io.bigquery import data_points_source
from verily.ds_sdk.core.io.bigquery import participant_mappings
from verily.ds_sdk.core.testing import test_data_points
from verily.ds_sdk.core.utils import timestamps


class FakeDsSdkCredentials:
    def get_credentials(self):
        return (mock.MagicMock(), None)


class FakeSchemaFetcher:
    def fetch_schema(self, table_id):
        del table_id


PD_TIMESTAMP = pd.Timestamp('2020-01-01')
BEAM_TIMESTAMP = timestamps.datetime_to_beam_timestamp(PD_TIMESTAMP)
DATA_POINT_METADATA = schemas.data_point_metadata_for_raw_data(
    # data_source_id=1302717404369281384,
    data_source_id=1621925824585309913,
    device_id='123',
    participant_id='321',
    participant_namespace=1,
    echo_metadata=schemas.EchoMetadata(
        bucket_start=BEAM_TIMESTAMP,
        bucket_write_time=BEAM_TIMESTAMP,
        deleted_time=None,
        snapshot_time=BEAM_TIMESTAMP,
    ),
    sensor_store_metadata=None,
    annotation_labels=set(),
)
DATA_POINT_METADATA_WITHOUT_ECHO_METADATA = (
    schemas.data_point_metadata_for_raw_data(  # pylint: disable=line-too-long
        data_source_id=6358860275287217292,
        device_id='123',
        participant_id='321',
        participant_namespace=1,
        echo_metadata=None,
        sensor_store_metadata=None,
        annotation_labels=set(),
    )
)


def create_echo_data_point_bq_row(
    data_point: Dict[str, Any], timestamp=PD_TIMESTAMP
) -> Dict[str, Any]:
    return {
        'DeviceID': '123',
        'UserID': 'UNUSED',
        'UserNamespace': 'UNUSED',
        'DataPointTime': timestamp,
        'SnapshotTime': timestamp,
        'BucketWriteTime': timestamp,
        'DeletedTime': None,
        'BucketStart': timestamp,
        'DataSource': {
            'name': 'data_source_name',
            'data_spec': {'field_specs': []},
        },
        'DataPoint': data_point,
    }


def create_echo_internal_data_point_bq_row(
    data_point: Dict[str, Any],
    data_source_id: int,
    timestamp=PD_TIMESTAMP,
    deleted_time=None,
) -> Dict[str, Any]:
    return {
        'DeviceID': '123',
        'DataPointWriteTime': timestamp,
        'DataPoint': data_point,
        'SnapshotTime': timestamp,
        'DataPointTime': timestamp,
        'DeletedTime': deleted_time,
        'DataSourceID': data_source_id,
    }


def create_custom_data_point_bq_row(
    data_point: Dict[str, Any], timestamp=PD_TIMESTAMP
) -> Dict[str, Any]:
    return {
        'DeviceID': '123',
        'UserID': 'UNUSED',
        'UserNamespace': 'UNUSED',
        'DataPointTime': timestamp,
        'DataSource': None,
        'DataPoint': data_point,
    }


def create_custom_data_point_bq_row_with_metadata(
    data_point: Dict[str, Any], timestamp=PD_TIMESTAMP, deleted_time=None
) -> Dict[str, Any]:
    return {
        'DeviceID': '123',
        'UserID': 'UNUSED',
        'UserNamespace': 'UNUSED',
        'DataPointTime': timestamp,
        'DataSource': None,
        'DataPoint': data_point,
        'BucketWriteTime': timestamp,
        'DeletedTime': deleted_time,
        'SnapshotTime': timestamp,
    }


def create_participant_mapping(start_timestamp, end_timestamp):
    return (
        '123',
        [
            participant_mappings.ParticipantInfo(
                '123',
                '321',
                1,
                timestamps.datetime_to_beam_timestamp(start_timestamp),
                timestamps.datetime_to_beam_timestamp(end_timestamp),
            )
        ],
    )


class DataPointsSourceTest(unittest.TestCase):
    @mock.patch(
        'verily.ds_sdk.core.gcp.bigquery_source_wrapper.GcpBigquerySourceWrapper'  # pylint: disable=line-too-long
    )
    @mock.patch(
        'verily.ds_sdk.core.io.bigquery.participant_mappings.BuildParticipantMappings'  # pylint: disable=line-too-long
    )
    @mock.patch('verily.ds_sdk.core.schema_fetcher.SchemaFetcher')
    def test_parse_echo_row(
        self, schema_fetcher_mock, participant_mock, bq_source_mock
    ):
        schema_fetcher_mock.return_value = FakeSchemaFetcher()
        pressure_bq_row = create_echo_data_point_bq_row(
            {'t': 'UNUSED', 'pressure': 987}
        )
        bq_source_mock.side_effect = [beam.Create([pressure_bq_row])]
        participant_mapping = create_participant_mapping(
            PD_TIMESTAMP, PD_TIMESTAMP + pd.Timedelta('1h')
        )
        participant_mock.side_effect = [beam.Create([participant_mapping])]

        expected_pressure = schemas.Pressure(
            DATA_POINT_METADATA,
            measurement_timestamp_utc=BEAM_TIMESTAMP,
            pressure=987,
        )

        with TestPipeline() as p:
            data_points, _ = p | data_points_source.DataPointRowSource(
                data_point_table_id='sensors-devteam.sensor_store_DevTeam.com_verily_pressure',  # pylint: disable=line-too-long
                participant_table_id='sensors-devteam.sensor_store_DevTeam.participant_associations',  # pylint: disable=line-too-long
                schema=schemas.Pressure,
                condition=None,
                source_options=options.BatchSourceOptions(),
                annotation_inner_join_options=None,
                incremental_query_options=None,
                creds=FakeDsSdkCredentials(),
                env='prod',
                billing_project='project',
                service_account='sa',
                bigquery_location='US',
                use_internal_echo=False,
            )

            assert_that(data_points, equal_to([expected_pressure]))

    @mock.patch(
        'verily.ds_sdk.core.gcp.bigquery_source_wrapper.GcpBigquerySourceWrapper'  # pylint: disable=line-too-long
    )
    @mock.patch(
        'verily.ds_sdk.core.io.bigquery.participant_mappings.BuildParticipantMappings'  # pylint: disable=line-too-long
    )
    @mock.patch('verily.ds_sdk.core.schema_fetcher.SchemaFetcher')
    def test_parse_echo_row_micros_timestamp(
        self, schema_fetcher_mock, participant_mock, bq_source_mock
    ):
        schema_fetcher_mock.return_value = FakeSchemaFetcher()
        user_tagged_event = create_echo_data_point_bq_row(
            {
                't': 'UNUSED',
                'app_ended_timestamp': BEAM_TIMESTAMP.micros,
                'app_started_timestamp': BEAM_TIMESTAMP.micros,
                'event_category': '',
                'event_end_timestamp': BEAM_TIMESTAMP.micros,
                'event_type': '',
                'is_auto_answered': None,
                'is_deleted': None,
                'question_prompt': None,
                'response': None,
                'responses': None,
                'survey_response_timestamp': [BEAM_TIMESTAMP.micros],
            },
            timestamp=BEAM_TIMESTAMP.micros,
        )
        bq_source_mock.side_effect = [beam.Create([user_tagged_event])]
        participant_mapping = create_participant_mapping(
            PD_TIMESTAMP, PD_TIMESTAMP + pd.Timedelta('1h')
        )
        participant_mock.side_effect = [beam.Create([participant_mapping])]

        expected_data = schemas.StudywatchUser_Tagged_Event(
            DATA_POINT_METADATA,
            measurement_timestamp_utc=BEAM_TIMESTAMP,
            app_ended_timestamp=BEAM_TIMESTAMP,
            app_started_timestamp=BEAM_TIMESTAMP,
            event_category='',
            event_end_timestamp=BEAM_TIMESTAMP,
            event_type='',
            is_auto_answered=None,
            is_deleted=None,
            question_prompt=None,
            response=None,
            responses=None,
            survey_response_timestamp=[BEAM_TIMESTAMP],
        )

        with TestPipeline() as p:
            data_points, _ = p | data_points_source.DataPointRowSource(
                data_point_table_id='sensors-devteam.sensor_store_DevTeam.com._verily_studywatch_user__tagged__event',  # pylint: disable=line-too-long
                participant_table_id='sensors-devteam.sensor_store_DevTeam.participant_associations',  # pylint: disable=line-too-long
                schema=schemas.StudywatchUser_Tagged_Event,
                condition=None,
                source_options=options.BatchSourceOptions(),
                annotation_inner_join_options=None,
                incremental_query_options=None,
                creds=FakeDsSdkCredentials(),
                env='prod',
                billing_project='project',
                service_account='sa',
                bigquery_location='US',
                use_internal_echo=False,
            )

            assert_that(data_points, equal_to([expected_data]))

    @mock.patch(
        'verily.ds_sdk.core.gcp.bigquery_source_wrapper.GcpBigquerySourceWrapper'  # pylint: disable=line-too-long
    )
    @mock.patch(
        'verily.ds_sdk.core.io.bigquery.participant_mappings.BuildParticipantMappings'  # pylint: disable=line-too-long
    )
    @mock.patch('verily.ds_sdk.core.schema_fetcher.SchemaFetcher')
    def test_parse_echo_row_data_point_timestamps(
        self, schema_fetcher_mock, participant_mock, bq_source_mock
    ):
        schema_fetcher_mock.return_value = FakeSchemaFetcher()
        user_tagged_event = create_echo_data_point_bq_row(
            {
                't': 'UNUSED',
                'app_ended_timestamp': PD_TIMESTAMP.to_pydatetime(),
                'app_started_timestamp': PD_TIMESTAMP.to_pydatetime(),
                'event_category': '',
                'event_end_timestamp': PD_TIMESTAMP.to_pydatetime(),
                'event_type': '',
                'is_auto_answered': None,
                'is_deleted': None,
                'question_prompt': None,
                'response': None,
                'responses': None,
                'survey_response_timestamp': [PD_TIMESTAMP.to_pydatetime()],
            }
        )
        bq_source_mock.side_effect = [beam.Create([user_tagged_event])]
        participant_mapping = create_participant_mapping(
            PD_TIMESTAMP, PD_TIMESTAMP + pd.Timedelta('1h')
        )
        participant_mock.side_effect = [beam.Create([participant_mapping])]

        expected_data = schemas.StudywatchUser_Tagged_Event(
            DATA_POINT_METADATA,
            measurement_timestamp_utc=BEAM_TIMESTAMP,
            app_ended_timestamp=BEAM_TIMESTAMP,
            app_started_timestamp=BEAM_TIMESTAMP,
            event_category='',
            event_end_timestamp=BEAM_TIMESTAMP,
            event_type='',
            is_auto_answered=None,
            is_deleted=None,
            question_prompt=None,
            response=None,
            responses=None,
            survey_response_timestamp=[BEAM_TIMESTAMP],
        )

        with TestPipeline() as p:
            data_points, _ = p | data_points_source.DataPointRowSource(
                data_point_table_id='sensors-devteam.sensor_store_DevTeam.com._verily_studywatch_user__tagged__event',  # pylint: disable=line-too-long
                participant_table_id='sensors-devteam.sensor_store_DevTeam.participant_associations',  # pylint: disable=line-too-long
                schema=schemas.StudywatchUser_Tagged_Event,
                condition=None,
                source_options=options.BatchSourceOptions(),
                annotation_inner_join_options=None,
                incremental_query_options=None,
                creds=FakeDsSdkCredentials(),
                env='prod',
                billing_project='project',
                service_account='sa',
                bigquery_location='US',
                use_internal_echo=False,
            )

            assert_that(data_points, equal_to([expected_data]))

    @mock.patch(
        'verily.ds_sdk.core.gcp.bigquery_source_wrapper.GcpBigquerySourceWrapper'  # pylint: disable=line-too-long
    )
    @mock.patch(
        'verily.ds_sdk.core.io.bigquery.participant_mappings.BuildParticipantMappings'  # pylint: disable=line-too-long
    )
    @mock.patch('verily.ds_sdk.core.schema_fetcher.SchemaFetcher')
    def test_no_device_participant_association(
        self, schema_fetcher_mock, participant_mock, bq_source_mock
    ):
        schema_fetcher_mock.return_value = FakeSchemaFetcher()
        pressure_bq_row = create_echo_data_point_bq_row(
            {'t': 'UNUSED', 'pressure': 987}
        )
        bq_source_mock.side_effect = [beam.Create([pressure_bq_row])]
        # No participant -> data should be dropped.
        participant_mock.side_effect = [beam.Create([])]

        no_participant = copy.deepcopy(DATA_POINT_METADATA)
        no_participant.participant_id = None
        no_participant.participant_namespace = None
        expected_pressure = schemas.Pressure(
            no_participant,
            measurement_timestamp_utc=BEAM_TIMESTAMP,
            pressure=987,
        )

        with TestPipeline() as p:
            data_points, _ = p | data_points_source.DataPointRowSource(
                data_point_table_id='sensors-devteam.sensor_store_DevTeam.com_verily_pressure',  # pylint: disable=line-too-long
                participant_table_id='sensors-devteam.sensor_store_DevTeam.participant_associations',  # pylint: disable=line-too-long
                schema=schemas.Pressure,
                condition=None,
                source_options=options.BatchSourceOptions(),
                annotation_inner_join_options=None,
                incremental_query_options=None,
                creds=FakeDsSdkCredentials(),
                env='prod',
                billing_project='project',
                service_account='sa',
                bigquery_location='US',
                use_internal_echo=False,
            )

            assert_that(data_points, equal_to([expected_pressure]))

    @mock.patch(
        'verily.ds_sdk.core.gcp.bigquery_source_wrapper.GcpBigquerySourceWrapper'  # pylint: disable=line-too-long
    )
    @mock.patch(
        'verily.ds_sdk.core.io.bigquery.participant_mappings.BuildParticipantMappings'  # pylint: disable=line-too-long
    )
    @mock.patch('verily.ds_sdk.core.schema_fetcher.SchemaFetcher')
    def test_no_device_time_participant_association(
        self, schema_fetcher_mock, participant_mock, bq_source_mock
    ):
        schema_fetcher_mock.return_value = FakeSchemaFetcher()
        pressure_bq_row = create_echo_data_point_bq_row(
            {'t': 'UNUSED', 'pressure': 987}
        )
        bq_source_mock.side_effect = [beam.Create([pressure_bq_row])]
        participant_mapping = create_participant_mapping(
            PD_TIMESTAMP + pd.Timedelta('1d'), PD_TIMESTAMP + pd.Timedelta('2d')
        )
        participant_mock.side_effect = [beam.Create([participant_mapping])]

        no_participant = copy.deepcopy(DATA_POINT_METADATA)
        no_participant.participant_id = None
        no_participant.participant_namespace = None
        expected_pressure = schemas.Pressure(
            no_participant,
            measurement_timestamp_utc=BEAM_TIMESTAMP,
            pressure=987,
        )

        with TestPipeline() as p:
            data_points, _ = p | data_points_source.DataPointRowSource(
                data_point_table_id='sensors-devteam.sensor_store_DevTeam.com_verily_pressure',  # pylint: disable=line-too-long
                participant_table_id='sensors-devteam.sensor_store_DevTeam.participant_associations',  # pylint: disable=line-too-long
                schema=schemas.Pressure,
                condition=None,
                source_options=options.BatchSourceOptions(),
                annotation_inner_join_options=None,
                incremental_query_options=None,
                creds=FakeDsSdkCredentials(),
                env='prod',
                billing_project='project',
                service_account='sa',
                bigquery_location='US',
                use_internal_echo=False,
            )

            assert_that(data_points, equal_to([expected_pressure]))

    @mock.patch(
        'verily.ds_sdk.core.gcp.bigquery_source_wrapper.GcpBigquerySourceWrapper'  # pylint: disable=line-too-long
    )
    @mock.patch(
        'verily.ds_sdk.core.io.bigquery.participant_mappings.BuildParticipantMappings'  # pylint: disable=line-too-long
    )
    @mock.patch('verily.ds_sdk.core.schema_fetcher.SchemaFetcher')
    def test_parse_custom_row(
        self, schema_fetcher_mock, participant_mock, bq_source_mock
    ):
        schema_fetcher_mock.return_value = FakeSchemaFetcher()
        custom_bq_row = create_custom_data_point_bq_row({'custom_field': 1337})
        bq_source_mock.side_effect = [beam.Create([custom_bq_row])]
        participant_mapping = create_participant_mapping(
            PD_TIMESTAMP, PD_TIMESTAMP + pd.Timedelta('1h')
        )
        participant_mock.side_effect = [beam.Create([participant_mapping])]

        expected_data_point = test_data_points.CustomDataPoint(
            DATA_POINT_METADATA_WITHOUT_ECHO_METADATA,
            measurement_timestamp_utc=BEAM_TIMESTAMP,
            custom_field=1337,
        )

        with beam.Pipeline(runner='direct') as p:
            data_points, _ = p | data_points_source.DataPointRowSource(
                data_point_table_id='custom_project.custom_dataset.custom_table',  # pylint: disable=line-too-long
                participant_table_id='sensors-devteam.sensor_store_DevTeam.participant_associations',  # pylint: disable=line-too-long
                schema=test_data_points.CustomDataPoint,
                condition=None,
                source_options=options.BatchSourceOptions(
                    remove_duplicates=False
                ),
                annotation_inner_join_options=None,
                incremental_query_options=None,
                creds=FakeDsSdkCredentials(),
                env='prod',
                billing_project='project',
                service_account='sa',
                bigquery_location='US',
                use_internal_echo=False,
            )

            assert_that(data_points, equal_to([expected_data_point]))

    @mock.patch(
        'verily.ds_sdk.core.gcp.bigquery_source_wrapper.GcpBigquerySourceWrapper'  # pylint: disable=line-too-long
    )
    @mock.patch(
        'verily.ds_sdk.core.io.bigquery.participant_mappings.BuildParticipantMappings'  # pylint: disable=line-too-long
    )
    @mock.patch('verily.ds_sdk.core.schema_fetcher.SchemaFetcher')
    def test_parse_custom_row_deleted(
        self, schema_fetcher_mock, participant_mock, bq_source_mock
    ):
        schema_fetcher_mock.return_value = FakeSchemaFetcher()
        custom_bq_row_deleted = create_custom_data_point_bq_row_with_metadata(
            {'custom_field': None}, deleted_time=PD_TIMESTAMP
        )
        bq_source_mock.side_effect = [beam.Create([custom_bq_row_deleted])]
        participant_mapping = create_participant_mapping(
            PD_TIMESTAMP, PD_TIMESTAMP + pd.Timedelta('1h')
        )
        participant_mock.side_effect = [beam.Create([participant_mapping])]

        with beam.Pipeline(runner='direct') as p:
            data_points, _ = p | data_points_source.DataPointRowSource(
                data_point_table_id='custom_project.custom_dataset.custom_table',  # pylint: disable=line-too-long
                participant_table_id='sensors-devteam.sensor_store_DevTeam.participant_associations',  # pylint: disable=line-too-long
                schema=test_data_points.CustomDataPoint,
                condition=None,
                source_options=options.BatchSourceOptions(
                    remove_duplicates=True
                ),
                annotation_inner_join_options=None,
                incremental_query_options=None,
                creds=FakeDsSdkCredentials(),
                env='prod',
                billing_project='project',
                service_account='sa',
                bigquery_location='US',
                use_internal_echo=False,
            )

            # Custom row is removed from output due to Deleted timestamp
            assert_that(data_points, equal_to([]))

    @mock.patch(
        'verily.ds_sdk.core.gcp.bigquery_source_wrapper.GcpBigquerySourceWrapper'  # pylint: disable=line-too-long
    )
    @mock.patch(
        'verily.ds_sdk.core.io.bigquery.participant_mappings.BuildParticipantMappings'  # pylint: disable=line-too-long
    )
    @mock.patch('verily.ds_sdk.core.schema_fetcher.SchemaFetcher')
    def test_parse_echo_internal_row(
        self, schema_fetcher_mock, participant_mock, bq_source_mock
    ):
        schema_fetcher_mock.return_value = FakeSchemaFetcher()
        pressure_ds_row = {
            'DataSourceID': DATA_POINT_METADATA.data_source_id,
            'DataSource': {
                'name': 'data_source_name',
                'data_spec': {'field_specs': []},
            },
        }
        pressure_dp_row = create_echo_internal_data_point_bq_row(
            {'pressure': 987}, DATA_POINT_METADATA.data_source_id
        )
        bq_source_mock.side_effect = [
            # DataSources are queried / parsed before DataPoints
            beam.Create([pressure_ds_row]),
            beam.Create([pressure_dp_row]),
        ]
        participant_mapping = create_participant_mapping(
            PD_TIMESTAMP, PD_TIMESTAMP + pd.Timedelta('1h')
        )
        participant_mock.side_effect = [beam.Create([participant_mapping])]

        expected_pressure = schemas.Pressure(
            DATA_POINT_METADATA,
            measurement_timestamp_utc=BEAM_TIMESTAMP,
            pressure=987,
        )

        with TestPipeline() as p:
            data_points, _ = p | data_points_source.DataPointRowSource(
                data_point_table_id='sensors-devteam.internal_DevTeam.com_verily_pressure',  # pylint: disable=line-too-long
                participant_table_id='sensors-devteam.sensor_store_DevTeam.participant_associations',  # pylint: disable=line-too-long
                schema=schemas.Pressure,
                condition=None,
                source_options=options.BatchSourceOptions(),
                annotation_inner_join_options=None,
                incremental_query_options=None,
                creds=FakeDsSdkCredentials(),
                env='prod',
                billing_project='project',
                service_account='sa',
                bigquery_location='US',
                data_source_mappings_table_id='sensors-devteam.internal_DevTeam.data_source_mappings',  # pylint: disable=line-too-long
                data_spec_name='com.verily.pressure',
                use_internal_echo=True,
            )

            assert_that(data_points, equal_to([expected_pressure]))

    @mock.patch(
        'verily.ds_sdk.core.gcp.bigquery_source_wrapper.GcpBigquerySourceWrapper'  # pylint: disable=line-too-long
    )
    @mock.patch(
        'verily.ds_sdk.core.io.bigquery.participant_mappings.BuildParticipantMappings'  # pylint: disable=line-too-long
    )
    @mock.patch('verily.ds_sdk.core.schema_fetcher.SchemaFetcher')
    def test_parse_echo_internal_deleted_row(
        self, schema_fetcher_mock, participant_mock, bq_source_mock
    ):
        schema_fetcher_mock.return_value = FakeSchemaFetcher()
        pressure_ds_row = {
            'DataSourceID': DATA_POINT_METADATA.data_source_id,
            'DataSource': {
                'name': 'data_source_name',
                'data_spec': {'field_specs': []},
            },
        }
        pressure_dp_row = create_echo_internal_data_point_bq_row(
            {'pressure': None},
            DATA_POINT_METADATA.data_source_id,
            deleted_time=PD_TIMESTAMP,
        )
        bq_source_mock.side_effect = [
            # DataSources are queried / parsed before DataPoints
            beam.Create([pressure_ds_row]),
            beam.Create([pressure_dp_row]),
        ]
        participant_mapping = create_participant_mapping(
            PD_TIMESTAMP, PD_TIMESTAMP + pd.Timedelta('1h')
        )
        participant_mock.side_effect = [beam.Create([participant_mapping])]

        with TestPipeline() as p:
            data_points, _ = p | data_points_source.DataPointRowSource(
                data_point_table_id='sensors-devteam.internal_DevTeam.com_verily_pressure',  # pylint: disable=line-too-long
                participant_table_id='sensors-devteam.sensor_store_DevTeam.participant_associations',  # pylint: disable=line-too-long
                schema=schemas.Pressure,
                condition=None,
                source_options=options.BatchSourceOptions(),
                annotation_inner_join_options=None,
                incremental_query_options=None,
                creds=FakeDsSdkCredentials(),
                env='prod',
                billing_project='project',
                service_account='sa',
                bigquery_location='US',
                data_source_mappings_table_id='sensors-devteam.internal_DevTeam.data_source_mappings',  # pylint: disable=line-too-long
                data_spec_name='com.verily.pressure',
                use_internal_echo=True,
            )

            # Deleted DataPoint containing null values is filtered out
            assert_that(data_points, equal_to([]))


if __name__ == '__main__':
    unittest.main()
