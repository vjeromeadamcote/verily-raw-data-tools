"""Tests for annotation_source."""

from copy import deepcopy
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
from verily.ds_sdk.core.io.bigquery import annotation_source
from verily.ds_sdk.core.io.bigquery import participant_mappings
from verily.ds_sdk.core.utils import timestamps


class FakeDsSdkCredentials:

    def get_credentials(self):
        return (mock.MagicMock(), None)


class FakeSchemaFetcher:

    def fetch_schema(self, table_id):
        del table_id


PD_START_TIMESTAMP = pd.Timestamp('2020-01-01')
BEAM_START_TIMESTAMP = timestamps.datetime_to_beam_timestamp(PD_START_TIMESTAMP)
PD_END_TIMESTAMP = pd.Timestamp('2020-01-02')
BEAM_END_TIMESTAMP = timestamps.datetime_to_beam_timestamp(PD_END_TIMESTAMP)
PD_END_TIMESTAMP2 = pd.Timestamp('2020-01-03')
BEAM_END_TIMESTAMP2 = timestamps.datetime_to_beam_timestamp(PD_END_TIMESTAMP2)

ANNOTATION_METADATA = schemas.AnnotationMetadata(device_id='123',
                                                 participant_id='321',
                                                 participant_namespace=1,
                                                 version_name='version_name',
                                                 version_number=1,
                                                 input_data_info=[])
ANNOTATION_METADATA2 = schemas.AnnotationMetadata(device_id='123',
                                                  participant_id='654',
                                                  participant_namespace=1,
                                                  version_name='version_name',
                                                  version_number=1,
                                                  input_data_info=[])


def create_annotation_bq_row(label,
                             start_timestamp=PD_START_TIMESTAMP,
                             end_timestamp=PD_END_TIMESTAMP) -> Dict[str, Any]:
    return {
        'device_id': '123',
        'user_id': 'UNUSED',
        'user_namespace': 'UNUSED',
        'start_timestamp_utc': start_timestamp,
        'end_timestamp_utc': end_timestamp,
        'annotation_label': label,
        'version_name': 'version_name',
        'version_number': 1,
    }


def create_participant_mapping(start_timestamp, end_timestamp):
    return ('123', [
        participant_mappings.ParticipantInfo(
            '123', '321', 1,
            timestamps.datetime_to_beam_timestamp(start_timestamp),
            timestamps.datetime_to_beam_timestamp(end_timestamp))
    ])


class AnnotationSourceTest(unittest.TestCase):

    @mock.patch(
        'verily.ds_sdk.core.gcp.bigquery_source_wrapper.GcpBigquerySourceWrapper'  # pylint: disable=line-too-long
    )
    @mock.patch(
        'verily.ds_sdk.core.io.bigquery.participant_mappings.BuildParticipantMappings'  # pylint: disable=line-too-long
    )
    @mock.patch('verily.ds_sdk.core.schema_fetcher.SchemaFetcher')
    def test_parse_annotation_row(self, schema_fetcher_mock, participant_mock,
                                  bq_source_mock):
        schema_fetcher_mock.return_value = FakeSchemaFetcher()
        annotation_row = create_annotation_bq_row('ann_label')
        bq_source_mock.side_effect = [beam.Create([annotation_row])]
        participant_mapping = create_participant_mapping(
            PD_START_TIMESTAMP, PD_END_TIMESTAMP)
        participant_mock.side_effect = [beam.Create([participant_mapping])]

        expected_annotation = schemas.Annotation('ann_label',
                                                 BEAM_START_TIMESTAMP,
                                                 BEAM_END_TIMESTAMP,
                                                 ANNOTATION_METADATA)

        with TestPipeline() as p:
            annotations = p | annotation_source.AnnotationRowSource(
                bigquery_table_id=
                'sensors-devteam.sensor_store_DevTeam.derived_annotations',  # pylint: disable=line-too-long
                participant_table_id=
                'sensors-devteam.sensor_store_DevTeam.participant_associations',  # pylint: disable=line-too-long
                source_options=options.BatchSourceOptions(),
                condition=None,
                creds=FakeDsSdkCredentials(),
                env='prod',
                billing_project='project',
                bigquery_location='US',
                service_account='sa')

            assert_that(annotations, equal_to([expected_annotation]))

    @mock.patch(
        'verily.ds_sdk.core.gcp.bigquery_source_wrapper.GcpBigquerySourceWrapper'  # pylint: disable=line-too-long
    )
    @mock.patch(
        'verily.ds_sdk.core.io.bigquery.participant_mappings.BuildParticipantMappings'  # pylint: disable=line-too-long
    )
    @mock.patch('verily.ds_sdk.core.schema_fetcher.SchemaFetcher')
    def test_parse_split_annotation_row(self, schema_fetcher_mock,
                                        participant_mock, bq_source_mock):
        schema_fetcher_mock.return_value = FakeSchemaFetcher()
        annotation_row = create_annotation_bq_row(
            'ann_label', end_timestamp=PD_END_TIMESTAMP2)
        bq_source_mock.side_effect = [beam.Create([annotation_row])]

        participant_mapping = ('123', [
            participant_mappings.ParticipantInfo(
                '123', '321', 1,
                timestamps.datetime_to_beam_timestamp(PD_START_TIMESTAMP),
                timestamps.datetime_to_beam_timestamp(PD_END_TIMESTAMP)),
            participant_mappings.ParticipantInfo(
                '123', '654', 1,
                timestamps.datetime_to_beam_timestamp(PD_END_TIMESTAMP),
                timestamps.datetime_to_beam_timestamp(PD_END_TIMESTAMP2))
        ])
        participant_mock.side_effect = [beam.Create([participant_mapping])]

        expected_annotations = [
            schemas.Annotation('ann_label', BEAM_START_TIMESTAMP,
                               BEAM_END_TIMESTAMP, ANNOTATION_METADATA),
            schemas.Annotation('ann_label', BEAM_END_TIMESTAMP,
                               BEAM_END_TIMESTAMP2, ANNOTATION_METADATA2)
        ]

        with TestPipeline() as p:
            annotations = p | annotation_source.AnnotationRowSource(
                bigquery_table_id=
                'sensors-devteam.sensor_store_DevTeam.derived_annotations',  # pylint: disable=line-too-long
                participant_table_id=
                'sensors-devteam.sensor_store_DevTeam.participant_associations',  # pylint: disable=line-too-long
                source_options=options.BatchSourceOptions(),
                condition=None,
                creds=FakeDsSdkCredentials(),
                env='prod',
                billing_project='project',
                bigquery_location='US',
                service_account='sa')

            assert_that(annotations, equal_to(expected_annotations))

    @mock.patch(
        'verily.ds_sdk.core.gcp.bigquery_source_wrapper.GcpBigquerySourceWrapper'  # pylint: disable=line-too-long
    )
    @mock.patch(
        'verily.ds_sdk.core.io.bigquery.participant_mappings.BuildParticipantMappings'  # pylint: disable=line-too-long
    )
    @mock.patch('verily.ds_sdk.core.schema_fetcher.SchemaFetcher')
    def test_no_participant_association(self, schema_fetcher_mock,
                                        participant_mock, bq_source_mock):
        schema_fetcher_mock.return_value = FakeSchemaFetcher()
        annotation_rows = [
            create_annotation_bq_row('ann_label', PD_START_TIMESTAMP,
                                     PD_END_TIMESTAMP),
            # This annotation has no participant -> should be filtered out.
            create_annotation_bq_row('ann_label', pd.Timestamp('2010-01-01'),
                                     pd.Timestamp('2011-01-01'))
        ]
        bq_source_mock.side_effect = [beam.Create(annotation_rows)]
        participant_mapping = create_participant_mapping(
            PD_START_TIMESTAMP, PD_END_TIMESTAMP)
        participant_mock.side_effect = [beam.Create([participant_mapping])]

        no_participant = deepcopy(ANNOTATION_METADATA)
        no_participant.participant_id = None
        no_participant.participant_namespace = None

        expected_annotations = [
            schemas.Annotation('ann_label', BEAM_START_TIMESTAMP,
                               BEAM_END_TIMESTAMP, ANNOTATION_METADATA),
            schemas.Annotation(
                'ann_label',
                timestamps.datetime_to_beam_timestamp(
                    pd.Timestamp('2010-01-01')),
                timestamps.datetime_to_beam_timestamp(
                    pd.Timestamp('2011-01-01')), no_participant)
        ]

        with TestPipeline() as p:
            annotations = p | annotation_source.AnnotationRowSource(
                bigquery_table_id=
                'sensors-devteam.sensor_store_DevTeam.derived_annotations',  # pylint: disable=line-too-long
                participant_table_id=
                'sensors-devteam.sensor_store_DevTeam.participant_associations',  # pylint: disable=line-too-long
                source_options=options.BatchSourceOptions(),
                condition=None,
                creds=FakeDsSdkCredentials(),
                env='prod',
                billing_project='project',
                bigquery_location='US',
                service_account='sa')

            assert_that(annotations, equal_to(expected_annotations))


if __name__ == '__main__':
    unittest.main()
