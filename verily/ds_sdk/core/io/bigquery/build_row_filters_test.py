"""Tests for build_row_filters."""

from typing import Iterable
import unittest
from unittest import mock

from apache_beam.utils.timestamp import Timestamp
import ibis_bigquery
import pandas as pd

from verily.ds_sdk.core import conditions
from verily.ds_sdk.core import schemas
from verily.ds_sdk.core.io.bigquery import build_row_filters
from verily.ds_sdk.core.testing import ibis_testing_utils

ANNOTATION_TABLE_ID = 'project.dataset.ann_table'
ANNOTATION_IBIS_TABLE = ibis_testing_utils.annotations_table(
    ANNOTATION_TABLE_ID
)

DATA_POINTS_TABLE_ID = 'project.dataset.dp_table'
DATA_POINTS_IBIS_TABLE = ibis_testing_utils.data_points_table(
    DATA_POINTS_TABLE_ID
)


class FakeDsSdkCredentials:
    def get_credentials(self):
        return (mock.MagicMock(), None)


class FakeTable:
    def __init__(self, is_view) -> None:
        if is_view:
            self.table_type = 'VIEW'
        else:
            self.table_type = 'TABLE'


class FakeQuery:
    def result(self):
        return


class FakeBigQueryClient:
    """Fake BQ Client for testing."""

    def __init__(self, views: Iterable[str] = ()) -> None:
        self._views = views
        self._query = ''

    def get_table(self, table_id: str):
        return FakeTable(table_id in self._views)

    def query(self, query, job_config):
        self._query = query
        del job_config
        return FakeQuery()

    def query_requested(self):
        return self._query


def build_timestamp(timestamp_str: str) -> Timestamp:
    return Timestamp.from_utc_datetime(pd.Timestamp(timestamp_str, tz='UTC'))


def build_annotation(
    device_id: str,
    annotation_label: str,
    start_timestamp_str: str,
    end_timestamp_str: str,
) -> schemas.Annotation:
    annotation_metadata = schemas.AnnotationMetadata(
        device_id=device_id,
        participant_id='participant_id',
        participant_namespace=1,
        version_name=None,
        version_number=None,
        input_data_info=[],
    )

    return schemas.Annotation(
        annotation_label=annotation_label,
        start_timestamp_utc=build_timestamp(start_timestamp_str),
        end_timestamp_utc=build_timestamp(end_timestamp_str),
        annotation_metadata=annotation_metadata,
    )


def get_annotation_row_filter(condition):
    cond = condition.annotations_condition(
        ANNOTATION_IBIS_TABLE, include_annotation_conditions=True
    )
    row_filter = None
    if cond is not None:
        query = ibis_bigquery.compile(ANNOTATION_IBIS_TABLE[cond])
        # The first index will be the actual condition we want to filter on.
        row_filter = query.split('WHERE', 1)[1]
    return row_filter


def get_data_points_row_filter(condition):
    cond = condition.data_points_condition(
        DATA_POINTS_IBIS_TABLE, include_annotation_conditions=False
    )
    row_filter = None
    if cond is not None:
        query = ibis_bigquery.compile(DATA_POINTS_IBIS_TABLE[cond])
        # The first index will be the actual condition we want to filter on.
        row_filter = query.split('WHERE', 1)[1]
    return row_filter


class BuildAnnotationTableRowFiltersTest(unittest.TestCase):
    @mock.patch(
        'google.cloud.bigquery.Client', return_value=FakeBigQueryClient()
    )
    def test_build_row_filters_no_condition(self, bq_mock):
        del bq_mock
        annotations_condition = None

        expected_row_filters = (ANNOTATION_TABLE_ID, None)

        row_filters = build_row_filters.BuildAnnotationTableRowFilters(
            ANNOTATION_TABLE_ID,
            annotations_condition,
            ANNOTATION_IBIS_TABLE,
            creds=FakeDsSdkCredentials(),
            billing_project='billing',
            bigquery_location='US',
        ).materialize()

        self.assertEqual(row_filters, expected_row_filters)

    @mock.patch(
        'google.cloud.bigquery.Client', return_value=FakeBigQueryClient()
    )
    def test_build_row_filters_with_device_filter(self, bq_mock):
        del bq_mock
        annotations_condition = conditions.DevicesCondition(['C2Q123'])

        expected_row_filters = (
            ANNOTATION_TABLE_ID,
            get_annotation_row_filter(annotations_condition),
        )

        row_filters = build_row_filters.BuildAnnotationTableRowFilters(
            ANNOTATION_TABLE_ID,
            annotations_condition,
            ANNOTATION_IBIS_TABLE,
            creds=FakeDsSdkCredentials(),
            billing_project='billing',
            bigquery_location='US',
        ).materialize()

        self.assertEqual(row_filters, expected_row_filters)

    @mock.patch(
        'google.cloud.bigquery.Client', return_value=FakeBigQueryClient()
    )
    def test_build_row_filters_materialize(self, bq_mock):
        del bq_mock
        annotations_condition = conditions.DevicesCondition(['C2Q123'])

        expected_row_filters = (
            ANNOTATION_TABLE_ID,
            get_annotation_row_filter(annotations_condition),
        )

        got = build_row_filters.BuildAnnotationTableRowFilters(
            ANNOTATION_TABLE_ID,
            annotations_condition,
            ANNOTATION_IBIS_TABLE,
            creds=FakeDsSdkCredentials(),
            billing_project='billing',
            bigquery_location='US',
        ).materialize()

        self.assertCountEqual(expected_row_filters, got)

    @mock.patch(
        'google.cloud.bigquery.Client',
        return_value=FakeBigQueryClient(ANNOTATION_TABLE_ID),
    )
    def test_build_row_filters_materialize_view(self, bq_mock):
        del bq_mock
        annotations_condition = None

        row_filters = build_row_filters.BuildAnnotationTableRowFilters(
            ANNOTATION_TABLE_ID,
            annotations_condition,
            ANNOTATION_IBIS_TABLE,
            creds=FakeDsSdkCredentials(),
            billing_project='billing',
            bigquery_location='US',
        )

        expected = 'project.datascience_sdk_temp.temp_materialized_view_'
        table, _ = row_filters.materialize()
        self.assertTrue(
            table.startswith(expected),
            f'Table: {table} did not start with: {expected}',
        )


class BuildDataPointsTableRowFiltersTest(unittest.TestCase):
    @mock.patch('google.cloud.bigquery.Client')
    def test_build_row_filters_no_data_point_condition_no_annotations(
        self, bq_mock
    ):
        bq_client = FakeBigQueryClient()
        bq_mock.return_value = bq_client
        annotation_rows = []
        ann_source_mock = mock.MagicMock()
        ann_source_mock.get_raw_annotations.return_value = annotation_rows

        data_points_condition = None

        table_id, row_filter = build_row_filters.BuildDataPointTableRowFilters(
            DATA_POINTS_TABLE_ID,
            data_points_condition,
            DATA_POINTS_IBIS_TABLE,
            annotations=[ann_source_mock],
            creds=FakeDsSdkCredentials(),
            billing_project='billing',
            bigquery_location='US',
            dedupe_unique_identifer='DataSourceID, DataPointTime',
            dedupe_column_to_keep_max_value='DataPointWriteTime',
        ).materialize()
        del table_id

        expected_query = """SELECT * EXCEPT(row_num)
        FROM (
            SELECT *,
            ROW_NUMBER() OVER(
                PARTITION BY DataSourceID, DataPointTime
                ORDER BY DataPointWriteTime DESC
            ) as row_num
            FROM (SELECT * FROM `project.dataset.dp_table`)
        )
        WHERE row_num = 1"""
        self.assertEqual(bq_client.query_requested(), expected_query)
        self.assertEqual(row_filter, None)

    @mock.patch('google.cloud.bigquery.Client')
    def test_build_row_filters_no_base_condition_with_annotations(
        self, bq_mock
    ):
        bq_client = FakeBigQueryClient()
        bq_mock.return_value = bq_client
        annotation_rows = [
            build_annotation(
                'C2Q123',
                'WEARING',
                '2020-01-01 12:00:00 UTC',
                '2020-01-01 13:00:00 UTC',
            )
        ]

        ann_source_mock = mock.MagicMock()
        ann_source_mock.get_raw_annotations.return_value = annotation_rows

        data_points_condition = None

        table_id, row_filter = build_row_filters.BuildDataPointTableRowFilters(
            DATA_POINTS_TABLE_ID,
            data_points_condition,
            DATA_POINTS_IBIS_TABLE,
            annotations=[ann_source_mock],
            creds=FakeDsSdkCredentials(),
            billing_project='billing',
            bigquery_location='US',
            dedupe_unique_identifer='DataSourceID, DataPointTime',
            dedupe_column_to_keep_max_value='DataPointWriteTime',
        ).materialize()
        del table_id, row_filter
        expected_query = """SELECT * EXCEPT(row_num)
        FROM (
            SELECT *,
            ROW_NUMBER() OVER(
                PARTITION BY DataSourceID, DataPointTime
                ORDER BY DataPointWriteTime DESC
            ) as row_num
            FROM (SELECT * FROM `project.dataset.dp_table` WHERE  (`DataPointTime` >= TIMESTAMP '2020-01-01 00:00:00+00:00') AND
      (`DataPointTime` < TIMESTAMP '2020-01-02 00:00:00+00:00'))
        )
        WHERE row_num = 1"""

        self.assertEqual(bq_client.query_requested(), expected_query)

    @mock.patch('google.cloud.bigquery.Client')
    def test_build_row_filters_base_condition_no_annotations(self, bq_mock):
        bq_client = FakeBigQueryClient()
        bq_mock.return_value = bq_client
        annotation_rows = []
        ann_source_mock = mock.MagicMock()
        ann_source_mock.get_raw_annotations.return_value = annotation_rows

        data_points_condition = conditions.DevicesCondition(['C2Q123'])

        # DataPoints condition & no annotations -> row filter should be the
        # given DataPoints condition.
        expected_row_filter = " `DeviceID` IN ('C2Q123')"

        table_id, row_filter = build_row_filters.BuildDataPointTableRowFilters(
            DATA_POINTS_TABLE_ID,
            data_points_condition,
            DATA_POINTS_IBIS_TABLE,
            annotations=[ann_source_mock],
            creds=FakeDsSdkCredentials(),
            billing_project='billing',
            bigquery_location='US',
            dedupe_unique_identifer='DataSourceID, DataPointTime',
            dedupe_column_to_keep_max_value='DataPointWriteTime',
        ).materialize()
        del table_id

        expected_query = """SELECT * EXCEPT(row_num)
        FROM (
            SELECT *,
            ROW_NUMBER() OVER(
                PARTITION BY DataSourceID, DataPointTime
                ORDER BY DataPointWriteTime DESC
            ) as row_num
            FROM (SELECT * FROM `project.dataset.dp_table` WHERE  `DeviceID` IN ('C2Q123'))
        )
        WHERE row_num = 1"""
        self.assertEqual(bq_client.query_requested(), expected_query)
        self.assertEqual(row_filter, expected_row_filter)

    @mock.patch('google.cloud.bigquery.Client')
    def test_build_row_filters_base_condition_with_annotations(self, bq_mock):
        bq_client = FakeBigQueryClient()
        bq_mock.return_value = bq_client
        annotation_rows = [
            build_annotation(
                'C2Q123',
                'WEARING',
                '2020-01-01 12:00:00 UTC',
                '2020-01-01 13:00:00 UTC',
            ),
            build_annotation(
                'C2Q123',
                'WEARING',
                '2020-01-02 12:00:00 UTC',
                '2020-01-03 13:00:00 UTC',
            ),
        ]
        ann_source_mock = mock.MagicMock()
        ann_source_mock.get_raw_annotations.return_value = annotation_rows

        day_partition_condition = conditions.TimeRangeCondition(
            conditions.TimeRange(
                pd.Timestamp('2020-01-01 00:00:00 UTC'),
                # The 2 annotations above should be merged into a single
                # condition.
                pd.Timestamp('2020-01-04 00:00:00 UTC'),
            )
        )

        data_points_condition = conditions.DevicesCondition(['C2Q123'])

        # DataPoints condition & annotations -> row filter should be the
        # combination of the two types of conditions.
        expected_row_filter = get_data_points_row_filter(
            data_points_condition & day_partition_condition
        )

        table_id, row_filter = build_row_filters.BuildDataPointTableRowFilters(
            DATA_POINTS_TABLE_ID,
            data_points_condition,
            DATA_POINTS_IBIS_TABLE,
            annotations=[ann_source_mock],
            creds=FakeDsSdkCredentials(),
            billing_project='billing',
            bigquery_location='US',
            dedupe_unique_identifer='DataSourceID, DataPointTime',
            dedupe_column_to_keep_max_value='DataPointWriteTime',
        ).materialize()
        del table_id

        expected_query = """SELECT * EXCEPT(row_num)
        FROM (
            SELECT *,
            ROW_NUMBER() OVER(
                PARTITION BY DataSourceID, DataPointTime
                ORDER BY DataPointWriteTime DESC
            ) as row_num
            FROM (SELECT * FROM `project.dataset.dp_table` WHERE  (`DeviceID` IN ('C2Q123')) AND
      (`DataPointTime` >= TIMESTAMP '2020-01-01 00:00:00+00:00') AND
      (`DataPointTime` < TIMESTAMP '2020-01-04 00:00:00+00:00'))
        )
        WHERE row_num = 1"""
        self.assertEqual(bq_client.query_requested(), expected_query)
        self.assertEqual(row_filter, expected_row_filter)

    @mock.patch('google.cloud.bigquery.Client')
    def test_build_row_filters_base_condition_with_annotations_materialized(
        self, bq_mock
    ):
        bq_client = FakeBigQueryClient()
        bq_mock.return_value = bq_client
        annotation_rows = [
            build_annotation(
                'C2Q123',
                'WEARING',
                '2020-01-01 12:00:00 UTC',
                '2020-01-01 13:00:00 UTC',
            ),
            build_annotation(
                'C2Q123',
                'WEARING',
                '2020-01-02 12:00:00 UTC',
                '2020-01-03 13:00:00 UTC',
            ),
        ]
        ann_source_mock = mock.MagicMock()
        ann_source_mock.get_raw_annotations.return_value = annotation_rows

        day_partition_condition = conditions.TimeRangeCondition(
            conditions.TimeRange(
                pd.Timestamp('2020-01-01 00:00:00 UTC'),
                # The 2 annotations above should be merged into a single
                # condition.
                pd.Timestamp('2020-01-04 00:00:00 UTC'),
            )
        )

        data_points_condition = conditions.DevicesCondition(['C2Q123'])

        # DataPoints condition & annotations -> row filter should be the
        # combination of the two types of conditions.
        expected_row_filter = get_data_points_row_filter(
            data_points_condition & day_partition_condition
        )

        table_id, got = build_row_filters.BuildDataPointTableRowFilters(
            DATA_POINTS_TABLE_ID,
            data_points_condition,
            DATA_POINTS_IBIS_TABLE,
            annotations=[ann_source_mock],
            creds=FakeDsSdkCredentials(),
            billing_project='billing',
            bigquery_location='US',
            dedupe_unique_identifer='DataSourceID, DataPointTime',
            dedupe_column_to_keep_max_value='DataPointWriteTime',
        ).materialize()
        del table_id

        expected_query = """SELECT * EXCEPT(row_num)
        FROM (
            SELECT *,
            ROW_NUMBER() OVER(
                PARTITION BY DataSourceID, DataPointTime
                ORDER BY DataPointWriteTime DESC
            ) as row_num
            FROM (SELECT * FROM `project.dataset.dp_table` WHERE  (`DeviceID` IN ('C2Q123')) AND
      (`DataPointTime` >= TIMESTAMP '2020-01-01 00:00:00+00:00') AND
      (`DataPointTime` < TIMESTAMP '2020-01-04 00:00:00+00:00'))
        )
        WHERE row_num = 1"""
        self.assertEqual(bq_client.query_requested(), expected_query)
        self.assertEqual(got, expected_row_filter)

    @mock.patch(
        'google.cloud.bigquery.Client',
        return_value=FakeBigQueryClient([DATA_POINTS_TABLE_ID]),
    )
    def test_build_row_filters_materialize_view_data_points(self, bq_mock):
        del bq_mock

        row_filters = build_row_filters.BuildDataPointTableRowFilters(
            DATA_POINTS_TABLE_ID,
            None,
            DATA_POINTS_IBIS_TABLE,
            annotations=[],
            creds=FakeDsSdkCredentials(),
            billing_project='billing',
            bigquery_location='US',
            dedupe_unique_identifer='DataSourceID, DataPointTime',
            dedupe_column_to_keep_max_value='DataPointWriteTime',
        )

        expected = 'project.datascience_sdk_temp.temp_materialized_view_'
        table, _ = row_filters.materialize()
        self.assertTrue(
            table.startswith(expected),
            f'Table: {table} did not start with: {expected}',
        )

    @mock.patch(
        'google.cloud.bigquery.Client',
        return_value=FakeBigQueryClient(ANNOTATION_TABLE_ID),
    )
    def test_build_row_filters_materialize_view_pass_through(self, bq_mock):
        del bq_mock

        row_filters = build_row_filters.PassThroughRowFilter(
            row_filter=(ANNOTATION_TABLE_ID, None),
            creds=FakeDsSdkCredentials(),
            billing_project='billing',
            bigquery_location='US',
        )

        expected = 'project.datascience_sdk_temp.temp_materialized_view_'
        table, _ = row_filters.materialize()
        self.assertTrue(
            table.startswith(expected),
            f'Table: {table} did not start with: {expected}',
        )


if __name__ == '__main__':
    unittest.main()
