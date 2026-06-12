"""Integration test for sandwich DS SDK.

This reads from the table:
    datascience-sdk.integration_test.com_verily_pressure

This table was generated from the query:

CREATE TABLE
  datascience-sdk.integration_test.com_verily_pressure AS (
  -- The first select is the "true" data and is the data that should be returned
  -- after deduping.
  SELECT
    * EXCEPT (BucketWriteTime),
    BucketWriteTime
  FROM
    `sensors-devteam.sensor_store_DevTeam.com_verily_pressure`
  WHERE
    DeviceID = "C2QSX20X181905BP"
    AND DataPointTime <= TIMESTAMP "2020-11-24 21:07:03.517 UTC"
    AND DataPointTime >= TIMESTAMP "2020-11-21 02:12:16.051 UTC"
  UNION ALL (
    -- Insert "dupes" that are identical
    SELECT
      * EXCEPT (BucketWriteTime),
      BucketWriteTime
    FROM
      `sensors-devteam.sensor_store_DevTeam.com_verily_pressure`
    WHERE
      DeviceID = "C2QSX20X181905BP"
      AND DataPointTime <= TIMESTAMP "2020-11-24 21:07:03.517 UTC"
      AND DataPointTime >= TIMESTAMP "2020-11-21 02:12:16.051 UTC")
  UNION ALL (
    -- Insert "dupes" that have a lower snapshot time
    SELECT
      * EXCEPT (SnapShotTime,
        BucketWriteTime,
        DataPointTime),
      BucketWriteTime,
      TIMESTAMP_SUB(SnapShotTime, INTERVAL 10 MINUTE) AS SnapShotTime,
      TIMESTAMP_SUB(DataPointTime, INTERVAL 1 SECOND) AS DataPointTime
    FROM
      `sensors-devteam.sensor_store_DevTeam.com_verily_pressure`
    WHERE
      DeviceID = "C2QSX20X181905BP"
      AND DataPointTime <= TIMESTAMP "2020-11-24 21:07:03.517 UTC"
      AND DataPointTime >= TIMESTAMP "2020-11-21 02:12:16.051 UTC"))
"""

import unittest
import uuid

from google.cloud import bigquery
import pandas as pd

from verily.ds_sdk.core import conditions
from verily.ds_sdk.core import options
from verily.ds_sdk.core.gcp import credentials
from verily.ds_sdk.integration import sandwich_batch_test_pipeline

INTEGRATION_TEST_REGISTRY = 'IntegrationTest'
TEST_REGISTRY_WITH_ASSOCIATIONS = 'IntegrationTestParticipantAssociations'
GCP_PROJECT = 'datascience-sdk'
BQ_DATASET = 'integration_test_output'


class SandwichBatchIntegrationTest(unittest.TestCase):
    """Integration test for sandwich DS SDK."""

    def setUp(self):
        unique_id = uuid.uuid1().hex
        self._bq_table = f'{GCP_PROJECT}.{BQ_DATASET}.sandwich_{unique_id}'

        self.device = 'C2QSX20X181905BP'
        self._gcs_temp_location = 'gs://ds_sdk_integration'
        self._runner = 'DirectRunner'

        ds_sdk_creds = credentials.DsSdkCredentials(
            runner=self._runner,
            service_account=
            'ds-sdk-readers@datascience-sdk.iam.gserviceaccount.com',  # pylint: disable=line-too-long
            billing_project=GCP_PROJECT)
        creds, _ = ds_sdk_creds.get_credentials()
        self.bq_client = bigquery.Client(project=GCP_PROJECT, credentials=creds)

    def test_simple_query(self):
        """Simply reads data into the SDK and outputs to BQ."""

        sandwich_batch_test_pipeline.run_pipeline(
            registry=TEST_REGISTRY_WITH_ASSOCIATIONS,
            runner=self._runner,
            bq_table=self._bq_table,
            temp_gcs_location=self._gcs_temp_location)

        query_job = self.bq_client.query(
            f'SELECT count(*) as count FROM {self._bq_table}')

        results = list(query_job.result())

        self.assertEqual(len(results), 1)
        self.assertEqual(results[0]['count'], 1000)

    def test_conditions_query(self):
        """Simply reads data into the SDK and outputs to BQ."""

        start_time = pd.Timestamp('2020-11-24 19:07:03.517 UTC')
        end_time = pd.Timestamp('2020-11-24 21:07:03.517 UTC')

        condition = (conditions.DevicesCondition(
            [self.device]) & conditions.TimeRangeCondition(
                conditions.TimeRange(start_time=start_time, end_time=end_time)))

        sandwich_batch_test_pipeline.run_pipeline(
            registry=TEST_REGISTRY_WITH_ASSOCIATIONS,
            runner=self._runner,
            bq_table=self._bq_table,
            temp_gcs_location=self._gcs_temp_location,
            condition=condition)

        query_job = self.bq_client.query(f'SELECT * FROM {self._bq_table}')

        results = query_job.result()

        for row in results:
            measurment_time = row['DataPointTime']
            self.assertGreaterEqual(measurment_time, start_time)
            self.assertLess(measurment_time, end_time)

    def test_read_results(self):
        sandwich_batch_test_pipeline.run_pipeline(
            registry=TEST_REGISTRY_WITH_ASSOCIATIONS,
            runner=self._runner,
            bq_table=self._bq_table,
            temp_gcs_location=self._gcs_temp_location)

        unique_id = uuid.uuid1().hex
        second_table = f'{GCP_PROJECT}.{BQ_DATASET}.sandwich_second_{unique_id}'

        sandwich_batch_test_pipeline.run_pipeline(
            registry=TEST_REGISTRY_WITH_ASSOCIATIONS,
            runner=self._runner,
            bq_table=second_table,
            temp_gcs_location=self._gcs_temp_location,
            custom_bigquery_table=self._bq_table)

        query_job = self.bq_client.query(
            f'SELECT count(*) as count FROM {second_table}')

        results = list(query_job.result())

        self.assertEqual(len(results), 1)
        self.assertEqual(results[0]['count'], 1000)

    def test_incremental_query(self):
        """Simply reads data into the SDK and outputs to BQ."""

        sandwich_batch_test_pipeline.run_pipeline(
            registry=TEST_REGISTRY_WITH_ASSOCIATIONS,
            runner=self._runner,
            bq_table=self._bq_table,
            temp_gcs_location=self._gcs_temp_location,
            incremental_query_options=options.IncrementalQueryOptions(
                write_start_time=pd.Timestamp('2020-11-20', tz='UTC'),
                write_end_time=pd.Timestamp('2020-11-25', tz='UTC')))

        query_job = self.bq_client.query(
            f'SELECT count(*) as count FROM {self._bq_table}')

        results = list(query_job.result())

        self.assertEqual(len(results), 1)
        self.assertEqual(results[0]['count'], 986)

    def test_empty_incremental_query(self):
        """Simply reads data into the SDK and outputs to BQ."""

        with self.assertLogs(level='WARNING') as cm:
            sandwich_batch_test_pipeline.run_pipeline(
                registry=TEST_REGISTRY_WITH_ASSOCIATIONS,
                runner=self._runner,
                bq_table=self._bq_table,
                temp_gcs_location=self._gcs_temp_location,
                incremental_query_options=options.IncrementalQueryOptions(
                    write_start_time=pd.Timestamp('2050-11-20', tz='UTC'),
                    write_end_time=pd.Timestamp('2050-11-25', tz='UTC')))

            query_job = self.bq_client.query(
                f'SELECT count(*) as count FROM {self._bq_table}')

            results = list(query_job.result())

            self.assertEqual(len(results), 1)
            self.assertEqual(results[0]['count'], 0)

            self.assertIn(
                ('WARNING:root:No data was found with the incremental '
                 'query parameters, nothing to do.'), cm.output)

    def test_annotations_inner_join_query(self):
        """Reads and joins with derived annotions and outputs to BQ."""

        sandwich_batch_test_pipeline.run_pipeline(
            registry=TEST_REGISTRY_WITH_ASSOCIATIONS,
            runner=self._runner,
            bq_table=self._bq_table,
            temp_gcs_location=self._gcs_temp_location,
            annotation_inner_join_options=options.AnnotationInnerJoinOptions(
                annotation_labels={'WEARING'},
                annotation_tables={
                    'datascience-sdk.integration_test.custom_annotations'
                }))

        query_job = self.bq_client.query(
            f'SELECT count(*) as count FROM {self._bq_table}')

        results = list(query_job.result())

        self.assertEqual(len(results), 1)
        self.assertEqual(results[0]['count'], 986)


if __name__ == '__main__':
    unittest.main()
