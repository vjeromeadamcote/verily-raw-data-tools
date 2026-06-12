"""Integration test to make sure sandwich pipeline can run on dataflow.

NOTE: These tests don't actually verify the contents of the output, they just
verify that a job can be launched and successfully completes.

TO run the test:

  python verily/ds_sdk/integration/sandwich_dataflow_integration_test.py
"""

import uuid

import pandas as pd

from verily.ds_sdk.core import options
from verily.ds_sdk.core import schemas
from verily.ds_sdk.core import sensors_io

TEST_REGISTRY_WITH_ASSOCIATIONS = 'IntegrationTestParticipantAssociations'
GCP_PROJECT = 'datascience-sdk'
BQ_DATASET = 'integration_test_output'


def run_integration_test():
    unique_id = uuid.uuid1().hex
    bq_table = f'{GCP_PROJECT}.{BQ_DATASET}.sandwich_{unique_id}'

    gcs_temp_location = 'gs://ds_sdk_integration/temp'
    runner = 'DataflowRunner'

    sensors_pipeline = sensors_io.SensorsIO(
        registry=TEST_REGISTRY_WITH_ASSOCIATIONS,
        runner=runner,
        env='prod',
        temp_gcs_bucket=gcs_temp_location,
        dataflow_options=options.DataflowOptions(
            job_name=f'integration-test-{unique_id}'),
        gcp_project='datascience-sdk')

    data_point_rows = sensors_pipeline.echo_data_point_rows(
        data_spec_name='com.verily.pressure',
        source_options=options.BatchSourceOptions(),
        condition=None,
        annotation_inner_join_options=None,
        incremental_query_options=options.IncrementalQueryOptions(
            write_start_time=pd.Timestamp('2020-02-01'),
            write_end_time=pd.Timestamp('2020-02-02')))

    _ = (data_point_rows | sensors_pipeline.write_data_points_to_big_query(
        bq_table, schemas.Pressure))

    sensors_pipeline.run()


def main():
    run_integration_test()


if __name__ == '__main__':
    main()
