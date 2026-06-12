"""Pipeline used for integration tests.

NOTE: No idea why but you need to run pip install . before running this to pick
up local changes.
"""

import sys
from typing import Optional

from verily.ds_sdk.core import options
from verily.ds_sdk.core import schemas
from verily.ds_sdk.core import sensors_io


def run_pipeline(registry: str,
                 runner: str,
                 bq_table: str,
                 temp_gcs_location: Optional[str] = None,
                 condition=None,
                 annotation_inner_join_options=None,
                 incremental_query_options=None,
                 custom_bigquery_table: Optional[str] = None):

    sensors_pipeline = sensors_io.SensorsIO(registry=registry,
                                            runner=runner,
                                            env='prod',
                                            temp_gcs_bucket=temp_gcs_location)

    if custom_bigquery_table is None:
        data_point_rows = sensors_pipeline.echo_data_point_rows(
            data_spec_name='com.verily.pressure',
            source_options=options.BatchSourceOptions(),
            condition=condition,
            annotation_inner_join_options=annotation_inner_join_options,
            incremental_query_options=incremental_query_options,
        )
    else:
        data_point_rows = sensors_pipeline.custom_data_point_rows(
            data_point_table_id=custom_bigquery_table,
            row_schema=schemas.Pressure,
            source_options=options.BatchSourceOptions(),
            condition=condition,
            annotation_inner_join_options=annotation_inner_join_options)

    _ = (data_point_rows | sensors_pipeline.write_data_points_to_big_query(
        bq_table, schemas.Pressure))

    sensors_pipeline.run()


def main(argv=None):
    del argv
    run_pipeline('DevTeam', 'DirectRunner',
                 'sensors-devteam.sandwich-testing.test_pressure')


if __name__ == '__main__':
    main(sys.argv)
