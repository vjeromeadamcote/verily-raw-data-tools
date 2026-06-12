"""Pipeline used for integration tests."""

from typing import Dict, Optional

import apache_beam as beam

from verily.ds_sdk.core import options
from verily.ds_sdk.core import schemas
from verily.ds_sdk.core import sensors_io


def run_pipeline(
    registry: str,
    runner: str,
    bq_table: str,
    api_key: str,
    temp_gcs_location: Optional[str] = None,
    condition=None,
):

    sensors_pipeline = sensors_io.SensorsIO(registry=registry,
                                            runner=runner,
                                            env='prod',
                                            temp_gcs_bucket=temp_gcs_location,
                                            streaming=True)

    data_spec_pcol_dict: Dict[str, beam.PCollection] = (
        sensors_pipeline.sensor_store_streaming_rows(
            data_spec_names=['com.verily.pressure'],
            streaming_options=options.StreamingSourceOptions(
                redis_endpoint='localhost:6388',
                topic='projects/datascience-sdk/topics/integration-testing'),
            condition=condition,
            pubsub_message_window_into=beam.WindowInto(
                beam.transforms.window.FixedWindows(5)),
            api_key=api_key))

    _ = (data_spec_pcol_dict['com.verily.pressure'] |
         sensors_pipeline.write_data_points_to_big_query(
             bq_table, schemas.Pressure))

    sensors_pipeline.run()
