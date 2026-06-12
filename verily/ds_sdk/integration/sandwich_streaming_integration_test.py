"""Streaming integration test for DS SDK."""

import datetime
import logging
import multiprocessing.pool
import random
import subprocess
import time
import unittest

from google.cloud import bigquery
import pandas as pd

from verily import ds_sdk
from verily.ds_sdk.core import schemas
from verily.ds_sdk.core.io.data_source_cache import DataSourceCache
from verily.ds_sdk.core.sensorsuite import sensor_store_client
from verily.ds_sdk.core.utils import timestamps
from verily.ds_sdk.integration import \
    sandwich_streaming_integration_test_pipeline
from verily.ds_sdk.protos import enums_pb2
from verily.ds_sdk.protos import types_pb2

MAX_WAIT_TIME = datetime.timedelta(minutes=5).total_seconds()
GCP_PROJECT = 'datascience-sdk'
DATASET = 'integration_test_output'
INTALL_REDIS_COMMAND = 'sudo apt-get install redis-server'
START_REDIS_COMMAND = 'redis-server --daemonize yes --port 6388'


class DsSdkIntegrationTest(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        proc = subprocess.Popen(  # pylint: disable=consider-using-with
            INTALL_REDIS_COMMAND.split(),
            stdin=subprocess.PIPE,
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT)
        stdout_data, _ = proc.communicate()
        print(f'Redis server install output: {stdout_data}')
        if proc.returncode != 0:
            raise RuntimeError('Failed to install redis server.')
        proc = subprocess.Popen(  # pylint: disable=consider-using-with
            START_REDIS_COMMAND.split(),
            stdin=subprocess.PIPE,
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT)
        stdout_data, _ = proc.communicate()
        print(f'Redis server start output: {stdout_data}')
        if proc.returncode != 0:
            raise RuntimeError('Failed to start redis server.')

    def setUp(self):
        self.unique_id = random.randint(0, 1000000)
        self.output_table = f'{GCP_PROJECT}.{DATASET}.sandwich_{self.unique_id}'

        self.device_id = 'streaming-integration-test'
        self.participant_id = 'SENSOR_REGISTRY_CUSTOM:43d8272a-2fba-b6bb-cb3a-657de9dcc4ef:streaming-integration-test'  # pylint: disable=line-too-long

        self.bq_client = bigquery.Client(project=GCP_PROJECT)

        self.creds = ds_sdk.core.gcp.credentials.DsSdkCredentials(
            runner='DirectRunner',
            service_account=
            'ds-sdk-readers@datascience-sdk.iam.gserviceaccount.com',  # pylint: disable=line-too-long
            billing_project='UNUSED')

        self.api_key = 'AIzaSyDdUP2S9PoUWnWu_KTX851ycB7AmH6TBdc'

    def pipeline_error_callback(self, exception: Exception):
        print(exception)
        raise ValueError(f'Pipeline failed: {exception}') from exception

    def run_pipeline(self):
        return sandwich_streaming_integration_test_pipeline.run_pipeline(
            registry='IntegrationTest',
            runner='DirectRunner',
            bq_table=self.output_table,
            api_key=self.api_key)

    def poll_results(self, output_table):
        query = (f'SELECT count(*) as count FROM {output_table} WHERE '
                 f'DataPoint.pressure = {self.unique_id}')
        start_time = time.time()
        while time.time() - start_time < MAX_WAIT_TIME:
            try:
                results = self.bq_client.query(query).result()
                if list(results)[0]['count'] == 1:
                    return True
            except Exception:  # pylint: disable=broad-except
                pass
            time.sleep(5)
        logging.error('Failed to find resuilts in BigQuery.')
        return False

    def write_to_sensor_store(self, unique_id):
        ss_client = sensor_store_client.SensorStoreClient(
            env='prod',
            creds=self.creds,
            api_key=self.api_key,
        )

        data_source_id = 123
        timestamp = timestamps.datetime_to_beam_timestamp(
            pd.Timestamp(time.time(), unit='s', tz='UTC'))
        print('%' * 10)
        print(timestamp)
        pressure = schemas.Pressure(
            pressure=unique_id,
            measurement_timestamp_utc=timestamp,
            data_point_metadata=schemas.data_point_metadata_for_raw_data(
                data_source_id=123,
                device_id=self.device_id,
                participant_id=self.participant_id,
                participant_namespace=enums_pb2.UserIdKeyspace.
                SENSOR_REGISTRY_CUSTOM,
                echo_metadata=None,
                sensor_store_metadata=None,
                annotation_labels=set()))
        data_source_cache = DataSourceCache({
            data_source_id:
                types_pb2.DataSource(
                    name='ds_sdk_integration',
                    device=types_pb2.Device(serial_number=self.device_id),
                    data_spec=types_pb2.DataSpec(
                        name='com.verily.pressure',
                        field_specs=[
                            types_pb2.DataFieldSpec(
                                name='pressure',
                                primitive=enums_pb2.PrimitiveType.INT64,
                                is_optional=False,
                                units=[
                                    types_pb2.Unit(
                                        type=enums_pb2.UnitType.PASCAL,
                                        scale=enums_pb2.UnitScale.UNIT,
                                        power=1,
                                        scale_factor=1)
                                ])
                        ]))
        })

        ss_client.write_data_point(pressure, data_source_cache)

    def test_simple_query(self):
        """Simply reads data into the SDK and outputs to BQ."""

        with multiprocessing.pool.ThreadPool(1) as pool:
            _ = pool.apply_async(self.run_pipeline,
                                 error_callback=self.pipeline_error_callback)
            # Sleep for 20 seconds to ensure pipeline is running before writing
            # to SensorStore.
            time.sleep(30)
            self.write_to_sensor_store(self.unique_id)
            results = self.poll_results(self.output_table)
            self.assertTrue(results)


if __name__ == '__main__':
    unittest.main()
