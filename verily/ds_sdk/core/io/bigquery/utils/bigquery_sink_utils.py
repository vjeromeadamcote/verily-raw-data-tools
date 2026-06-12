"""Utils for writing data to BigQuery."""

from collections import OrderedDict
import io
import json
import logging
import os
from typing import Any, Dict, Iterable, Tuple

import apache_beam as beam
from apache_beam.utils import timestamp
# NOTE: We have to import the submodules for avro in-order for it to work on
# Flume.
import avro.datafile
import avro.io
import avro.schema as avro_schema_lib
import google.api_core
from google.cloud import bigquery  # type: ignore
from google.cloud import storage  # type: ignore
import numpy as np

from verily.ds_sdk.core.gcp import credentials

PY_TYPE_TO_AVRO_PRIMITIVE = {
    int: 'long',
    str: 'string',
    float: 'double',
    bytes: 'bytes',
    bool: 'boolean',
    np.int64: 'long',
    np.int32: 'long',
    np.int16: 'long',
    np.int8: 'long',
    np.longdouble: 'double',
    np.float64: 'double',
    np.float32: 'double',
    np.float16: 'double',
    timestamp.Timestamp: {
        'type': 'long',
        'logicalType': 'timestamp-millis'
    }
}

PY_TYPE_PRIMITIVE_TO_AVRO_PRIMITIVE = {
    int: 'long',
    str: 'string',
    float: 'double',
    bytes: 'bytes',
    bool: 'boolean',
}


def get_gcs_client(ds_sdk_creds: credentials.DsSdkCredentials,
                   gcs_project: str) -> storage.Client:
    creds, _ = ds_sdk_creds.get_credentials()
    return storage.Client(project=gcs_project, credentials=creds)


def get_bigquery_client(ds_sdk_creds: credentials.DsSdkCredentials,
                        billing_project: str,
                        bigquery_location: str) -> bigquery.Client:
    creds, _ = ds_sdk_creds.get_credentials()
    return bigquery.Client(project=billing_project,
                           credentials=creds,
                           location=bigquery_location)


def _find_mismatching_avro_type(datum: Dict[str, Any], schema: Dict[str, Any],
                                prefix: str):
    avro_type_mapping = OrderedDict()
    for field in schema['fields']:
        field_name = field['name']
        if not prefix:
            key = field_name
        else:
            key = f'{prefix}.{field["name"]}'
        if not field_name in datum:
            if 'default' not in field:
                avro_type_mapping[key] = {
                    'python_type': 'MISSING',
                    'avro_type': field['type']
                }
        elif isinstance(datum[field_name], dict):
            nested_schema = field['type']
            if isinstance(nested_schema, list):
                nested_schema = list(
                    filter(lambda x: isinstance(x, dict), nested_schema))[0]
            nested_type_mapping = _find_mismatching_avro_type(
                datum[field_name], nested_schema, key)
            avro_type_mapping.update(nested_type_mapping)
        else:
            pytype = type(datum[field['name']])
            avro_mapping = PY_TYPE_PRIMITIVE_TO_AVRO_PRIMITIVE.get(pytype, None)
            # Exclude DataPointTime since it is special case.
            if ((avro_mapping is None or not avro_mapping in field['type']) and
                    key != 'DataPointTime'):
                avro_type_mapping[key] = {
                    'python_type': str(pytype),
                    'avro_type': field['type']
                }
    return avro_type_mapping


class LoadGCSToBigQuery(beam.DoFn):
    """Loads GCS files to BQ."""

    def __init__(self, table_id: str, bq_billing_project: str,
                 bigquery_location: str,
                 ds_sdk_creds: credentials.DsSdkCredentials,
                 write_disposition: str):
        super().__init__()
        self._table_id = table_id
        self._bq_billing_project = bq_billing_project
        self._bigquery_location = bigquery_location
        self._creds = ds_sdk_creds
        self._write_disposition = write_disposition

    def start_bundle(self):
        self._bigquery_client = get_bigquery_client(self._creds,
                                                    self._bq_billing_project,
                                                    self._bigquery_location)

    def process(  # type: ignore[override]
            self, keyed_data: Tuple[str, Iterable[str]]):
        # Input: [file_path, List[files in file path]]
        # We don't actually use the values since we glob upload all the files.
        file_path = keyed_data[0]
        globed_file_path = os.path.join(file_path, '*')
        try:
            load_job = self._bigquery_client.load_table_from_uri(
                globed_file_path,
                self._table_id,
                job_config=bigquery.LoadJobConfig(
                    autodetect=True,
                    source_format='AVRO',
                    use_avro_logical_types=True,
                    write_disposition=self._write_disposition))
        except google.api_core.exceptions.GoogleAPICallError as e:
            logging.error('API call error: %s', e)
            for individual_error in e.errors:
                logging.error('Error detail: %s', individual_error)
            raise ValueError(
                f'BigQuery load failed, see logs for more details. error: {e}'
            ) from e

        try:
            result = load_job.result()  # waits for job to complete
        except google.api_core.exceptions.GoogleAPIError as e:
            raise ValueError(f'BigQuery load failed {e}') from e

        yield (file_path, result.job_id)


def get_gcs_bucket_with_folder(creds: credentials.DsSdkCredentials,
                               project: str, bucket_name: str,
                               folder_name: str) -> Tuple[storage.Bucket, str]:
    """Sets up a GCS bucket for writing to BigQuery.

  Returns a reference to the bucket and the folder path within that bucket to
  write files to.
  """
    bucket_path = bucket_name.split('/')
    if not bucket_path:
        raise ValueError(f'Could not find bucket name from: {bucket_name}')
    bucket_name = bucket_path[0]

    gcs_client = get_gcs_client(creds, project)
    bucket = gcs_client.bucket(bucket_name)

    if len(bucket_path) > 1:
        bucket_folders = bucket_path[1:]
        bucket_folders.append(folder_name)
        folder_name = os.path.join(*bucket_folders)

    # Create an empty folder to store the temporary avro files.
    folder = bucket.blob(folder_name + '/')
    if not folder.exists():
        try:
            folder.upload_from_string('')
        except Exception as e:
            raise ValueError('failed to upload to GCS bucket') from e
    return bucket


def write_avro_to_gcs(
    *,
    bucket: storage.Bucket,
    file_name: str,
    folder_path: str,
    avro_schema: Dict[str, Any],
    avro_datums: Iterable[Dict[str, Any]],
) -> Tuple[str, str]:
    """Writes the avro_datums to GCS.

  Returns a tuple containing the GCS path to the file and the name of the
  object.
  """
    serialized_avro_schema = avro_schema_lib.SchemaFromJSONData(avro_schema)

    # Write avro to local bytes instead of a file. This is required since
    # borg is now diskless.
    in_mem_avro_data = io.BytesIO()
    avro_writer = avro.datafile.DataFileWriter(in_mem_avro_data,
                                               avro.io.DatumWriter(),
                                               serialized_avro_schema)

    for datum in avro_datums:
        try:
            avro_writer.append(datum)
        except avro.io.AvroTypeException as e:
            type_mappings = _find_mismatching_avro_type(datum, avro_schema, '')
            type_str = json.dumps(type_mappings, indent=4)
            error_msg = (
                'Error converting datum to avro schema. See mis-matched types: '
                f'\n{type_str}')
            raise ValueError((error_msg)) from e

    # Flush data so it is in the bytes IO object.
    avro_writer.flush()

    gcs_object_name = os.path.join(folder_path, file_name)
    blob = bucket.blob(gcs_object_name)
    try:
        blob.upload_from_string(in_mem_avro_data.getvalue())
    except Exception as e:
        raise ValueError('failed to upload to GCS bucket') from e

    return (f'gs://{os.path.join(bucket.name, folder_path)}', gcs_object_name)


def setup_bigquery_table(
    *,
    creds: credentials.DsSdkCredentials,
    billing_project: str,
    bigquery_location: str,
    table_id: str,
    partition_colum: str,
    write_disposition: str,
    bq_schema: Iterable[bigquery.SchemaField],
):
    bq_client = get_bigquery_client(creds, billing_project, bigquery_location)

    partition = bigquery.TimePartitioning(field=partition_colum)
    bq_table = bigquery.Table(table_id, bq_schema)
    bq_table.time_partitioning = partition

    try:
        fetched_table = bq_client.get_table(bq_table)
        # We only need to check the schema if we are appending to the table.
        if (write_disposition == bigquery.WriteDisposition.WRITE_APPEND and
                fetched_table.schema != bq_schema):
            raise ValueError(
                'If appending to an existing table the schemas must be '
                'identical.')
        elif (write_disposition == bigquery.WriteDisposition.WRITE_EMPTY and
              fetched_table.num_rows > 0):
            raise ValueError(
                f'Table `{table_id}` already exists and WRITE_EMPTY was '
                'provided, if you wish to overwrite the table set write '
                f'disposition to {bigquery.WriteDisposition.WRITE_TRUNCATE}, '
                'or if you wish to append set write disposition to '
                f'{bigquery.WriteDisposition.WRITE_APPEND}')
    except google.api_core.exceptions.NotFound:
        bq_client.create_table(bq_table)
