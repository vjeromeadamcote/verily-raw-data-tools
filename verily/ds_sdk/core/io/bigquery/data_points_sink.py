"""PTransform for writing data points to BigQuery."""

from collections import abc
import dataclasses
import logging
import math
import time
from typing import Any, Dict, Iterable, List, Tuple, Type, Union
import uuid

import apache_beam as beam
from apache_beam.utils import timestamp
import avro.schema as avro_schema_lib
import google.api_core.exceptions
from google.cloud import bigquery  # type: ignore
from google.protobuf import json_format
import numpy as np
import pandas as pd
import typing_inspect

from verily.ds_sdk.core import schemas
from verily.ds_sdk.core.gcp import credentials
from verily.ds_sdk.core.io.bigquery.utils import bigquery_sink_utils
from verily.ds_sdk.core.io.data_source_cache import DataSourceCache
from verily.ds_sdk.core.schemas import schema_utils
from verily.ds_sdk.core.sensorsuite import derived_data_sources
from verily.ds_sdk.core.utils import avro_utils
from verily.ds_sdk.core.utils import timezone_utils
from verily.ds_sdk.protos import types_pb2

DEVICE_ID_COLUMN = 'DeviceID'
MEASUREMENT_TIME_COLUMN = 'DataPointTime'
DATA_SOURCE_COLUMN = 'DataSource'
DATA_POINT_COLUMN = 'DataPoint'
_MAX_BQ_INSERT_ATTEMPTS = 10
_BACK_OFF_SECONDS = 5


def _iterable_to_avro(sub_fields: List[Type]) -> List[Any]:
    items = []
    for sub_field in sub_fields:
        items.extend(_get_avro_type(sub_field))
    if len(items) == 1:
        items = items[0]
    return [{'type': 'array', 'items': items}]


def _union_to_avro(sub_fields: List[Type]) -> List[Any]:
    items = []
    for sub_field in sub_fields:
        items.extend(_get_avro_type(sub_field))
    return items


def is_generic_type(type_hint) -> bool:
    return (typing_inspect.is_generic_type(type_hint) or
            typing_inspect.is_union_type(type_hint))


_GENERIC_TYPES_TO_AVRO_TYPE = {
    abc.Iterable: _iterable_to_avro,
    list: _iterable_to_avro,
    set: _iterable_to_avro,
    Union: _union_to_avro,
}


def _data_point_to_bigquery_dict(
        data_point: schemas.DataPointType,
        data_source_cache: DataSourceCache,
        preserve_timestamp_fields: bool = False) -> Dict[str, Any]:
    data_point_data = {}
    schema_fields = schema_utils.get_schema_fields(type(data_point))
    for field_name in schema_fields:
        if (field_name
                in [schema_utils.METADATA_FIELD, schema_utils.TIMESTAMP_FIELD]):
            continue
        field_value = getattr(data_point, field_name)
        if isinstance(field_value, float) and math.isnan(field_value):
            field_value = None
        if isinstance(field_value, set):
            field_value = list(field_value)
        if isinstance(field_value,
                      (np.float16, np.float32, np.float64, np.longdouble)):
            field_value = float(field_value)
        if isinstance(field_value, (np.int8, np.int16, np.int32, np.int64)):
            field_value = int(field_value)
        if isinstance(field_value, timestamp.Timestamp):
            if preserve_timestamp_fields:
                field_value = field_value.to_utc_datetime()
            else:
                field_value = int(field_value.micros / 1000)
        data_point_data[field_name] = field_value

    data_source = data_source_cache.get(
        data_point.data_point_metadata.data_source_id, None)  # type: ignore
    if data_source is None:
        data_source = types_pb2.DataSource(device=types_pb2.Device(
            serial_number=data_point.data_point_metadata.device_id))
    # TODO(tanke): Add an option for users to add these fields.
    _, data_source = derived_data_sources.update_data_source_for_derived_data(
        data_source, '', '', '')
    # We clear these fields since the DataSource may pulled from a source point.
    data_source.ClearField('application')
    data_source.ClearField('data_spec')
    data_source.ClearField('algorithm')

    data_source_dict = json_format.MessageToDict(  # type: ignore[call-arg]
        data_source,
        including_default_value_fields=True,
        preserving_proto_field_name=True,
        use_integers_for_enums=True)

    if preserve_timestamp_fields:
        measurement_time = data_point.measurement_timestamp_utc.to_utc_datetime(
        )
    else:
        measurement_time = timezone_utils.timestamp_to_ms(
            data_point.measurement_timestamp_utc.to_utc_datetime())

    return {
        DEVICE_ID_COLUMN: data_point.data_point_metadata.device_id,
        MEASUREMENT_TIME_COLUMN: measurement_time,
        DATA_SOURCE_COLUMN: data_source_dict,
        DATA_POINT_COLUMN: data_point_data
    }


def _get_avro_type(type_hint) -> List[Any]:
    if type_hint in bigquery_sink_utils.PY_TYPE_TO_AVRO_PRIMITIVE:
        return [bigquery_sink_utils.PY_TYPE_TO_AVRO_PRIMITIVE[type_hint]]
    elif type(None) == type_hint:  # pylint: disable=unidiomatic-typecheck
        # Disable pylint because isinstance doesn't work with nested types like:
        # Optional[List[int]]
        return ['null']
    elif is_generic_type(type_hint):
        type_args = typing_inspect.get_args(type_hint)
        origin_type = typing_inspect.get_origin(type_hint)
        if origin_type in _GENERIC_TYPES_TO_AVRO_TYPE:
            return _GENERIC_TYPES_TO_AVRO_TYPE[origin_type](type_args)
        else:
            raise ValueError(
                f'Unsupported generic type for writing to BigQuery: {type_hint}'
            )
    else:
        raise ValueError(
            f'Unsupported type for writing to BigQuery: {type_hint}')


# TODO(dyke/tanke): consider moving this somewhere closer to the schemas.
def _get_avro_schema(schema_class: Type[schemas.DataPointType]):
    names = avro_schema_lib.Names(  # type: ignore
        default_namespace=schema_class.__name__)
    data_source_schema = avro_utils.data_source_avro_schema(names)

    # Returns dict of field name and type
    schema_fields = dataclasses.fields(schema_class)
    data_point_fields = []
    for field in schema_fields:
        field_name = field.name
        field_type = field.type
        if (field_name
                in [schema_utils.METADATA_FIELD, schema_utils.TIMESTAMP_FIELD]):
            continue
        avro_field_type = _get_avro_type(field_type)
        if len(avro_field_type) == 1:
            avro_field_type = avro_field_type[0]
        data_point_fields.append({'name': field_name, 'type': avro_field_type})

    data_point_json_schema = {
        'name': DATA_POINT_COLUMN,
        'namespace': 'data_point_schema',
        'type': 'record',
        'fields': data_point_fields,
    }

    data_point_schema = avro_schema_lib.SchemaFromJSONData(
        data_point_json_schema, names).fullname

    fields = [{
        'name': DEVICE_ID_COLUMN,
        'type': 'string',
    }, {
        'name': MEASUREMENT_TIME_COLUMN,
        'type': {
            'type': 'long',
            'logicalType': 'timestamp-millis'
        },
    }, {
        'name': DATA_POINT_COLUMN,
        'type': data_point_schema
    }, {
        'name': DATA_SOURCE_COLUMN,
        'type': ['null', data_source_schema]
    }]

    avro_schema = {
        'namespace': 'ds_sdk.avro',
        'name': 'DsSdkAvroSchema',
        'type': 'record',
        'fields': fields
    }
    return avro_schema_lib.SchemaFromJSONData(avro_schema, names)


def _key_by_device_day(
    datum: schemas.DataPointType
) -> Tuple[Tuple[str, int], schemas.DataPointType]:
    device_id = datum.data_point_metadata.device_id
    timestamp_key = pd.Timestamp(
        datum.measurement_timestamp_utc.to_utc_datetime())
    return (device_id,
            timezone_utils.timestamp_to_ms(timestamp_key.floor('1d'))), datum


class WriteDataPointsToBigQuery(beam.PTransform):
    """Writes data points to BigQuery."""

    def __init__(
        self,
        *,
        table_id: str,
        project_id: str,
        schema: Type[schemas.DataPointType],
        creds: credentials.DsSdkCredentials,
        temp_gcs_bucket: str,
        data_source_cache: DataSourceCache,
        write_disposition: str = bigquery.WriteDisposition.WRITE_TRUNCATE,
        streaming: bool,
        bigquery_location: str,
    ):
        """Creates a WriteDataPointsToBigQuery PTransform.

    Args:
      table_id: The table ID to write data to. Of the format:
        project.dataset.table
      project_id: The GCP project ID to bill / store temp avro files in. If
        unset, defaults to using the project included in the table_id.
      schema: The schema that is being used to write to BigQuery.
      creds: Credentials to use for authentication.
      temp_gcs_bucket: The temporary GCS bucket to write files to.
      data_source_cache: Cache of data source objects.
      write_disposition: The write disposition to use when writing to BigQuery.
        Defaults to WRITE_TRUNCATE.
      streaming: Whether or not the pipeline is in streaming mode.
      bigquery_location: The BigQuery location to write to (EU | US).
    """
        super().__init__()
        self._table_id = table_id
        self._schema = schema
        self._creds = creds
        self._temp_gcs_bucket = temp_gcs_bucket
        self._data_source_cache = data_source_cache
        self._write_disposition = write_disposition
        self._streaming = streaming
        self._bigquery_location = bigquery_location

        if not project_id:
            self._project_id: str = self._table_id.split('.')[0]
        else:
            self._project_id = project_id

    def expand(
            self,
            pcol: beam.PCollection[schemas.DataPointType]) -> beam.PCollection:
        schema_utils.validate_required_fields(self._schema)

        avro_schema = _get_avro_schema(self._schema)

        bq_schema = avro_utils.avro_schema_to_bq_schema(avro_schema)

        bigquery_sink_utils.setup_bigquery_table(
            creds=self._creds,
            billing_project=self._project_id,
            bigquery_location=self._bigquery_location,
            table_id=self._table_id,
            partition_colum=MEASUREMENT_TIME_COLUMN,
            write_disposition=self._write_disposition,
            bq_schema=bq_schema)

        if self._streaming:
            return pcol | 'StreamingWriteToBigQuery' >> beam.ParDo(
                _StreamToBigQuery(table_id=self._table_id,
                                  bq_billing_project=self._project_id,
                                  bigquery_location=self._bigquery_location,
                                  creds=self._creds), self._data_source_cache)

        return (pcol | beam.Map(_key_by_device_day) | beam.GroupByKey() |
                beam.ParDo(_AvroToGcs(self._temp_gcs_bucket,
                                      self._project_id,
                                      avro_utils.to_json(avro_schema),
                                      ds_sdk_creds=self._creds),
                           data_source_cache=self._data_source_cache) |
                'GroupByFilePathLoad' >> beam.GroupByKey() | beam.ParDo(
                    bigquery_sink_utils.LoadGCSToBigQuery(
                        self._table_id,
                        bq_billing_project=self._project_id,
                        bigquery_location=self._bigquery_location,
                        ds_sdk_creds=self._creds,
                        write_disposition=self._write_disposition)))


class _AvroToGcs(beam.DoFn):
    """Writes a DPS in avro format to GCS.

  Returns a tuple of the format:

  (file_path_prefix, file_name)

  The entire file path can be computed with:
      os.path.join(file_path_prefix, file_name)
  """

    def __init__(
        self,
        gcs_bucket: str,
        gcs_project: str,
        avro_schema: Dict[str, Any],
        ds_sdk_creds: credentials.DsSdkCredentials,
    ):
        super().__init__()
        # GCS client requires no `gs://` prefix.
        self._gcs_bucket = gcs_bucket.replace('gs://', '')
        self._gcs_project = gcs_project
        self._gcs_object_prefix = uuid.uuid4().hex
        self._avro_schema = avro_schema
        self._creds = ds_sdk_creds

    def start_bundle(self):
        self._bucket = (bigquery_sink_utils.get_gcs_bucket_with_folder(
            self._creds, self._gcs_project, self._gcs_bucket,
            self._gcs_object_prefix))

    def process(  # type: ignore[override]
            self, keyed_data: Tuple[Tuple[str, int],
                                    Iterable[schemas.DataPointType]],
            data_source_cache: DataSourceCache):
        key, grouped_data = keyed_data
        device_id, timestamp_ms = key
        file_name = f'{device_id}_{timestamp_ms}.avro'

        datums_to_write = []
        for datum in grouped_data:
            to_write = _data_point_to_bigquery_dict(datum, data_source_cache)
            datums_to_write.append(to_write)

        yield bigquery_sink_utils.write_avro_to_gcs(
            bucket=self._bucket,
            file_name=file_name,
            folder_path=self._gcs_object_prefix,
            avro_schema=self._avro_schema,
            avro_datums=datums_to_write)


class _StreamToBigQuery(beam.DoFn):
    """Writes to BigQuery using streaming writes."""

    def __init__(
        self,
        table_id: str,
        creds: credentials.DsSdkCredentials,
        bq_billing_project: str,
        bigquery_location: str,
    ):
        super().__init__()
        self._table_id = table_id
        self._creds = creds
        self._bq_billing_project = bq_billing_project
        self._bigquery_location = bigquery_location

    def start_bundle(self):
        self._bigquery_client = bigquery_sink_utils.get_bigquery_client(
            self._creds, self._bq_billing_project, self._bigquery_location)
        self._table = self._bigquery_client.get_table(self._table_id)

    def process(  # type: ignore[override]
            self, elem: schemas.DataPointType,
            data_source_cache: DataSourceCache) -> Iterable[bool]:

        to_write = _data_point_to_bigquery_dict(elem,
                                                data_source_cache,
                                                preserve_timestamp_fields=True)

        num_tries = 0

        last_exception = None
        retry_write_failure_metric = beam.metrics.Metrics.counter(
            'write_to_bigquery', 'retryable_write_failure')
        giving_up_failure_metric = beam.metrics.Metrics.counter(
            'write_to_bigquery', 'giving_up_failure')
        while True:
            num_tries += 1
            if num_tries > _MAX_BQ_INSERT_ATTEMPTS:
                giving_up_failure_metric.inc()
                raise ValueError('Max BQ retry attempts reached, giving up.'
                                ) from last_exception
            try:
                errors = self._bigquery_client.insert_rows(
                    self._table, [to_write])

                if errors:
                    logging.error('BigQuery streaming writes failed: %s. ',
                                  errors)
                break
            except google.api_core.exceptions.RetryError as e:
                last_exception = e
                wait_time = num_tries * _BACK_OFF_SECONDS
                logging.warning(
                    'BigQuery request failed after internal retries with: %s'
                    ' waiting %s seconds.', e, wait_time)
                retry_write_failure_metric.inc()
                time.sleep(wait_time)

        return [True]
