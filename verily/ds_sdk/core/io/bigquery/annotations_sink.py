"""PTransform for writing annotations to BigQuery.

TODO: add streaming BQ sink
"""

import dataclasses
from typing import Any, Dict, Iterable, Tuple
import uuid

import apache_beam as beam
import avro.schema as avro_schema_lib
from google.cloud import bigquery  # type: ignore
import pandas as pd

from verily.ds_sdk.core import schemas
from verily.ds_sdk.core.arrotolata import source as arro_source
from verily.ds_sdk.core.gcp import credentials
from verily.ds_sdk.core.io.bigquery.utils import bigquery_sink_utils
from verily.ds_sdk.core.utils import avro_utils
from verily.ds_sdk.core.utils import timezone_utils
from verily.ds_sdk.protos import management_resources_pb2


# TODO(dyke/tanke): consider moving this somewhere closer to the schemas.
def _get_avro_schema():
    """Returns the avros schema for this AnnotationSchema."""

    fields = [{
        'name': 'device_id',
        'type': 'string',
    }, {
        'name': 'start_timestamp_utc',
        'type': {
            'type': 'long',
            'logicalType': 'timestamp-millis'
        },
    }, {
        'name': 'end_timestamp_utc',
        'type': {
            'type': 'long',
            'logicalType': 'timestamp-millis'
        },
    }, {
        'name': 'annotation_label',
        'type': 'string',
    }, {
        'name': 'participant_id',
        'type': ['null', 'string'],
    }, {
        'name': 'participant_namespace',
        'type': ['null', 'string'],
    }, {
        'name': 'version_name',
        'type': 'string',
    }, {
        'name': 'version_number',
        'type': 'long',
    }]
    names = avro_schema_lib.Names()  # type: ignore

    metric_type_schema = {
        'name_space':
            'annotation_input_data_info',
        'name':
            'MetricType',
        'type':
            'record',
        'fields': [
            {
                'name': 'stream_item_type',
                'type': ['null', 'string'],
                'default': None,
            },
            {
                'name': 'derived_data_type',
                'type': ['null', 'string'],
                'default': None,
            },
            {
                'name': 'annotation_type',
                'type': ['null', 'string'],
                'default': None,
            },
        ]
    }
    metric_type_name = avro_schema_lib.SchemaFromJSONData(metric_type_schema,
                                                          names=names).fullname
    input_data_info_schema = {
        'namespace':
            'annotation_input_data_info',
        'name':
            'InputDataInfo',
        'type':
            'record',
        'fields': [{
            'name': 'version_number',
            'type': ['null', 'long'],
        }, {
            'name': 'version_name',
            'type': ['null', 'string']
        }, {
            'name': 'metric_type',
            'type': metric_type_name
        }]
    }
    input_data_info_name = avro_schema_lib.SchemaFromJSONData(
        input_data_info_schema, names=names).fullname
    fields.append({
        'name': 'input_data_info',
        'type': {
            'type': 'array',
            'items': input_data_info_name
        },
    })
    schema = {
        'namespace': 'ds_sdk.avro',
        'name': 'AnnotationSchemaAvro',
        'type': 'record',
        'fields': fields
    }

    return avro_schema_lib.SchemaFromJSONData(schema, names=names)


AVRO_SCHEMA = _get_avro_schema()
BIGQUERY_SCHEMA = avro_utils.avro_schema_to_bq_schema(AVRO_SCHEMA)


def _annotation_to_bigquery_dict(
        annotation: schemas.Annotation) -> Dict[str, Any]:
    bigquery_row = {
        'device_id':
            annotation.annotation_metadata.device_id,
        'start_timestamp_utc':
            timezone_utils.timestamp_to_ms(
                annotation.start_timestamp_utc.to_utc_datetime()),
        'end_timestamp_utc':
            timezone_utils.timestamp_to_ms(
                annotation.end_timestamp_utc.to_utc_datetime()),
        'annotation_label':
            annotation.annotation_label,
        'participant_id':
            annotation.annotation_metadata.participant_id,
        'participant_namespace':
            annotation.annotation_metadata.participant_namespace,
        'version_name':
            annotation.annotation_metadata.version_name,
        'version_number':
            annotation.annotation_metadata.version_number
    }

    bigquery_row[
        'participant_namespace'] = management_resources_pb2.Participant.ParticipantNamespace.Name(  # pylint: disable=line-too-long
            annotation.annotation_metadata.participant_namespace  # type: ignore
        )

    input_data_infos = []
    for input_data_info in annotation.annotation_metadata.input_data_info:
        input_data_info_dict = dataclasses.asdict(input_data_info)
        input_data_info_dict['metric_type'] = dataclasses.asdict(
            input_data_info.metric_type)
        if input_data_info.metric_type.derived_data_type is not None:
            derived_data_type = arro_source.DerivedMetricType(
                input_data_info.metric_type.derived_data_type)
            input_data_info_dict['metric_type']['derived_data_type'] = (
                derived_data_type.name)
        if input_data_info.metric_type.stream_item_type is not None:
            stream_item_type = arro_source.DeviceMeasurementType(
                input_data_info.metric_type.stream_item_type)
            input_data_info_dict['metric_type']['stream_item_type'] = (
                stream_item_type.name)
        if input_data_info.metric_type.annotation_type is not None:
            annotation_type = arro_source.AnnotationType(
                input_data_info.metric_type.annotation_type)
            input_data_info_dict['metric_type']['annotation_type'] = (
                annotation_type.name)
        input_data_infos.append(input_data_info_dict)

    bigquery_row['input_data_info'] = input_data_infos

    return bigquery_row


def _key_by_device_day(
        datum: schemas.Annotation
) -> Tuple[Tuple[str, int], schemas.Annotation]:
    device_id = datum.annotation_metadata.device_id
    timestamp_key = pd.Timestamp(datum.start_timestamp_utc.to_utc_datetime())
    return (device_id,
            timezone_utils.timestamp_to_ms(timestamp_key.floor('1d'))), datum


class WriteAnnotationsToBigQuery(beam.PTransform):
    """Writes annotations to BigQuery."""

    def __init__(
        self,
        *,
        table_id: str,
        creds: credentials.DsSdkCredentials,
        temp_gcs_bucket: str,
        write_disposition: str = bigquery.WriteDisposition.WRITE_TRUNCATE,
        bigquery_location: str,
    ):
        """Creates a WriteAnnotationsToBigQuery PTransform.

    Args:
      table_id: The table ID to write data to. Of the format:
        project.dataset.table
      creds: Credentials to use for authentication.
      temp_gcs_bucket: The temporary GCS bucket to write files to.
      write_disposition: The write disposition to use when writing to BigQuery.
        Defaults to WRITE_TRUNCATE.
      bigquery_location: The BigQuery location to write to (EU | US).
    """
        super().__init__()
        self._table_id = table_id
        self._project_id = self._table_id.split('.')[0]
        self._creds = creds
        self._temp_gcs_bucket = temp_gcs_bucket
        self._write_disposition = write_disposition
        self._bigquery_location = bigquery_location

    def expand(self,
               pcol: beam.PCollection[schemas.Annotation]) -> beam.PCollection:
        bigquery_sink_utils.setup_bigquery_table(
            creds=self._creds,
            billing_project=self._project_id,
            bigquery_location=self._bigquery_location,
            table_id=self._table_id,
            partition_colum='start_timestamp_utc',
            write_disposition=self._write_disposition,
            bq_schema=BIGQUERY_SCHEMA)
        return (pcol | beam.Map(_key_by_device_day) | beam.GroupByKey() |
                beam.ParDo(
                    _AvroToGcs(self._temp_gcs_bucket,
                               self._project_id,
                               avro_utils.to_json(AVRO_SCHEMA),
                               ds_sdk_creds=self._creds)) |
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
                                    Iterable[schemas.Annotation]]):
        key, grouped_data = keyed_data
        device_id, timestamp_ms = key
        file_name = f'annotations_{device_id}_{timestamp_ms}.avro'
        datums_to_write = []
        for datum in grouped_data:
            datums_to_write.append(_annotation_to_bigquery_dict(datum))

        yield bigquery_sink_utils.write_avro_to_gcs(
            bucket=self._bucket,
            file_name=file_name,
            folder_path=self._gcs_object_prefix,
            avro_schema=self._avro_schema,
            avro_datums=datums_to_write)
