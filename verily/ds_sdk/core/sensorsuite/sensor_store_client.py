"""Client for calling SensorStore."""

import dataclasses
import json
import logging
import random
import time
import typing
from typing import Any, Dict, Iterable, List, Optional

import apache_beam as beam
from apache_beam.utils import timestamp
from frozendict import frozendict  # type: ignore
from google.protobuf import json_format
from googleapiclient import errors
from googleapiclient.discovery import build_from_document
import httplib2
import pandas as pd

from verily.ds_sdk.core import schemas
from verily.ds_sdk.core import studies
from verily.ds_sdk.core.gcp import credentials
from verily.ds_sdk.core.io.data_source_cache import DataSourceCache
from verily.ds_sdk.core.metrics import histogram
from verily.ds_sdk.core.sensorsuite import derived_data_sources
from verily.ds_sdk.core.sensorsuite import overwrite_keys
from verily.ds_sdk.core.utils import timestamps
from verily.ds_sdk.protos import types_pb2

_ENV_TO_ENDPOINT = frozendict({
    'preprod': 'preprod-lifesciencesensorstore.sandbox',
    'prod': 'lifesciencesensorstore',
    'prod-batch': 'prod-batch-lifesciencesensorstore.sandbox',
    'qa': 'staging-lifesciencesensorstore.sandbox',
    'autopush': 'autopush-lifesciencesensorstore.sandbox'
})

_SENSOR_STORE_RETRIES = 30
# Set the upper bound wait time between requests to ten minutes.
_RETRY_BACK_OFF_UPPER_BOUND_SECS = 60 * 10
# Only retry on specific errors
_ALLOWED_ERROR_CODES = [302, 405, 429, 500, 502, 503]

_PARTICIPANT_NAMESPACE_MAPPINGS = {
    'GAIA': 'GAIA_ID',
    'CSP': 'CSP_INTERNAL_ID',
}


class SensorStoreError(Exception):

    def __init__(self, message: str, device_id: str):
        super().__init__()
        self.message = message
        self.device_id = device_id

    def __str__(self) -> str:
        return (f'SensorStoreError: device_id: {self.device_id}. root error: '
                f'{self.message}')


class SensorStoreClient:
    """Client for making RPCs to SensorStore."""

    _discovery_document = None

    def __init__(self,
                 env: str,
                 creds: credentials.DsSdkCredentials,
                 api_key: str,
                 request_retry_timeout: pd.Timedelta = pd.Timedelta('1h')):
        creds = creds.get_impersonated_credentials()
        self.env = env

        if self._discovery_document is None:
            self._discovery_document = fetch_discovery_document(env, api_key)

        if self._discovery_document is not None:
            try:
                self._service = build_from_document(self._discovery_document,
                                                    credentials=creds,
                                                    developerKey=api_key)
            except KeyError as e:
                raise ValueError(
                    'failed to create sensor store client from discover doc: '
                    f'{self._discovery_document}') from e

        self._request_retry_timeout = request_retry_timeout

    def write_data_point(self, data_point: schemas.DataPointType,
                         data_source_cache: DataSourceCache):
        data_source = data_source_cache.get_data_source(
            data_point.data_point_metadata.data_source_id)
        data_source_http = json_format.MessageToDict(  # type: ignore[call-arg]
            data_source,
            preserving_proto_field_name=True,
            use_integers_for_enums=True,
            including_default_value_fields=True)
        data_source_http['application'] = {'id': 'STUDY_KIT'}
        # Remove this line after SensorStore supports
        # daylight_saving_time in prod
        data_source_http['device'].pop('daylight_saving_time', None)
        data_point_http = to_sensor_store_data_point(data_point)

        data_sets = [{
            'source': data_source_http,
            'data_points': [data_point_http],
        }]

        data_batch = {'dataSets': data_sets}

        method = self._service.data().write(body=data_batch)
        return self._run_method_with_retry(method)

    def overwrite_data_point_batches(self, data_spec_name: str,
                                     algorithm_name: str,
                                     algorithm_version: str,
                                     data_points: Iterable[
                                         schemas.DataPointType],
                                     data_source_cache: DataSourceCache,
                                     overwrite_key: overwrite_keys.OverwriteKey,
                                     study: str) -> Optional[Dict[Any, Any]]:
        study_info = studies.get_study_info(study, self.env)
        if study_info.registry_id is None:
            raise ValueError(
                'Unable to determine registry_id for study: '
                f'{study} and env: {self.env}'
            )
        registry = f'registries/{study_info.registry_id}'

        data_source_keyed_data_points: Dict[bytes, List[Dict[str, Any]]] = {}
        for data_point in data_points:
            data_source = data_source_cache.get_data_source(
                data_point.data_point_metadata.data_source_id)
            data_point_http = to_sensor_store_data_point(data_point)
            data_source_bytes = data_source.SerializeToString()
            if data_source_bytes in data_source_keyed_data_points:
                data_source_keyed_data_points[data_source_bytes].append(
                    data_point_http)
            else:
                data_source_keyed_data_points[data_source_bytes] = [
                    data_point_http
                ]

        data_sets = []
        for data_source_bytes, data_points_http in data_source_keyed_data_points.items():  # pylint: disable=line-too-long
            data_source = types_pb2.DataSource()
            data_source.ParseFromString(data_source_bytes)
            _, data_source = derived_data_sources.update_data_source_for_derived_data(  # pylint: disable=line-too-long
                data_source, data_spec_name, algorithm_name, algorithm_version)
            data_source_http = (
                json_format.MessageToDict(  # type: ignore[call-arg]
                    data_source,
                    preserving_proto_field_name=True,
                    use_integers_for_enums=True,
                    including_default_value_fields=True))
            data_source_http['application'] = {'id': 'STUDY_KIT'}
            # Remove this line after SensorStore supports
            # daylight_saving_time in prod
            data_source_http['device'].pop('daylight_saving_time', None)
            data_source_http['registry'] = registry
            data_sets.append({
                'source': data_source_http,
                'data_points': data_points_http
            })

        overwrite_token: Dict[str, Any] = {'overwrite_key': overwrite_key.key}
        if overwrite_key.version is not None:
            overwrite_token['source_data_version'] = overwrite_key.version

        data_batch = {'overwrite_token': overwrite_token, 'datasets': data_sets}
        http_body = {'new_data': [data_batch]}

        method = self._service.data().overwriteDataPointBatches(body=http_body)
        return self._run_method_with_retry(method)

    def list_defined_data_specs(self):
        method = self._service.definedDataSpecs().list(includeDefinitions=True)
        return self._run_method_with_retry(method)

    def read_data_points(self, participant_id: str, participant_namespace: str,
                         device_id: str, data_spec_name: str,
                         start_time: pd.Timestamp, end_time: pd.Timestamp):
        if participant_namespace in _PARTICIPANT_NAMESPACE_MAPPINGS:  # pylint: disable=consider-using-get
            participant_namespace = _PARTICIPANT_NAMESPACE_MAPPINGS[
                participant_namespace]

        def user_id():
            user_id_body = {'keyspace': participant_namespace}
            if participant_namespace == 'GAIA_ID':
                user_id_body['userId'] = int(participant_id)
            else:
                user_id_body['userString'] = participant_id
            return user_id_body

        http_body = {
            'userId': user_id(),
            'timeInterval': {
                'start': start_time.isoformat(),
                'end': end_time.isoformat(),
            },
            'dataSources': {
                'dataSpec': {
                    'name': data_spec_name,
                },
                'device': {
                    'serialNumber': device_id,
                }
            }
        }

        method = self._service.data().read(body=http_body)
        return self._run_method_with_retry(method)

    def _run_method_with_retry(self, method):
        # Track the number of attempts it took for the method to succeed.
        num_retries_histogram_metric = histogram.LinearHistogram(
            f'execute_{method.methodId}',
            'num_retries',
            num_buckets=_SENSOR_STORE_RETRIES,
            bucket_step=1)
        permanent_failure_metric = beam.metrics.Metrics.counter(
            f'execute_{method.methodId}', 'sensor_store_permanent_failures')

        method_start_time = pd.Timestamp.now()
        attempt = 0
        while True:
            try:
                res = method.execute()
                num_retries_histogram_metric.update(attempt)
                return res
            except Exception as e:  # pylint: disable=broad-except
                # Exit early for certain HTTP errors (i.e. unauthorized).
                if (isinstance(e, errors.HttpError) and
                        e.resp.status not in _ALLOWED_ERROR_CODES):
                    permanent_failure_metric.inc()
                    error_msg = (
                        f'Method {method.methodId} hit an unretryable error: '
                        f'{e}')
                    device_id = _parse_body(method.body, 'serial_number')
                    raise SensorStoreError(error_msg, device_id) from e

                # Exit the loop after the retry timeout is exceeded.
                if pd.Timestamp.now(
                ) - method_start_time > self._request_retry_timeout:
                    error_msg = (
                        f'Method {method.methodId} exceeded max retry timeout '
                        f'({self._request_retry_timeout}). error: {e}')
                    device_id = _parse_body(method.body, 'serial_number')
                    raise SensorStoreError(error_msg, device_id) from e

                # Increment counts and retry
                attempt += 1
                num_retries_histogram_metric.update(attempt)
                sleep_time = random.random() * 2**attempt
                if sleep_time > _RETRY_BACK_OFF_UPPER_BOUND_SECS:
                    sleep_time = _RETRY_BACK_OFF_UPPER_BOUND_SECS
                    # Spread out max retries between 10 and 15 minutes.
                    sleep_time = sleep_time * random.uniform(1.0, 1.5)
                logging.warning(
                    ('Method %s failed on attempt %s, sleeping for %f seconds '
                     'error: %s'), method.methodId, attempt, sleep_time, str(e))
                time.sleep(sleep_time)


def fetch_discovery_document(env: str, api_key: str) -> Dict[Any, Any]:
    if env not in _ENV_TO_ENDPOINT:
        supported = _ENV_TO_ENDPOINT.keys()
        raise ValueError(
            f'Unsupported environment: {env}. Supported environments: '
            f'{supported}')
    document_uri = f'https://{_ENV_TO_ENDPOINT[env]}.googleapis.com/$discovery/rest?version=v1&key={api_key}'  # pylint: disable=line-too-long
    for attempt in range(_SENSOR_STORE_RETRIES):
        try:
            h = httplib2.Http()
            _, content = h.request(document_uri)
            return json.loads(content)
        except json.decoder.JSONDecodeError:
            sleep_time = min(random.random() * 2**attempt,
                             _RETRY_BACK_OFF_UPPER_BOUND_SECS)
            time.sleep(sleep_time)
    raise RuntimeError(
        f'Failed to fetch discovery document after {_SENSOR_STORE_RETRIES} '
        'attempts.')


def is_type_or_optional(field_type, wanted_type) -> bool:
    return field_type in [wanted_type, Optional[wanted_type]]


def set_ss_value(ss_value: types_pb2.Value, ss_value_field: str,
                 value_to_set: Optional[Any]):
    if value_to_set is not None:
        setattr(ss_value, ss_value_field, value_to_set)


def set_ss_list_value(ss_value: types_pb2.Value, ss_value_field: str,
                      value_to_set: Optional[List[Any]]):
    if value_to_set is not None:
        ss_value_list = getattr(ss_value, ss_value_field)
        ss_value_list.values.extend(value_to_set)


def to_sensor_store_data_point(
        data_point: schemas.DataPointType) -> Dict[str, Any]:
    data_point_fields = dataclasses.fields(data_point)
    data_point_http_fields = []
    for field in data_point_fields:
        field_name = field.name
        field_type = field.type

        if field_name in ['data_point_metadata', 'measurement_timestamp_utc']:
            # Don't include metadata fields.
            continue

        field_value = getattr(data_point, field_name)
        if field_value is None:
            continue

        ss_value = types_pb2.Value(field_name=field_name)

        # TODO(dyke): we may need to eventually support things like numpy types
        # and converting them to the appropriate primitive type.
        # TODO(dyke): we may need to update this to support Iterable and Set in
        # addition to List.
        if is_type_or_optional(field_type, bool):
            set_ss_value(ss_value, 'boolean_value', field_value)
        elif is_type_or_optional(field_type, typing.List[bool]):
            set_ss_list_value(ss_value, 'boolean_list', field_value)
        elif is_type_or_optional(field_type, int):
            set_ss_value(ss_value, 'int64_value', field_value)
        elif is_type_or_optional(field_type, typing.List[int]):
            set_ss_list_value(ss_value, 'int64_list', field_value)
        elif is_type_or_optional(field_type, float):
            set_ss_value(ss_value, 'float64_value', field_value)
        elif is_type_or_optional(field_type, typing.List[float]):
            set_ss_list_value(ss_value, 'float64_list', field_value)
        elif is_type_or_optional(field_type, str):
            set_ss_value(ss_value, 'string_value', field_value)
        elif is_type_or_optional(field_type, typing.List[str]):
            set_ss_list_value(ss_value, 'string_list', field_value)
        elif is_type_or_optional(field_type, bytes):
            set_ss_value(ss_value, 'blob', field_value)
        elif is_type_or_optional(field_type, typing.List[bytes]):
            set_ss_list_value(ss_value, 'blob_list', field_value)
        # TODO(dyke): Currently we just convert timestamps to millis, this may
        # not be what the data spec actually expects. Maybe we should set the
        # data spec units to say we are providing millis? Also what if the
        # timestamp field was a float?
        elif is_type_or_optional(field_type, timestamp.Timestamp):
            set_ss_value(ss_value, 'int64_value',
                         timestamps.beam_timestamp_to_ms(field_value))
        elif is_type_or_optional(field_type, typing.List[timestamp.Timestamp]):
            ms_timestamps = None
            if field_value is not None:
                ms_timestamps = []
                for beam_timestamp in field_value:
                    ms_timestamps.append(
                        timestamps.beam_timestamp_to_ms((beam_timestamp)))
            set_ss_list_value(ss_value, 'int64_value', ms_timestamps)

        http_ss_value = json_format.MessageToDict(  # type: ignore[call-arg]
            ss_value,
            preserving_proto_field_name=True,
            use_integers_for_enums=True,
            including_default_value_fields=True)
        # There is a bug in MessageToDict where the int values are not
        # actually parsed to ints and left as strings.
        if 'int64_value' in http_ss_value:
            http_ss_value['int64_value'] = int(http_ss_value['int64_value'])
        if 'int64_list' in http_ss_value:
            http_ss_value['int64_list'] = [
                int(v) for v in http_ss_value['int64_list']
            ]
        data_point_http_fields.append(http_ss_value)

    data_point_http = {
        'fields':
            data_point_http_fields,
        'measurement_time_millis':
            timestamps.beam_timestamp_to_ms(data_point.measurement_timestamp_utc
                                           )
    }
    return data_point_http


def _parse_body(http_body: str, target: str, default: Any = ''):
    """Recursively searches a http body for the value of a target key."""

    def value_generator(container, target_key):
        if isinstance(container, dict):
            for k, v in container.items():
                if k == target_key:
                    yield v
                else:
                    yield from value_generator(v, target_key)
        elif isinstance(container, list):
            for item in container:
                yield from value_generator(item, target_key)

    matched_values = set(list(value_generator(json.loads(http_body), target)))
    if len(matched_values) > 1:
        logging.error('Found %s matches for %s: %s while parsing HTTP body',
                      len(matched_values), target, matched_values)
    elif len(matched_values) == 1:
        return list(matched_values)[0]
    return default
