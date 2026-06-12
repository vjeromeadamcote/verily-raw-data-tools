"""PubSub source converting SensorStore upload notifications into DataPoints."""

import base64
import dataclasses
import logging
from typing import Any, Callable, Dict, Iterable, List, Optional, Set, Tuple

import apache_beam as beam
from apache_beam.io.gcp.pubsub import PubsubMessage
import frozendict
from google.protobuf import json_format
import pandas as pd
import redis

from verily.ds_sdk.core import conditions
from verily.ds_sdk.core import options
from verily.ds_sdk.core import schemas
from verily.ds_sdk.core.gcp import credentials
from verily.ds_sdk.core.io.bigquery.utils import cache_utils
from verily.ds_sdk.core.io.bigquery.utils import echo_utils
from verily.ds_sdk.core.sensorsuite import sensor_store_client
from verily.ds_sdk.core.transforms import key_by
from verily.ds_sdk.core.utils import timestamps
from verily.ds_sdk.protos import management_resources_pb2
from verily.ds_sdk.protos import types_pb2

_SENSOR_STORE_TYPE_TO_PARSING_FN: Dict[str, Callable[[Any], Any]] = {
    'int64Value':
        int,
    'int64List':
        lambda int_list: [int(val) for val in int_list],
    'float64Value':
        float,
    'float64List':
        lambda float_list: [float(val) for val in float_list],
    'stringValue':
        str,
    'stringList':
        lambda str_list: [str(val) for val in str_list],
    'booleanValue':
        lambda bool_str: bool_str == 'true',
    'booleanList':
        lambda bool_str_list: [val == 'true' for val in bool_str_list],
    'blobValue':
        base64.b64decode,
    'blobList':
        lambda blob_list: [base64.b64decode(val) for val in blob_list],
}


@dataclasses.dataclass(frozen=True)
class _SensorStoreReadRequest:
    """Stores the contents of a SensorStore Pub/Sub Upload Notification."""
    participant_id: str
    participant_namespace_str: str
    device_id: str
    data_spec_name: str
    start_time_iso_str: str
    end_time_iso_str: str

    def key(self) -> str:
        return '::'.join([
            self.participant_id, self.participant_namespace_str, self.device_id,
            self.data_spec_name, self.start_time_iso_str, self.end_time_iso_str
        ])

    @property
    def participant_namespace(self) -> int:
        return management_resources_pb2.Participant.ParticipantNamespace.Value(
            self.participant_namespace_str)

    @property
    def start_time(self) -> pd.Timestamp:
        return pd.Timestamp(self.start_time_iso_str)

    @property
    def end_time(self) -> pd.Timestamp:
        return pd.Timestamp(self.end_time_iso_str)


def _filter_pubsub_messages(pubsub_message: PubsubMessage,
                            condition: conditions.Condition,
                            requested_data_spec_names: Set[str]) -> bool:
    pubsub_data_spec_names = set(
        pubsub_message.attributes['dataSpecs'].split(','))
    return (condition.pubsub_condition(pubsub_message) and
            bool(requested_data_spec_names & pubsub_data_spec_names))


def _pubsub_message_to_ss_requests(
    pubsub_message: PubsubMessage,
    requested_data_spec_names: Set[str],
    split_duration: Optional[pd.Timedelta],
) -> Iterable[_SensorStoreReadRequest]:
    participant_id = pubsub_message.attributes['participantId']
    participant_namespace_str = pubsub_message.attributes[
        'participantNamespace']
    device_id = pubsub_message.attributes['deviceId']
    data_spec_names = set(pubsub_message.attributes['dataSpecs'].split(','))
    # This message would have been filtered out if the intersection is empty.
    data_spec_names = data_spec_names & requested_data_spec_names
    start_millis = int(pubsub_message.attributes['startMillis'])
    end_millis = int(pubsub_message.attributes['endMillis'])
    start_time = pd.Timestamp(start_millis, unit='ms', tz='UTC')
    end_time = pd.Timestamp(end_millis, unit='ms', tz='UTC')

    if split_duration is None:
        for data_spec_name in data_spec_names:
            yield _SensorStoreReadRequest(participant_id,
                                          participant_namespace_str,
                                          device_id, data_spec_name,
                                          start_time.isoformat(),
                                          end_time.isoformat())
    else:
        start_time = start_time.floor(split_duration)
        end_time = end_time.ceil(split_duration)
        split_start_time = start_time
        while split_start_time < end_time:
            split_end_time = split_start_time + split_duration
            for data_spec_name in data_spec_names:
                yield _SensorStoreReadRequest(participant_id,
                                              participant_namespace_str,
                                              device_id, data_spec_name,
                                              split_start_time.isoformat(),
                                              split_end_time.isoformat())
            split_start_time = split_end_time


class _SensorStoreRequestToDataPoints(beam.DoFn):
    """Reads DataPoints from SensorStore to build a DS Cache/DataPoint rows."""

    def __init__(self, registry: str, creds: credentials.DsSdkCredentials,
                 env: str, api_key: str, request_retry_timeout: pd.Timedelta,
                 redis_endpoint: Optional[str], group_returned_points: bool):
        super().__init__()

        self._registry = registry
        self._creds = creds
        self._env = env
        self._api_key = api_key
        self._request_retry_timeout = request_retry_timeout
        self._group_returned_points = group_returned_points

        self._redis_client = None
        self._redis_endpoint = redis_endpoint

        self._sensor_store_client = None

    def setup(self):
        super().setup()
        self._sensor_store_client = sensor_store_client.SensorStoreClient(
            self._env, self._creds, self._api_key, self._request_retry_timeout)
        if self._redis_endpoint is not None:
            host, port = self._redis_endpoint.split(':')
            self._redis_client = redis.Redis(host=host, port=int(port))

    def process(  # type: ignore[override]
            self, read_request: _SensorStoreReadRequest):
        response = self._sensor_store_client.read_data_points(  # type: ignore
            read_request.participant_id, read_request.participant_namespace_str,
            read_request.device_id, read_request.data_spec_name,
            read_request.start_time, read_request.end_time)

        if response is None:
            logging.warning(
                'SensorStore read_data_points request failed for %s.',
                read_request.device_id)
        elif 'dataSets' not in response:
            logging.warning(
                'No data points returned by SensorStore for device %s. '
                'This is likely an authentication issue.',
                read_request.device_id)
        else:
            schema = schemas.DATA_SPEC_NAME_TO_SCHEMA_CLASS[
                read_request.data_spec_name]
            for data_set in response['dataSets']:
                data_source_dict = echo_utils.to_snake_case(data_set['source'])
                data_source = json_format.ParseDict(data_source_dict,
                                                    types_pb2.DataSource(),
                                                    ignore_unknown_fields=True)

                data_source_id = cache_utils.hash_data_source_proto(data_source)
                if self._redis_client is not None:
                    self._redis_client.set(data_source_id,
                                           data_source.SerializeToString())

                # These vars are only used if `group_returned_points` is True.
                grouped_points = []
                # TODO(tanke): Add methods to allow the user to update
                # additional keys and only set data_source_id.
                additional_keys = {
                    'data_source_id':
                        data_source_id,
                    'sensor_id':
                        data_source.sensor.id,
                    'start_time_range_micros':
                        int(read_request.start_time.value // 1e3),
                    'end_time_range_micros':
                        int(read_request.end_time.value // 1e3),
                }
                group_key = key_by.Key(
                    device_id=read_request.device_id,
                    participant_id=read_request.participant_id,
                    participant_namespace=read_request.participant_namespace,
                    additional_keys=frozendict.FrozenOrderedDict(
                        additional_keys))

                for data_point in data_set['dataPoints']:

                    data_point_dict: Dict[str, Any] = {}

                    data_point_dict[
                        'measurement_timestamp_utc'] = timestamps.parse_sensor_store_timestamp(  # pylint: disable=line-too-long
                            data_point['measurementTimeMillis'])

                    data_point_dict[
                        'data_point_metadata'] = schemas.data_point_metadata_for_raw_data(  # pylint: disable=line-too-long
                            data_source_id=data_source_id,
                            device_id=read_request.device_id,
                            participant_id=read_request.participant_id,
                            participant_namespace=read_request.
                            participant_namespace,
                            echo_metadata=None,
                            sensor_store_metadata=schemas.SensorStoreMetadata(
                                sensor_store_write_time=timestamps.
                                parse_sensor_store_timestamp(
                                    data_point['writeTime'])),
                            annotation_labels=set())

                    for field in data_point['fields']:
                        field_name = field['fieldName']
                        del field['fieldName']
                        # There is only one other item in the dict: the value
                        # of the field.
                        value_type, unparsed_value = list(field.items())[0]
                        if isinstance(unparsed_value,
                                      dict) and 'values' in unparsed_value:
                            unparsed_value = unparsed_value['values']
                        parsing_fn = _SENSOR_STORE_TYPE_TO_PARSING_FN[
                            value_type]
                        data_point_dict[field_name] = parsing_fn(unparsed_value)

                    data_point = schema(**data_point_dict)
                    if self._group_returned_points:
                        grouped_points.append(data_point)
                    else:
                        yield data_point

                # This case is triggered when `group_returned_points` is True.
                if grouped_points:
                    yield (group_key, grouped_points)


class StreamingSensorStoreSource(beam.PTransform):
    """Reads from SensorStore and parses rows into DataPoints."""

    def __init__(self, *, data_spec_names: List[str],
                 source_options: options.StreamingSourceOptions,
                 pubsub_message_window_into: beam.WindowInto,
                 condition: Optional[conditions.Condition], registry: str,
                 creds: credentials.DsSdkCredentials, env: str, api_key: str,
                 request_retry_timeout: pd.Timedelta):
        super().__init__()

        self._data_spec_names = set(data_spec_names)
        self._pubsub_message_window_into = pubsub_message_window_into
        self._condition = condition
        self._registry = registry
        self._creds = creds
        self._env = env
        self._api_key = api_key
        self._request_retry_timeout = request_retry_timeout

        self._group_returned_points = source_options.group_returned_points

        self._redis_endpoint = None
        if source_options.cache_data_source:
            if source_options.redis_endpoint is None:
                raise ValueError(
                    'Cannot cache DataSources in streaming mode without a Redis'
                    ' instance. Either disable the cache or reach out to '
                    'sensors-infra for help setting up a Redis instance.')
            self._redis_endpoint = source_options.redis_endpoint

        self._ss_request_split_duration = (
            source_options.ss_request_split_duration)
        self._pubsub_source = beam.io.ReadFromPubSub(
            topic=source_options.topic,
            subscription=source_options.subscription,
            with_attributes=True)

    def expand(self, pcoll):

        messages = pcoll | 'Read PubSub Messages' >> self._pubsub_source
        if self._condition is not None:
            messages = (messages | 'Filter PubSub Messages' >> beam.Filter(
                _filter_pubsub_messages,
                condition=self._condition,
                requested_data_spec_names=self._data_spec_names))

        split_requests = (messages |
                          'PubSub To SensorStore Requests' >> beam.FlatMap(
                              _pubsub_message_to_ss_requests,
                              requested_data_spec_names=self._data_spec_names,
                              split_duration=self._ss_request_split_duration))

        # We need to dynamically change the output type based on whether or not
        # the source is grouping points or not.
        output_type = schemas.DataPointType
        if self._group_returned_points:
            output_type = Tuple[key_by.Key, List[schemas.DataPointType]]

        pcolls_keyed_by_data_spec_name = {}
        for data_spec_name in self._data_spec_names:
            pcolls_keyed_by_data_spec_name[data_spec_name] = (
                split_requests |
                f'Filter SensorStore Requests {data_spec_name}' >> beam.Filter(
                    lambda req, ds_name: req.data_spec_name == ds_name,
                    ds_name=data_spec_name) |
                f'Key SensorStore Requests {data_spec_name}' >>
                beam.Map(lambda req: (req.key(), req)) |
                f'Window SensorStore Requests {data_spec_name}' >>
                self._pubsub_message_window_into |
                f'Remove Key From SensorStore Requests {data_spec_name}' >>
                beam.Values()  # pylint: disable=no-value-for-parameter
                | f'Get Distinct SensorStore Requests {data_spec_name}' >>
                beam.Distinct()  # pylint: disable=no-value-for-parameter
                | f'Materialize Requests Into DataPoints {data_spec_name}' >>
                beam.ParDo(
                    _SensorStoreRequestToDataPoints(
                        self._registry, self._creds, self._env, self._api_key,
                        self._request_retry_timeout, self._redis_endpoint, self.
                        _group_returned_points)).with_output_types(output_type))

        return pcolls_keyed_by_data_spec_name
