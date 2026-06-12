"""Transform for grouping DataPoints & Annotations by Device / User / Time."""

import dataclasses
import datetime
import pickle
from typing import Any, Callable, Dict, Iterable, Optional, Tuple, Union

import apache_beam as beam
from apache_beam.utils.timestamp import Timestamp
import frozendict
import pandas as pd
import pytz

from verily.ds_sdk.contrib.rialto_timezone_fix import UtcOffsetMap
from verily.ds_sdk.core import schemas
from verily.ds_sdk.core.io.data_source_cache import DataSourceCache
from verily.ds_sdk.core.utils import timestamps
from verily.ds_sdk.core.utils import timezone_utils


def set_frozendict(frozen_dict, key, value):
    if hasattr(frozen_dict, 'set'):
        return frozen_dict.set(key, value)
    else:
        # This can happen for older versions of frozendict, such as the one in
        # g3.
        d = {key: value}
        return frozen_dict.copy(**d)


@dataclasses.dataclass(frozen=True)
class Key:
    """Key class for grouping together DataPoints & Annotations.

      Attributes:
        device_id: Unique identifier of a device in string format.
        participant_id: Unique participant id of the participant using the
            device.
        participant_namespace: Namespace for the participant_id.
        additional_keys: Any additional keys provided by the user.
    """

    device_id: Optional[str]
    participant_id: Optional[str]
    participant_namespace: Optional[int]
    additional_keys: frozendict.FrozenOrderedDict

    def __post_init__(self):
        if not isinstance(self.additional_keys, frozendict.FrozenOrderedDict):
            raise TypeError(
                '`additional_keys` must be an frozendict.FrozenOrderedDict to '
                'ensure deterministic encoding.')


class KeyCoder(beam.coders.Coder):
    """Coder for encoding Key objects."""

    def encode(self, k):
        return pickle.dumps(k)

    def decode(self, kb):
        return pickle.loads(kb)

    def is_deterministic(self):
        return True

    def to_type_hint(self):
        return Key


beam.coders.registry.register_coder(Key, KeyCoder)


class _AnnotationKeyFn(beam.DoFn):
    """Keys Annotations by device, participant, and/or additional functions."""

    def __init__(self, *, by_device: bool, by_participant: bool,
                 additional_key_fns: Dict[str, Callable]):
        super().__init__()
        self._by_device = by_device
        self._by_participant = by_participant
        self._additional_key_fns = additional_key_fns

    def process(  # type: ignore[override]
        self, annotation: schemas.Annotation) -> Iterable[Tuple[
            Key, schemas.Annotation]]:
        key_args: Dict[str, Any] = {
            'device_id': None,
            'participant_id': None,
            'participant_namespace': None,
        }
        if self._by_device:
            key_args['device_id'] = annotation.annotation_metadata.device_id
        if self._by_participant:
            key_args['participant_id'] = (
                annotation.annotation_metadata.participant_id)
            key_args['participant_namespace'] = annotation.annotation_metadata.participant_namespace  # pylint: disable=line-too-long
        additional_keys = {}
        for label, key_fn in self._additional_key_fns.items():
            additional_keys[label] = key_fn(annotation)
        key_args['additional_keys'] = frozendict.FrozenOrderedDict(
            additional_keys)

        yield (Key(**key_args), annotation)  # type: ignore


class KeyAnnotationsBy(beam.PTransform):
    """Keys Annotations by device, participant, and/or additional functions."""

    def __init__(self,
                 *,
                 by_device: bool,
                 by_participant: bool,
                 additional_key_fns: Optional[Dict[str, Callable]] = None):
        """Creates a KeyAnnotationsBy transform.

        At least one of the arguments to the constructor must be passed in.
        Args:
          by_device: Whether the output collection should be grouped by device.
          by_participant: Whether the output collection should be grouped by
            participant id and namespace.
          additional_key_fns: List of functions to extract fields to add to the
            key.
        """
        super().__init__()
        if (not by_device and not by_participant and
                additional_key_fns is None):
            raise ValueError(
                'One of by_device, by_participant, additional_key_fns must be '
                'given.')
        self._by_device = by_device
        self._by_participant = by_participant
        self._additional_key_fns = additional_key_fns or {}

    def expand(
        self, pcol: beam.PCollection[schemas.Annotation]
    ) -> beam.PCollection[Tuple[Key, schemas.Annotation]]:
        return (pcol | 'KeyAnnotations' >> beam.ParDo(
            _AnnotationKeyFn(by_device=self._by_device,
                             by_participant=self._by_participant,
                             additional_key_fns=self._additional_key_fns)))


def _attach_time_range_annotations(
    keyed_annotation: Tuple[Key, schemas.Annotation],
    beam_window_fn: beam.transforms.window.WindowFn,
    by_start_timestamp: bool,
    by_end_timestamp: bool,
    utc_offset_map: Optional[UtcOffsetMap] = None,
    timezone: Optional[datetime.tzinfo] = None,
) -> Iterable[Tuple[Key, schemas.Annotation]]:
    key, annotation = keyed_annotation

    if by_start_timestamp:
        timestamp = annotation.start_timestamp_utc
    if by_end_timestamp:
        timestamp = annotation.end_timestamp_utc

    if utc_offset_map is not None:
        utc_offset_hours = utc_offset_map.get_utc_offset(
            annotation.annotation_metadata.device_id) / 60
        timezone = pytz.FixedOffset(utc_offset_hours)

    if timezone is not None:
        pd_timestamp = timestamps.beam_timestamp_to_pandas_timestamp(timestamp)
        offset = pd_timestamp.astimezone(timezone).utcoffset().total_seconds()
        timestamp_seconds = pd_timestamp.timestamp() + offset
        timestamp = Timestamp(timestamp_seconds)

    context = beam.transforms.window.WindowFn.AssignContext(timestamp)
    time_range_keyed = []
    for window in beam_window_fn.assign(context):
        start = window.start
        end = window.end

        additional_keys = key.additional_keys
        additional_keys = set_frozendict(additional_keys,
                                         'start_time_range_micros',
                                         start.micros)
        additional_keys = set_frozendict(additional_keys,
                                         'end_time_range_micros', end.micros)

        new_key = Key(device_id=key.device_id,
                      participant_id=key.participant_id,
                      participant_namespace=key.participant_namespace,
                      additional_keys=additional_keys)
        time_range_keyed.append((new_key, annotation))
    return time_range_keyed


class KeyAnnotationsByParticipantDeviceTimeRange(beam.PTransform):
    """Keys annotations by participant, device, and time range."""

    def __init__(
        self,
        beam_window_fn: beam.transforms.window.WindowFn,
        by_start_timestamp: bool,
        by_end_timestamp: bool,
    ):
        super().__init__()

        if by_start_timestamp == by_end_timestamp:
            raise ValueError('One, and only one of, `by_start_timestamp` and '
                             '`by_end_timestamp` should be provided.')

        self._beam_window_fn = beam_window_fn
        self._by_start_timestamp = by_start_timestamp
        self._by_end_timestamp = by_end_timestamp
        self._kwargs: Dict[str, Any] = {}

    def expand(
        self, pcol: beam.PCollection[schemas.Annotation]
    ) -> beam.PCollection[Tuple[Key, schemas.Annotation]]:
        return (pcol | 'Key Annotations By Participant Device' >>
                KeyAnnotationsBy(by_device=True, by_participant=True) |
                'Attach Time Range To Key' >> beam.FlatMap(
                    _attach_time_range_annotations,
                    beam_window_fn=self._beam_window_fn,
                    by_start_timestamp=self._by_start_timestamp,
                    by_end_timestamp=self._by_end_timestamp,
                    **self._kwargs))


class KeyAnnotationsByParticipantDeviceTimeRangeInLocalTimezone(
        KeyAnnotationsByParticipantDeviceTimeRange):
    """Keys annotations by participant, device, and time range in local tz.

    Args:
        beam_window_fn (beam.transforms.window.WindowFn): Base window function
            to use when keying the Data Points
        by_start_timestamp (bool): Boolean indicating whether to window based on
            annotation start time
        by_end_timestamp (bool): Boolean indicating whether to window based on
            annotation end time
        utc_offset_map (verily.ds_sdk.contrib.rialto_timezone_fix.UtcOffsetMap):
            [Optional] mapping of device ids (str) to utc offset in seconds
            (float). This should be generated using
           verily.ds_sdk.contrib.rialto_timezone_fix.BuildMostCommonUtcOffsetMap

    NOTE: One and only one of by_start_timestamp or by_end_timestamp must be
        True otherwise and exception is raised.

    Input PCollection:
        verily.ds_sdk.core.schemas.annotation_schema.Annotation

    Output PCollection:
        Tuple[verily.ds_sdk.core.transforms.atomic.key_by.Key, Annotation]
        ->  Key.additional_keys.keys() = [
                'start_time_range_micros',
                'end_time_range_micros'
            ]
    """

    def __init__(self, beam_window_fn: beam.transforms.window.WindowFn,
                 by_start_timestamp: bool, by_end_timestamp: bool,
                 utc_offset_map: UtcOffsetMap):

        if by_start_timestamp == by_end_timestamp:
            raise ValueError('One, and only one of, `by_start_timestamp` and '
                             '`by_end_timestamp` should be provided.')
        super().__init__(beam_window_fn=beam_window_fn,
                         by_start_timestamp=by_start_timestamp,
                         by_end_timestamp=by_end_timestamp)

        self._kwargs.update({'utc_offset_map': utc_offset_map})


class KeyAnnotationsByParticipantDeviceTimeRangeInTimezone(beam.PTransform):
    """Keys annotations by participant, device, and time range."""

    def __init__(self, beam_window_fn: beam.transforms.window.WindowFn,
                 by_start_timestamp: bool, by_end_timestamp: bool,
                 timezone: datetime.tzinfo):
        super().__init__()

        if by_start_timestamp == by_end_timestamp:
            raise ValueError('One, and only one of, `by_start_timestamp` and '
                             '`by_end_timestamp` should be provided.')

        self._beam_window_fn = beam_window_fn
        self._by_start_timestamp = by_start_timestamp
        self._by_end_timestamp = by_end_timestamp
        self._timezone = timezone

    def expand(
        self, pcol: beam.PCollection[schemas.Annotation]
    ) -> beam.PCollection[Tuple[Key, schemas.Annotation]]:
        return (pcol | 'Key Annotations By Participant Device' >>
                KeyAnnotationsBy(by_device=True, by_participant=True) |
                'Attach Time Range To Key' >> beam.FlatMap(
                    _attach_time_range_annotations,
                    beam_window_fn=self._beam_window_fn,
                    by_start_timestamp=self._by_start_timestamp,
                    by_end_timestamp=self._by_end_timestamp,
                    timezone=self._timezone))


class _DataPointKeyFn(beam.DoFn):
    """Keys DataPoints by device, participant, and/or additional functions."""

    def __init__(self, *, by_device: bool, by_participant: bool,
                 additional_key_fns: Dict[str, Callable]):
        super().__init__()
        self._by_participant = by_participant
        self._by_device = by_device
        self._additional_key_fns = additional_key_fns

    def process(  # type: ignore[override]
        self, data_point: schemas.DataPointType
    ) -> Iterable[Tuple[Key, schemas.DataPointType]]:
        key_args: Dict[str, Any] = {
            'device_id': None,
            'participant_id': None,
            'participant_namespace': None,
        }
        if self._by_device:
            key_args['device_id'] = data_point.data_point_metadata.device_id
        if self._by_participant:
            key_args['participant_id'] = (
                data_point.data_point_metadata.participant_id)
            key_args['participant_namespace'] = data_point.data_point_metadata.participant_namespace  # pylint: disable=line-too-long
        additional_keys = {}
        for label, key_fn in self._additional_key_fns.items():
            additional_keys[label] = key_fn(data_point)
        key_args['additional_keys'] = frozendict.FrozenOrderedDict(
            additional_keys)

        yield (Key(**key_args), data_point)  # type: ignore


class KeyDataPointsBy(beam.PTransform):
    """Keys DataPoints by device, participant, and/or additional functions."""

    def __init__(self,
                 *,
                 by_device: bool,
                 by_participant: bool,
                 additional_key_fns: Optional[Dict[str, Callable]] = None):
        """Creates a KeyDataPointsBy transform.

        At least one of the arguments to the constructor must be passed in.
        Args:
          by_device: Whether the output collection should be grouped by device.
          by_participant: Whether the output collection should be grouped by
            participant id and namespace.
          additional_key_fns: List of functions to extract fields to add to the
            key.
        """
        super().__init__()
        if (not by_device and not by_participant and
                additional_key_fns is None):
            raise ValueError(
                'One of by_device, by_participant, additional_key_fns must be '
                'given.')
        self._by_device = by_device
        self._by_participant = by_participant
        self._additional_key_fns = additional_key_fns or {}

    def expand(
        self, pcol: beam.PCollection[schemas.DataPointType]
    ) -> beam.PCollection[Tuple[Key, schemas.DataPointType]]:
        return (pcol | 'KeyDataPoints' >> beam.ParDo(
            _DataPointKeyFn(by_device=self._by_device,
                            by_participant=self._by_participant,
                            additional_key_fns=self._additional_key_fns)))


def _attach_sensor_id_to_keyed_data_point(
        keyed_data_point: Tuple[Key, schemas.DataPointType],
        data_source_cache: DataSourceCache
) -> Tuple[Key, schemas.DataPointType]:
    key, data_point = keyed_data_point
    data_source = data_source_cache.get(
        data_point.data_point_metadata.data_source_id, None)
    if data_source is None:
        raise ValueError(
            f'No DataSource for {data_point} while looking up sensor.id')

    additional_keys = set_frozendict(key.additional_keys, 'sensor_id',
                                     data_source.sensor.id)

    key = Key(device_id=key.device_id,
              participant_id=key.participant_id,
              participant_namespace=key.participant_namespace,
              additional_keys=additional_keys)
    return (key, data_point)


class KeyDataPointsByParticipantDeviceSensor(beam.PTransform):
    """Keys DataPoints by device, participant, and sensor."""

    def __init__(self, data_source_cache: beam.pvalue.AsSingleton):
        super().__init__()

        self._data_source_cache = data_source_cache

    def expand(
        self, pcol: beam.PCollection[schemas.DataPointType]
    ) -> beam.PCollection[Tuple[Key, schemas.DataPointType]]:
        return (pcol | 'Key DataPoints By Participant Device' >>
                KeyDataPointsBy(by_device=True, by_participant=True) |
                'Attach SensorId To Key' >> beam.Map(
                    _attach_sensor_id_to_keyed_data_point,
                    data_source_cache=self._data_source_cache))


def _attach_time_range_data_points(
    keyed_data_point: Tuple[Key, schemas.DataPointType],
    beam_window_fn: beam.transforms.window.WindowFn,
    data_source_cache: Optional[DataSourceCache] = None,
    utc_offset_map: Optional[UtcOffsetMap] = None,
    timezone: Optional[Union[datetime.tzinfo, str]] = None,
) -> Iterable[Tuple[Key, schemas.DataPointType]]:
    key, data_point = keyed_data_point

    timestamp = data_point.measurement_timestamp_utc
    in_local = data_source_cache is not None or utc_offset_map is not None
    if in_local:
        if data_source_cache is not None and utc_offset_map is not None:
            raise ValueError(
                'Only one of data_source_cache or utc_offset_map must be '
                'provided when keying by a local timezone.')

        if data_source_cache is not None:
            data_source = data_source_cache.get(
                data_point.data_point_metadata.data_source_id)
            data_source_timezone = data_source.device.time_zone_name
            timezone = timezone_utils.convert_timezone(
                pd.Timestamp.now(tz=pytz.utc), data_source_timezone)

        elif utc_offset_map is not None:
            dev_offset = utc_offset_map.get_utc_offset(
                data_point.data_point_metadata.device_id)
            # Check if we are working with readable time zone code
            if isinstance(dev_offset, str):
                timezone = dev_offset
            else:  # otherwise is an integer in minutes
                utc_offset_hours = dev_offset / 60
                timezone = pytz.FixedOffset(utc_offset_hours)

    if timezone is not None:
        pd_timestamp = timestamps.beam_timestamp_to_pandas_timestamp(timestamp)
        offset = pd_timestamp.astimezone(timezone).utcoffset().total_seconds()
        timestamp_seconds = pd_timestamp.timestamp() + offset
        timestamp = Timestamp(timestamp_seconds)

    context = beam.transforms.window.WindowFn.AssignContext(timestamp)
    time_range_keyed = []
    for window in beam_window_fn.assign(context):
        additional_keys = key.additional_keys
        additional_keys = set_frozendict(additional_keys,
                                         'start_time_range_micros',
                                         window.start.micros)
        additional_keys = set_frozendict(additional_keys,
                                         'end_time_range_micros',
                                         window.end.micros)

        new_key = Key(device_id=key.device_id,
                      participant_id=key.participant_id,
                      participant_namespace=key.participant_namespace,
                      additional_keys=additional_keys)
        time_range_keyed.append((new_key, data_point))
    return time_range_keyed


class KeyDataPointsByParticipantDeviceTimeRange(beam.PTransform):
    """"Keys data points by participant, device, and time range.

    Args:
        beam_window_fn (beam.transforms.window.WindowFn): Base window function
        to use when keying the Data Points

    Input PCollection:
        verily.ds_sdk.core.schemas.shared_schemas.DataPointType

    Output PCollection:
        Tuple[verily.ds_sdk.core.transforms.atomic.key_by.Key, DataPointType]
        ->  Key.additional_keys.keys() = [
                'start_time_range_micros',
                'end_time_range_micros'
            ]
    """

    def __init__(
        self,
        beam_window_fn: beam.transforms.window.WindowFn,
    ):
        super().__init__()

        self._beam_window_fn = beam_window_fn
        self._kwargs: Dict[str, Any] = {}

    def expand(
        self, pcol: beam.PCollection[schemas.DataPointType]
    ) -> beam.PCollection[Tuple[Key, schemas.DataPointType]]:
        return (pcol | 'Key DataPoints By Participant Device' >>
                KeyDataPointsBy(by_device=True, by_participant=True) |
                'Attach Time Range To Key' >> beam.FlatMap(
                    _attach_time_range_data_points,
                    beam_window_fn=self._beam_window_fn,
                    **self._kwargs))


class KeyDataPointsByParticipantDeviceTimeRangeInLocalTimezone(
        KeyDataPointsByParticipantDeviceTimeRange):
    """Keys data points by participant, device, and time range in local tz.

    Args:
        beam_window_fn (beam.transforms.window.WindowFn): Base window function
            to use when keying the Data Points
        data_source_cache
            (verily.ds_sdk.core.io.data_source_cache.DataSourceCache):
            [Optional] mapping of data source ids (int) to DataSource objects
            which contain the time zone info for a given source. This should be
            obtained using
            verily.ds_sdk.core.sensors_io.SensorsIO.get_data_source_dict()
        utc_offset_map (verily.ds_sdk.contrib.rialto_timezone_fix.UtcOffsetMap):
            [Optional] mapping of device ids (str) to utc offset in seconds
            (float). This should be generated using
           verily.ds_sdk.contrib.rialto_timezone_fix.BuildMostCommonUtcOffsetMap

    NOTE: One and only one of data_source_cache or utc_offset_map must be
        provided. Providing neither or both with raise and exception.

    Input PCollection:
        verily.ds_sdk.core.schemas.shared_schemas.DataPointType

    Output PCollection:
        Tuple[verily.ds_sdk.core.transforms.atomic.key_by.Key, DataPointType]
        ->  Key.additional_keys.keys() = [
                'start_time_range_micros',
                'end_time_range_micros'
            ]
    """

    def __init__(self,
                 beam_window_fn: beam.transforms.window.WindowFn,
                 data_source_cache: Optional[DataSourceCache] = None,
                 utc_offset_map: Optional[UtcOffsetMap] = None):

        if ((data_source_cache is not None and utc_offset_map is not None) or
            (data_source_cache is None and utc_offset_map is None)):
            raise ValueError('One, and only one of, `data_source_cache` and '
                             '`utc_offset_map` should be provided.')

        super().__init__(beam_window_fn=beam_window_fn)
        self._kwargs.update({
            'data_source_cache': data_source_cache,
            'utc_offset_map': utc_offset_map
        })


class KeyDataPointsByParticipantDeviceTimeRangeInTimezone(
        KeyDataPointsByParticipantDeviceTimeRange):
    """Keys data points by participant, device, and time range in fixed tz.

    Args:
        beam_window_fn (beam.transforms.window.WindowFn): Base window function
            to use when keying the Data Points
        timezone (datetime.datetime.tzinfo): Time zone to convert all data point
            timestamps to

    Input PCollection:
        verily.ds_sdk.core.schemas.shared_schemas.DataPointType

    Output PCollection:
        Tuple[verily.ds_sdk.core.transforms.atomic.key_by.Key, DataPointType]
        ->  Key.additional_keys.keys() = [
                'start_time_range_micros',
                'end_time_range_micros'
            ]
    """

    def __init__(self,
                 beam_window_fn: beam.transforms.window.WindowFn,
                 timezone=datetime.tzinfo):

        super().__init__(beam_window_fn=beam_window_fn)
        self._kwargs.update({'timezone': timezone})


class KeyDataPointsByParticipantDeviceSensorTimeRange(beam.PTransform):
    """Keys DataPoints by device, participant, sensor, and time range."""

    def __init__(self, beam_window_fn: beam.transforms.window.WindowFn,
                 data_source_cache: beam.pvalue.AsSingleton):
        super().__init__()

        self._beam_window_fn = beam_window_fn
        self._data_source_cache = data_source_cache

    def expand(
        self, pcol: beam.PCollection[schemas.DataPointType]
    ) -> beam.PCollection[Tuple[Key, schemas.DataPointType]]:
        return (pcol | 'Key DataPoints By Participant Device' >>
                KeyDataPointsBy(by_device=True, by_participant=True) |
                'Attach SensorId To Key' >> beam.Map(
                    _attach_sensor_id_to_keyed_data_point,
                    data_source_cache=self._data_source_cache) |
                'Attach Time Range To Key' >> beam.FlatMap(
                    _attach_time_range_data_points,
                    beam_window_fn=self._beam_window_fn))


class KeyDataPointsByParticipantTimeRange(beam.PTransform):
    """Keys DataPoints by participant and time range (not by device)."""

    def __init__(self, beam_window_fn: beam.transforms.window.WindowFn):
        super().__init__()

        self._beam_window_fn = beam_window_fn

    def expand(
        self, pcol: beam.PCollection[schemas.DataPointType]
    ) -> beam.PCollection[Tuple[Key, schemas.DataPointType]]:
        return (pcol | 'Key DataPoints By Participant TimeRange' >>
                KeyDataPointsBy(by_device=False, by_participant=True) |
                'Attach Time Range To Key' >> beam.FlatMap(
                    _attach_time_range_data_points,
                    beam_window_fn=self._beam_window_fn))
