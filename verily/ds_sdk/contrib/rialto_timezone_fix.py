"""Transforms for fixing timezones reported by rialtos.

Rialtos are known for flipping timezones causing lots of incorrectly reported
timezones. The below transforms help smooth out this "incorrect" data.
"""

import collections
from typing import Any, Dict, Iterable, Tuple

import apache_beam as beam
from apache_beam.utils.timestamp import Timestamp
import immutabledict
import pandas as pd

from verily.ds_sdk.core.utils import timestamps
from verily.ds_sdk.core.utils import timezone_utils
from verily.ds_sdk.protos import types_pb2

# Mappings from Standard Offset Hours from UTC to readable codes, which enable
# the use of DST offsets in downstream pipelines, when applicable.
# Canonical names were chosen from STD times listed here:
# https://en.wikipedia.org/wiki/List_of_tz_database_time_zones#List
TIMEZONE_DICT = immutabledict.immutabledict({  # type: ignore
    '-11:00': 'Pacific/Pago_Pago',  # Note: Does not observe DST
    '-10:00': 'Pacific/Honolulu',  # Note: Does not observe DST
    '-09:30': 'Pacific/Marquesas',  # Note: Does not observe DST
    '-09:00': 'America/Anchorage',
    '-08:00': 'America/Los_Angeles',
    '-07:00': 'America/Denver',
    '-06:00': 'America/Chicago',
    '-05:00': 'America/New_York',
    '-04:00': 'America/Halifax',
    '-03:30': 'America/St_Johns',
    '-03:00': 'America/Sao_Paulo',  # Note: Does not observe DST
    '-02:30': 'America/St_Johns',
    '-02:00': 'America/Noronha',  # Note: Does not observe DST
    '-01:00': 'Atlantic/Azores',
    '+00:00': 'Europe/London',
    '+01:00': 'Europe/Paris',
    '+02:00': 'Europe/Athens',
    '+03:00': 'Europe/Moscow',  # Note: Does not observe DST
    '+03:30': 'Asia/Tehran',  # Note: Does not observe DST
    '+04:00': 'Asia/Dubai',  # Note: Does not observe DST
    '+04:30': 'Asia/Kabul',  # Note: Does not observe DST
    '+05:00': 'Asia/Karachi',  # Note: Does not observe DST
    '+05:30': 'Asia/Kolkata',  # Note: Does not observe DST
    '+05:45': 'Asia/Kathmandu',  # Note: Does not observe DST
    '+06:00': 'Asia/Dhaka',  # Note: Does not observe DST
    '+06:30': 'Asia/Yangon',  # Note: Does not observe DST
    '+07:00': 'Asia/Jakarta',  # Note: Does not observe DST
    '+08:00': 'Asia/Singapore',  # Note: Does not observe DST
    '+08:45': 'Australia/Eucla',  # Note: Does not observe DST
    '+09:00': 'Asia/Tokyo',  # Note: Does not observe DST
    '+09:30': 'Australia/Adelaide',
    '+10:00': 'Australia/Melbourne',
    '+10:30': 'Australia/Lord_Howe',  # Note: Observes only 30 minutes of DST
    '+11:00': 'Pacific/Guadalcanal',  # Note: Does not observe DST
    '+12:00': 'Pacific/Auckland',
    '+12:45': 'Pacific/Chatham',
    '+13:00': 'Pacific/Kanton',  # Note: Does not observe DST
    '+14:00': 'Pacific/Kiritimati'  # Note: Does not observe DST
})


def _generate_utc_offset_seconds_dict() -> Dict[float, str]:
    placeholder_timestamp = pd.Timestamp('2020-01-01', tz='UTC')
    output_dict = {}
    for utc_offset, timezone_name in TIMEZONE_DICT.items():
        tz = timezone_utils.convert_timezone(placeholder_timestamp, utc_offset)
        utc_offset_seconds = placeholder_timestamp.astimezone(
            tz).utcoffset().total_seconds()
        output_dict.update({utc_offset_seconds: timezone_name})
    return output_dict


# Similar to mappings above, but keys are compatible with utc_offset_seconds in
# below methods
UTC_OFFSET_SECONDS_TIMEZONE_DICT = _generate_utc_offset_seconds_dict()


class UtcOffsetMap:
    """Mapping from device -> utc offset.

    This class should only be constructed using the
    `BuildMostCommonUtcOffsetMap` below.
    """

    def __init__(self) -> None:
        self.device_time_map: Dict[str, float] = {}

    def get_utc_offset(self, device: str) -> Any:
        """Gets the most common utc offset for a device."""

        if device not in self.device_time_map:
            raise ValueError(f'No timezone found for: {device}')
        return self.device_time_map[device]

    def add_utc_offset(self, device: str, utc_offset: Any):
        """Adds a utc offset to the utc offset map."""
        if device in self.device_time_map:
            raise ValueError(f'{device} already found in utc offset map.')
        self.device_time_map[device] = utc_offset

    def __eq__(self, other: object) -> bool:
        if not isinstance(other, UtcOffsetMap):
            raise ValueError(
                'cannot compare UtcOffsetMap objects with arbitraty types.')
        return other.device_time_map == self.device_time_map

    def __repr__(self) -> str:
        return str(self.device_time_map)


def _key_utc_offset_by_device(
        elem: Tuple[Timestamp, types_pb2.DataSource]) -> Tuple[str, float]:
    timestamp, data_source = elem
    pd_timestamp = timestamps.beam_timestamp_to_pandas_timestamp(timestamp)
    tz = timezone_utils.convert_timezone(pd_timestamp,
                                         data_source.device.time_zone_name)
    utc_offset_seconds = pd_timestamp.astimezone(tz).utcoffset().total_seconds()
    return (data_source.device.serial_number, utc_offset_seconds)


def _max_count_utc_offset_per_key(timezones: Iterable[float]) -> float:
    utc_offset_count: Dict[float, int] = collections.defaultdict(int)
    for timezone in timezones:
        utc_offset_count[timezone] += 1
    return max(utc_offset_count, key=utc_offset_count.get)  # type: ignore


def _to_utc_offset_map(elem: Tuple[int, Iterable[Tuple[str, float]]],
                       return_readable_timezone: bool = False):
    utc_offset_map = UtcOffsetMap()
    _, max_utc_offset = elem
    for device, utc_offset in max_utc_offset:
        if return_readable_timezone:
            try:
                utc_offset_map.add_utc_offset(
                    device, UTC_OFFSET_SECONDS_TIMEZONE_DICT[utc_offset])
            except KeyError as e:
                raise ValueError(
                    f'UTC Offset of {utc_offset} seconds did not have a '
                    'readable timezone mapping.') from e
        else:
            utc_offset_map.add_utc_offset(device, utc_offset)
    return utc_offset_map


class BuildMostCommonUtcOffsetMap(beam.PTransform):
    """Transform for building a map of device to utc offset.

    The output of this PTransform should typically be used as a side input.
    """

    def __init__(self, return_readable_timezone: bool = False):
        super().__init__()
        self._return_readable_timezone = return_readable_timezone

    def expand(
        self, pcol: beam.PCollection[Tuple[Timestamp, types_pb2.DataSource]]
    ) -> beam.PCollection[UtcOffsetMap]:
        return (pcol | beam.Map(_key_utc_offset_by_device) | beam.CombinePerKey(
            _max_count_utc_offset_per_key  # type: ignore[arg-type]
        ) | beam.Map(lambda elem: (1, elem)) | beam.GroupByKey() | beam.Map(
            _to_utc_offset_map,
            return_readable_timezone=self._return_readable_timezone))
