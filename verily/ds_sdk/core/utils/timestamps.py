"""Utils for working with timestamps."""

import datetime
from typing import Any, Optional, Union

from apache_beam.utils.timestamp import Timestamp
import pandas as pd
import pytz

from verily.ds_sdk.core.utils import timezone_utils

_BQ_MAX_TIMESTAMP = 9223372036854776


def beam_timestamp_to_pandas_timestamp(
        beam_timestamp: Timestamp) -> pd.Timestamp:
    return pd.Timestamp(beam_timestamp.to_utc_datetime(), tz='UTC')


def datetime_to_beam_timestamp(timestamp: Optional[Union[datetime.datetime,
                                                         pd.Timestamp]],
                               allow_null: bool = False) -> Timestamp:
    """Takes a timestamp object and returns the corresponding beam timestamp.

    NOTE: This will work with pandas timestamp objects also.

    Raises:
        Raises a ValueError if the timezone is not equal to UTC or None.
    """
    # Check for None or pandas NaT (Not a Time)
    if timestamp is None or pd.isna(timestamp):
        if not allow_null:
            raise ValueError(
                'Encountered null timestamp while parsing Echo row.')
        return None  # type: ignore[return-value]

    if (timestamp.tzinfo is not None and timestamp.tzinfo != pytz.utc and
            timestamp.tzinfo != datetime.timezone.utc):
        raise ValueError(
            f'timestamp had a non UTC timezone set: {timestamp.tzinfo}. '
            'timestamp must be in UTC or not have a timezone set.')

    if isinstance(timestamp, datetime.datetime) and timestamp.tzinfo is None:
        timestamp = pytz.utc.localize(timestamp)
    if timestamp.tzinfo != pytz.utc:
        # NOTE: beam requires the timezone to be a pytz type.
        timestamp = timestamp.astimezone(pytz.utc)
    return Timestamp.from_utc_datetime(timestamp)


def parse_bigquery_timestamp(timestamp: Any,
                             allow_null: bool = False) -> Timestamp:
    """Parses a timestamp read in from BigQuery.

    If from the GCP source the data will already be in a datetime object.

    If from google3 the data is in microseconds.
    """
    if isinstance(timestamp, int):
        # BigQuery rounds up to the nearest nanosecond, which will cause an
        # OverflowError for max timestamps.
        if timestamp == _BQ_MAX_TIMESTAMP:
            timestamp = pd.Timestamp.max.tz_localize(pytz.utc)
        else:
            timestamp = pd.Timestamp(timestamp, unit='us', tz=pytz.utc)
    elif isinstance(timestamp, datetime.datetime):
        if timestamp.tzinfo is None:
            timestamp = pd.Timestamp(timestamp, tz=pytz.utc)
    return datetime_to_beam_timestamp(timestamp, allow_null)


def parse_sensor_store_timestamp(timestamp_str: str) -> Timestamp:
    """Parses a timestamp read in from SensorStore.

    Timestamps returned by SensorStore are strings formatted as
    `Y-m-d H:M:S.f Z` or strings in milliseconds.
    """
    if timestamp_str.isnumeric():
        timestamp = pd.Timestamp(int(timestamp_str), unit='ms', tz=pytz.utc)
    else:
        timestamp = ensure_utc_timestamp(pd.Timestamp(timestamp_str))
    return datetime_to_beam_timestamp(timestamp, allow_null=False)


def beam_timestamp_to_ms(beam_timestamp: Timestamp) -> int:
    """Converts beam timestamp to ms since epoch."""
    return timezone_utils.timestamp_to_ms(
        beam_timestamp_to_pandas_timestamp(beam_timestamp))


def ensure_utc_timestamp(pd_timestamp: pd.Timestamp) -> pd.Timestamp:
    """Ensures the pandas timestamp is set to UTC.

    Returns a timestamp object guaranteed to be set to UTC.

    Raises a ValueError if the timestamp is localized to a non-UTC timestamp.
    """
    if pd_timestamp.tzinfo == pytz.utc:
        return pd_timestamp
    elif pd_timestamp.tzinfo is None:
        return pd_timestamp.replace(tzinfo=pytz.UTC)
    elif pd_timestamp.utcoffset() == datetime.timedelta(0):
        # For somereason pandas sets the timezone to 'localtz()' even though the
        # UTC offest is zero. So if the utc offset is zero we'll set the
        # timezone to UTC.
        return pd_timestamp.tz_convert(pytz.utc)
    else:
        raise ValueError(
            f'Pandas timestamp had a local timezone: {pd_timestamp.tzinfo}')


def millis_to_utc_datetime(utc_timestamp_millis: int):
    """Converts UTC timestamp in milliseconds to a datetime.datetime"""
    return datetime.datetime.fromtimestamp(
        utc_timestamp_millis // 1000,
        tz=datetime.timezone.utc).replace(microsecond=utc_timestamp_millis %
                                          1000 * 1000)

def round_down_to_nearest_second(pd_timestamp: pd.Timestamp) -> pd.Timestamp:
    return pd.Timestamp(pd_timestamp.floor('s').to_pydatetime())

def round_up_to_nearest_second(pd_timestamp: pd.Timestamp) -> pd.Timestamp:
    return pd.Timestamp(pd_timestamp.ceil('s').to_pydatetime())
