"""Helper functions for handling timezones.

This library of helper functions assists working with timezone information
when you have simpler data types such as str or pd.Timestamp.
"""

import enum
import logging
import re
from typing import Text

import pandas as pd
import pytz


class TimezoneType(enum.Enum):
    NAMED_TZ = 1
    OFFSET_TZ = 2
    INVALID = 3


def _get_tz_type(time_zone: Text):
    """Determines if a timezone is a UTC offset or named timezone."""
    if time_zone in pytz.all_timezones_set:
        return TimezoneType.NAMED_TZ
    pattern = re.compile(r'^[+-][0-2]\d{1,2}:?[0-6]\d{1}')
    res = pattern.match(time_zone)
    if res:
        return TimezoneType.OFFSET_TZ
    return TimezoneType.INVALID


def get_date_in_timezone(utc_timestamp: pd.Timestamp, timezone: Text):
    """Convert a pandas Timestamp in UTC to a date (YYYY-MM-DD) in the timezone.

  Args:
    utc_timestamp: A pandas Timestamp object in UTC timezone.
    timezone: A string containing the timezone name or offset. This could be a
      location (i.e. 'US/Pacific') or an offset from UTC (i.e. '-05:00').

  Returns:
    A date (YYYY-MM-DD) constructed from the UTC Timestamp in the local
      timezone.
  """
    converted_tz_timestamp = convert_timestamp_timezone(utc_timestamp, timezone)
    offset_timestamp = utc_timestamp + converted_tz_timestamp.utcoffset()
    return offset_timestamp.strftime('%Y-%m-%d')


def convert_timezone(utc_timestamp: pd.Timestamp, timezone: Text):
    """Converts timezone so it can be used by Pandas timezone functions."""
    tz_type = _get_tz_type(timezone)
    # If location string (i.e. 'US/Pacific').
    if tz_type == TimezoneType.NAMED_TZ:
        return timezone
    # If UTC offset string (i.e. '-05:00').
    if tz_type == TimezoneType.OFFSET_TZ:
        timestamp_str = f'{str(utc_timestamp.tz_convert(None))} {timezone}'
        temp_timestamp = pd.to_datetime(timestamp_str)
        return temp_timestamp.tz
    # Default to UTC if invalid timezone is given.
    logging.warning('Failed to convert to timezone: %s. Defaulting to UTC.',
                    timezone)
    return 'UTC'


def convert_timestamp_timezone(utc_timestamp: pd.Timestamp, timezone: Text):
    """Convert a pandas Timestamp in UTC to a given timezone.

  Args:
    utc_timestamp: A pandas Timestamp object in UTC timezone.
    timezone: A string containing the timezone name or offset. This could be a
      location (i.e. 'US/Pacific') or an offset from UTC (i.e. '-05:00').

  Returns:
    A pandas Timestamp object in the given local timezone.
  """
    timezone = convert_timezone(utc_timestamp, timezone)
    if timezone == 'UTC':
        return utc_timestamp
    else:
        return utc_timestamp.tz_convert(timezone)


def time_string_to_int(time_string: Text):
    """Convert a timestamp encoded in a string to int64.

  Args:
    time_string: The timestamp string to convert.

  Returns:
    The milliseconds timestamp as an int64.
  """
    datetime = pd.to_datetime(time_string)
    return timestamp_to_ms(datetime)


def timestamp_to_ms(date_time: pd.Timestamp):
    """Converts a pandas Timestamp object to milliseconds.

  Args:
    date_time: The pandas Timestamp object to convert.

  Returns:
    The milliseconds timestamp as an int64
  """
    if pd.isnull(date_time):
        raise ValueError(f'Cannot convert null type date: {date_time}')

    if date_time.tzinfo is None:
        date_time = date_time.replace(tzinfo=pytz.utc)

    # Multiply by 1000 to convert from seconds to milliseconds.
    return int(date_time.timestamp() * 10e2)
