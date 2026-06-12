"""Utilities for escaping and validating data spec names."""

import re
from typing import Text


def to_echo_name(data_spec_name: Text) -> Text:
    """Converts a data spec name in SensorStore format to the Echo format.

  Replaces all '.' with '_' and all '_' with '__'

  Args:
    data_spec_name: The name of the data spec to escape.

  Returns:
    The data spec name that is converted to the Echo format.
  """
    return data_spec_name.replace('_', '__').replace('.', '_')


def to_sensor_store_name(data_spec_name: Text) -> Text:
    """Converts a data spec name in Echo format to the SensorStore format.

  Only operates on data spec names in the non-Legacy Echo format. See above.

  Replaces all '_' with '.' and all '__' with '_'.

  Args:
    data_spec_name: The name of the data spec to escape.

  Returns:
    The data spec name that is converted to the sensor store format.
  """
    data_spec_name = re.sub('(?<!_)_(?!_)', '.', data_spec_name)
    return data_spec_name.replace('__', '_')


def validate_sensor_store_name(data_spec_name: Text):
    if data_spec_name.startswith('com_verily_'):
        raise ValueError(
            'Data spec name using legacy format please use the true data spec'
            'name. For example: `com.verily.heart_rate`. Data specs can be '
            'found at: go/sensors-data-specs-prod. Invalid dataspec: '
            f'{data_spec_name}')
