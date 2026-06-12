"""Util functions for caching DataSources."""

import json
from typing import Any, Dict

import farmhash
from google.protobuf import text_format
import numpy as np

from verily.ds_sdk.protos import enums_pb2
from verily.ds_sdk.protos import types_pb2


def _get_field_specs_in_echo_format(data_source: types_pb2.DataSource):
    """Converts DataFieldSpecs into the format echo stores them as."""
    strings = []
    for field_spec in data_source.data_spec.field_specs:
        # TODO: Find the method Echo uses to build these strs & replicate the
        # logic.
        s = text_format.MessageToString(field_spec, as_one_line=True)
        s = s.replace('.0 ', ' ')
        s = s.replace('units { ', 'units:<')
        s = s.replace(' } ', ' > ')
        s = s.replace('"', '\"')
        strings.append(s + ' ')
    return ', '.join(strings)


def _build_echo_data_source_dict(data_source):
    """Builds a DataSource (as a python Dict) in the Echo format."""
    return {
        'name': data_source.name,
        'application': {
            'id': enums_pb2.ApplicationId.Name(data_source.application.id),
            'version': data_source.application.version,
        },
        'device': {
            'serial_number': data_source.device.serial_number,
            'name': data_source.device.name,
            'hardware_version': data_source.device.hardware_version,
            'firmware_version': data_source.device.firmware_version,
            'software_version': data_source.device.software_version,
            'time_zone_name': data_source.device.time_zone_name,
            'manufacturer': data_source.device.manufacturer,
            'model': data_source.device.model,
            'os_version': data_source.device.os_version,
            'android_metadata': '',
        },
        'data_spec': {
            'name': data_source.data_spec.name,
            'field_specs': _get_field_specs_in_echo_format(data_source),
        },
        'sensor': {
            'id': data_source.sensor.id,
        },
        'algorithm': {
            'name': data_source.algorithm.name,
            'version': data_source.algorithm.version,
        }
    }


def hash_echo_data_source_dict(data_source_dict: Dict[str, Any]) -> int:
    json_str = json.dumps(data_source_dict).replace(': ',
                                                    ':').replace(', "', ',"')
    return int(np.uint64(farmhash.fingerprint64(json_str)).astype('int64'))


def hash_data_source_proto(data_source: types_pb2.DataSource) -> int:
    json_data = _build_echo_data_source_dict(data_source)
    return hash_echo_data_source_dict(json_data)
