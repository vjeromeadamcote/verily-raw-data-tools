"""Utility method for creating data sources."""

from typing import Tuple

from verily.ds_sdk.core.io.bigquery.utils import cache_utils
from verily.ds_sdk.protos import types_pb2

_DERIVED_DATA_SOURCE_NAME = 'Ds_Sdk_Derived'


def get_base_data_source(device_id: str) -> types_pb2.DataSource:
    return types_pb2.DataSource(
        name=_DERIVED_DATA_SOURCE_NAME,
        device=types_pb2.Device(serial_number=device_id))


def create_data_source(
        device_id: str, data_spec_name: str, algorithm_name: str,
        algorithm_version: str) -> Tuple[int, types_pb2.DataSource]:
    """Creates a DataSource with the provided data spec name and algo info.

    Returns the data source ID and the created DataSource.
    """
    data_source = types_pb2.DataSource(
        device=types_pb2.Device(serial_number=device_id),
        name=_DERIVED_DATA_SOURCE_NAME,
        algorithm=types_pb2.Algorithm(name=algorithm_name,
                                      version=algorithm_version),
        data_spec=types_pb2.DataSpec(name=data_spec_name))

    return cache_utils.hash_data_source_proto(data_source), data_source


def create_data_source_with_sensor(
        device_id: str, data_spec_name: str, algorithm_name: str,
        algorithm_version: str,
        sensor_id: str) -> Tuple[int, types_pb2.DataSource]:
    """Creates a DataSource with data spec name, algo info, and sensor ID.

    Returns the data source ID and the created DataSource.
    """

    data_source = types_pb2.DataSource(
        device=types_pb2.Device(serial_number=device_id),
        name=_DERIVED_DATA_SOURCE_NAME,
        algorithm=types_pb2.Algorithm(name=algorithm_name,
                                      version=algorithm_version),
        data_spec=types_pb2.DataSpec(name=data_spec_name),
        sensor=types_pb2.Sensor(id=sensor_id))

    return cache_utils.hash_data_source_proto(data_source), data_source


def create_data_source_with_timezone(
        device_id: str, data_spec_name: str, algorithm_name: str,
        algorithm_version: str,
        timezone: str) -> Tuple[int, types_pb2.DataSource]:
    """Creates a DataSource with data spec name, algo info, and timezone.

    Returns the data source ID and the created DataSource.
    """

    data_source = types_pb2.DataSource(
        device=types_pb2.Device(serial_number=device_id,
                                time_zone_name=timezone),
        name=_DERIVED_DATA_SOURCE_NAME,
        algorithm=types_pb2.Algorithm(name=algorithm_name,
                                      version=algorithm_version),
        data_spec=types_pb2.DataSpec(name=data_spec_name))

    return cache_utils.hash_data_source_proto(data_source), data_source


def create_data_source_with_timezone_and_sensor(
        device_id: str, data_spec_name: str, algorithm_name: str,
        algorithm_version: str, timezone: str,
        sensor_id: str) -> Tuple[int, types_pb2.DataSource]:
    """Creates a DataSource with data spec, algo info, timezone, and sensor_id.

    Returns the data source ID and the created DataSource.
    """

    data_source = types_pb2.DataSource(
        device=types_pb2.Device(serial_number=device_id,
                                time_zone_name=timezone),
        name=_DERIVED_DATA_SOURCE_NAME,
        algorithm=types_pb2.Algorithm(name=algorithm_name,
                                      version=algorithm_version),
        data_spec=types_pb2.DataSpec(name=data_spec_name),
        sensor=types_pb2.Sensor(id=sensor_id))

    return cache_utils.hash_data_source_proto(data_source), data_source


def update_data_source_for_derived_data(
        data_source: types_pb2.DataSource, data_spec_name: str,
        algorithm_name: str,
        algorithm_version: str) -> Tuple[int, types_pb2.DataSource]:
    """Updates a DataSource with the provided data spec name and algo info.

    Returns the data source ID and the created DataSource.
    """
    updated_data_source = types_pb2.DataSource()
    updated_data_source.CopyFrom(data_source)
    updated_data_source.name = _DERIVED_DATA_SOURCE_NAME
    updated_data_source.algorithm.CopyFrom(
        types_pb2.Algorithm(name=algorithm_name, version=algorithm_version))
    updated_data_source.data_spec.CopyFrom(
        types_pb2.DataSpec(name=data_spec_name))

    return cache_utils.hash_data_source_proto(
        updated_data_source), updated_data_source
