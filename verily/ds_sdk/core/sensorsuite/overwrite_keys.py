"""Utilities for generating overwrite keys."""

import abc
from typing import NamedTuple, Optional, TypeVar

import pandas as pd

from verily.ds_sdk.core import schemas
from verily.ds_sdk.core.utils import timestamps
from verily.ds_sdk.protos import types_pb2


def _join_overwrite_key_components(*key_components: str):
    return ':'.join(key_components)


class OverwriteKey(NamedTuple):
    key: str
    version: Optional[int] = None


class OverwriteKeyGenerator:
    """Base class for generating overwrite keys for sensor store."""

    @abc.abstractmethod
    def generate_overwrite_key(
        self,
        data_point: schemas.DataPointType,
        algorithm_name: str,
        algorithm_version: str,
        data_source: Optional[types_pb2.DataSource],
    ) -> OverwriteKey:
        key = _join_overwrite_key_components(
            data_point.data_spec_from_decorator,  # type: ignore
            algorithm_name,
            algorithm_version)
        return OverwriteKey(key)


OverwriteKeyGeneratorType = TypeVar(  # pylint: disable=invalid-name
    'OverwriteKeyGeneratorType',
    bound=OverwriteKeyGenerator)


class OverWriteKeyGenerators:
    """Container class for holding implementations of OverwriteKeyGenerator."""

    class TimeWindow(OverwriteKeyGenerator):
        """Generates an OverwriteKey based on a timerange.

        Takes the provided time_window string and creates a time range like:
        [measurement_time.floor(time_window),
         measurement_time.ceil(time_window))

        Returns an overwrite key with the following format:

        {data_spec}:{algo_name}:{algo_version}:{start_time}-{end_time}
        """

        def __init__(self, time_window: str):
            self._time_window = time_window

        def generate_overwrite_key(
            self,
            data_point: schemas.DataPointType,
            algorithm_name: str,
            algorithm_version: str,
            data_source: Optional[types_pb2.DataSource],
        ) -> OverwriteKey:
            base_key = super().generate_overwrite_key(data_point,
                                                      algorithm_name,
                                                      algorithm_version,
                                                      data_source)

            measurement_time = timestamps.beam_timestamp_to_pandas_timestamp(
                data_point.measurement_timestamp_utc)
            start_time = measurement_time.floor(self._time_window)
            end_time = start_time + pd.Timedelta(self._time_window)

            return OverwriteKey(
                _join_overwrite_key_components(
                    base_key.key,
                    f'{start_time.timestamp()}-{end_time.timestamp()}'))

    class SensorTimeWindow(TimeWindow):
        """Generates an OverwriteKey based on a timerange and sensor ID.

        Returns an overwrite key with the following format:

        {data_spec}:{algo_name}:{algo_version}:{start_time}-{end_time}:{sensor_id}  # pylint: disable=line-too-long
        """

        def generate_overwrite_key(
            self,
            data_point: schemas.DataPointType,
            algorithm_name: str,
            algorithm_version: str,
            data_source: Optional[types_pb2.DataSource],
        ) -> OverwriteKey:
            base_key = super().generate_overwrite_key(data_point,
                                                      algorithm_name,
                                                      algorithm_version,
                                                      data_source)
            if data_source is None:
                # TODO(dyke): We should have a guide on how to attach data
                # source to data points.
                raise ValueError(
                    'No data source found when trying to generate overwrite key'
                    ' with sensor ID. Make sure you have attached a data source'
                    ' to your data point.')
            if not data_source.sensor.id:
                raise ValueError(
                    'Attempting to generate a sensor time range overwrite key '
                    'with no sensor ID.')

            return OverwriteKey(
                _join_overwrite_key_components(base_key.key,
                                               str(data_source.sensor.id)))

    class SensorTimeWindowWithWriteTimeVersion(SensorTimeWindow):
        """Generates an OverwriteKey based on a timerange, sensor ID and builds
        a version using SensorStore write time.

        Returns an overwrite key with the following format:

        key: {data_spec}:{algo_name}:{algo_version}:{start_time}-{end_time}:{sensor_id}  # pylint: disable=line-too-long
        version: data_point.data_point_metadata.sensor_store_write_time
        """

        def generate_overwrite_key(
            self,
            data_point: schemas.DataPointType,
            algorithm_name: str,
            algorithm_version: str,
            data_source: Optional[types_pb2.DataSource],
        ) -> OverwriteKey:
            base_key = super().generate_overwrite_key(data_point,
                                                      algorithm_name,
                                                      algorithm_version,
                                                      data_source)
            ss_metadata = data_point.data_point_metadata.sensor_store_metadata
            if ss_metadata is None:
                raise ValueError(
                    'Attempting to generate a sensor store write time overwrite'
                    ' version from None.')

            return OverwriteKey(base_key.key,
                                ss_metadata.sensor_store_write_time.micros)
