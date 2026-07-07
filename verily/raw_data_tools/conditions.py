"""Condition provides classes for conditions on fetching data from Echo."""

import abc
import collections
import dataclasses
import datetime
import enum
import functools
import operator
import re
from typing import Any, Iterable, List, Optional

from apache_beam.io.gcp.pubsub import PubsubMessage
import pandas as pd

from verily.raw_data_tools.schemas import schemas
from verily.raw_data_tools.utils.data_source_cache import DataSourceCache
from verily.raw_data_tools.utils import timestamps
from verily.raw_data_tools.protos import enums_pb2


def _to_ordered_set(items: Iterable[Any]):
    dictionary = {item: None for item in items}
    return list(collections.OrderedDict(dictionary).keys())


@dataclasses.dataclass
class TimeRange:
    """Encapsulates an open-closed time range [start_time, to end_time)."""
    # The start of the time range.
    start_time: datetime.datetime
    # The end of the time range.
    end_time: datetime.datetime

    def __post_init__(self):
        self.start_time = timestamps.ensure_utc_timestamp(self.start_time)
        self.end_time = timestamps.ensure_utc_timestamp(self.end_time)


class Condition(metaclass=abc.ABCMeta):
    """Condition is a constraint on data to fetch from the SensorSuite SDK."""

    # A condition is applied either against an SDK schema or a raw schema. A
    # condition needs to be specific (e.g. with regards to column names) to that
    # schema.
    #
    # If you are working with an SDK schema, the canonical reference is in the
    # corresponding *_schema.py file (e.g. data_point_schema.py). The schema
    # definition is there and the __init__() describes the available fields. See
    # also go/data-point-schema.
    #
    # If you are working with an Echo ("raw") schema, the canonical reference is
    # in GCP BigQuery. We recommend going to the console and viewing the table
    # schema.

    @abc.abstractmethod
    def pubsub_condition(self, pubsub_message: PubsubMessage) -> bool:
        pass

    @abc.abstractmethod
    def data_points_condition(
        self,
        table: Any,
        include_annotation_conditions: bool = True,
    ) -> Optional[Any]:
        """SQL WHERE clause applying the condition on a data points table.

        Args:
        table: The ibis table that the condition will be applied to.
        include_annotation_conditions: Whether to include conditions applying to
            annotations or not.
        """
        pass

    @abc.abstractmethod
    def annotations_condition(
        self,
        table: Any,
        include_annotation_conditions: bool = True,
    ) -> Optional[Any]:
        """SQL WHERE clause applying the condition on an annotations table."""
        pass

    @abc.abstractmethod
    def data_point_row_condition(self, row: schemas.DataPointType,
                                 data_source_cache: DataSourceCache) -> bool:
        """Evaluates the condition on the data point row objects."""
        pass

    def applies_to_annotations(self) -> bool:
        """Whether or not the condition applies to querying annotations."""
        return True

    def __and__(self, condition: 'Condition'):
        return AndCondition(conditions=_to_ordered_set([self, condition]))

    def __or__(self, condition: 'Condition'):
        return OrCondition(conditions=_to_ordered_set([self, condition]))

    def __neg__(self):
        return NegateCondition(self)

    def __eq__(self, cond) -> bool:
        return str(cond) == str(self)

    def __hash__(self):
        return hash(str(self))


class AndCondition(Condition):
    """AndCondition holds a set of conditions which must all be satisfied."""

    def __init__(self, conditions: Iterable[Condition]):
        """Constructs an AndCondition object.

    Args:
      conditions: The set of conditions to AND together.
    """
        super().__init__()
        self.conditions = _to_ordered_set(conditions)

    def pubsub_condition(self, pubsub_message: PubsubMessage) -> bool:
        ret_value = True
        for condition in self.conditions:
            ret_value = ret_value & condition.pubsub_condition(pubsub_message)
        return ret_value

    def data_points_condition(
        self,
        table: Any,
        include_annotation_conditions: bool = True,
    ) -> Optional[Any]:
        data_point_conditions = []
        for cond in self.conditions:
            where_clause = cond.data_points_condition(
                table, include_annotation_conditions)
            if where_clause is not None:
                data_point_conditions.append(where_clause)
        if data_point_conditions:
            return functools.reduce(operator.and_, data_point_conditions)
        return None

    def annotations_condition(
        self,
        table: Any,
        include_annotation_conditions: bool = True
    ) -> Optional[Any]:
        annotation_conditions = []
        for cond in self.conditions:
            if cond.applies_to_annotations:
                where_clause = cond.annotations_condition(
                    table, include_annotation_conditions)

            if where_clause is not None:
                annotation_conditions.append(where_clause)
        if annotation_conditions:
            return functools.reduce(operator.and_, annotation_conditions)
        return None

    def data_point_row_condition(self, row: schemas.DataPointType,
                                 data_source_cache: DataSourceCache) -> bool:
        ret_value = True
        for condition in self.conditions:
            ret_value = (ret_value & condition.data_point_row_condition(
                row, data_source_cache))
        return ret_value

    def __and__(self, cond: Condition):
        return AndCondition(self.conditions + [cond])

    def __repr__(self) -> str:
        cond_str = ' and '.join([str(cond) for cond in self.conditions])
        return f'({cond_str})'


class OrCondition(Condition):
    """OrCondition holds a set of conditions of which one must be satisfied."""

    def __init__(self, conditions: Iterable[Condition]):
        """Constructs an OrCondition object.

        Args:
        conditions: The set of conditions to OR together.
        """
        super().__init__()
        self.conditions = _to_ordered_set(conditions)

    def pubsub_condition(self, pubsub_message: PubsubMessage) -> bool:
        ret_value = False
        for condition in self.conditions:
            ret_value = ret_value | condition.pubsub_condition(pubsub_message)
        return ret_value

    def data_points_condition(
        self,
        table: Any,
        include_annotation_conditions: bool = True,
    ) -> Optional[Any]:
        data_point_conditions = []
        for cond in self.conditions:
            where_clause = cond.data_points_condition(
                table, include_annotation_conditions)
            if where_clause is not None:
                data_point_conditions.append(where_clause)

        if data_point_conditions:
            return functools.reduce(operator.or_, data_point_conditions)
        return None

    def annotations_condition(
        self,
        table: Any,
        include_annotation_conditions: bool = True
    ) -> Optional[Any]:
        annotation_conditions = []
        for cond in self.conditions:
            if cond.applies_to_annotations:
                where_clause = cond.annotations_condition(
                    table, include_annotation_conditions)

            if where_clause is not None:
                annotation_conditions.append(where_clause)
        if annotation_conditions:
            return functools.reduce(operator.or_, annotation_conditions)
        return None

    def data_point_row_condition(self, row: schemas.DataPointType,
                                 data_source_cache: DataSourceCache) -> bool:
        ret_value = False
        for condition in self.conditions:
            ret_value = (ret_value | condition.data_point_row_condition(
                row, data_source_cache))
        return ret_value

    def __or__(self, cond: Any):
        return OrCondition(self.conditions + [cond])

    def __repr__(self) -> str:
        cond_str = ' or '.join([str(cond) for cond in self.conditions])
        return f'({cond_str})'


class NegateCondition(Condition):
    """NegateCondition holds a a conditions which will be negated."""

    def __init__(self, to_negate: Condition):
        """Constructs an NegateCondition object.

        Args:
        to_negate: The condition to negate.
        """
        super().__init__()
        self.to_negate = to_negate

    def data_points_condition(
        self,
        table: Any,
        include_annotation_conditions: bool = True,
    ) -> Optional[Any]:
        to_negate = self.to_negate.data_points_condition(
            table, include_annotation_conditions)
        if to_negate is not None:
            return -to_negate
        return None

    def annotations_condition(
        self,
        table: Any,
        include_annotation_conditions: bool = True
    ) -> Optional[Any]:
        to_negate = self.to_negate.annotations_condition(
            table, include_annotation_conditions)
        if to_negate is not None:
            return -to_negate
        return None

    def data_point_row_condition(self, row: schemas.DataPointType,
                                 data_source_cache: DataSourceCache) -> bool:
        return not self.to_negate.data_point_row_condition(
            row, data_source_cache)

    def pubsub_condition(self, pubsub_message: PubsubMessage) -> bool:
        return not self.to_negate.pubsub_condition(pubsub_message)

    def __repr__(self) -> str:
        return f'(not {str(self.to_negate)})'


class DevicesCondition(Condition):
    """Condition for filtering on particular device IDs."""

    def __init__(self, device_ids: Iterable[str]):
        """Constructs a DevicesCondition object.

        Args:
        device_ids: The device IDs for which to filter.
        """
        super().__init__()
        self._device_ids = device_ids

    def pubsub_condition(self, pubsub_message: PubsubMessage) -> bool:
        return pubsub_message.attributes['deviceId'] in self._device_ids

    def data_points_condition(
        self,
        table: Any,
        include_annotation_conditions: bool = True,
    ) -> Optional[Any]:
        return table.DeviceID.isin(self._device_ids)

    def annotations_condition(
        self,
        table: Any,
        include_annotation_conditions: bool = True
    ) -> Optional[Any]:
        return self._device_cond(table)

    def _device_cond(
            self,
            table: Any) -> Optional[Any]:
        """Adds an or condition for each device.

        We do this as opposed to 'IS IN' so the sql query is deterministic,
        which makes testing much easier.

        Args:
        table: Table for which to apply conditions to.

        Returns:
        An ibis condition, where all devices are OR'd together.
        """
        return table.device_id.isin(self._device_ids)

    def data_point_row_condition(self, row: schemas.DataPointType,
                                 data_source_cache: DataSourceCache) -> bool:
        return row.data_point_metadata.device_id in self._device_ids

    def __repr__(self) -> str:
        return f'{type(self).__name__}(device_ids={self._device_ids})'


class SensorsCondition(Condition):
    """Condition for filtering on particular sensor IDs."""

    def __init__(self, sensor_ids: Iterable[str]):
        """Constructs a SensorsCondition object.

        Args:
        sensor_ids: The sensor IDs for which to filter.
        """
        super().__init__()
        self._sensor_ids = sensor_ids

    def data_points_condition(
        self,
        table: Any,
        include_annotation_conditions: bool = True,
    ) -> Optional[Any]:
        # Echo internal tables do not contain the entire data source so we need
        # to filter after querying from BigQuery.
        return None

    def annotations_condition(
        self,
        table: Any,
        include_annotation_conditions: bool = True
    ) -> Optional[Any]:
        # Not relevant to annotations.
        return None

    def pubsub_condition(self, pubsub_message: PubsubMessage) -> bool:
        raise ValueError(
            'annotation conditions not supported for streaming pipelines.')

    def data_point_row_condition(self, row: schemas.DataPointType,
                                 data_source_cache: DataSourceCache) -> bool:
        data_source = data_source_cache[row.data_point_metadata.data_source_id]
        return data_source.sensor.id in self._sensor_ids

    def __repr__(self) -> str:
        return f'{type(self).__name__}(sensors_ids={self._sensor_ids})'


class UsersCondition(Condition):
    """Condition for filtering on particular user IDs.

    Currently these MUST be the sensor store user IDs.  You can find a mapping
    of these IDs in the `user_mappings` BigQuery table in the study specific
    project.
    """

    def __init__(self, user_ids: List[str]):
        """Constructs a UsersCondition object.

    Args:
      user_ids: The user IDs for which to filter.
    """
        super().__init__()
        self._user_ids = user_ids

    def pubsub_condition(self, pubsub_message: PubsubMessage) -> bool:
        return pubsub_message.attributes['participantId'] in self._user_ids

    def data_points_condition(
        self,
        table: Any,
        include_annotation_conditions: bool = True,
    ) -> Optional[Any]:
        try:
            return table.UserID.isin(self._user_ids)
        except AttributeError as e:
            # TODO(b/252813694): Clean up UserID conditions after Echo internal
            # is fully launched.
            raise RuntimeError(
                'User conditions are not supported when reading from Echo '
                'Internal. Please use a Device condition instead.') from e

    def annotations_condition(
        self,
        table: Any,
        include_annotation_conditions: bool = True
    ) -> Optional[Any]:
        return self._user_cond(table)

    def _user_cond(
            self,
            table: Any) -> Optional[Any]:
        """Adds an or condition for each user.

        We do this as opposed to 'IS IN' so the sql query is deterministic,
        which makes testing much easier.

        Args:
        table: Table for which to apply conditions to.

        Returns:
        An ibis condition, where all users are OR'd together.
        """
        return table.user_id.isin(self._user_ids)

    def data_point_row_condition(self, row: schemas.DataPointType,
                                 data_source_cache: DataSourceCache) -> bool:
        return row.data_point_metadata.participant_id in self._user_ids

    def __repr__(self) -> str:
        return f'{type(self).__name__}(user_ids={self._user_ids})'


class AnnotationCondition(Condition):
    """Condition for filtering on the equality of the label of an Annotation."""

    def __init__(self, annotation_label: str):
        """Constructs a AnnotationCondition object.

        Args:
        annotation_label: The name of the annotation for which to filter.
        """
        super().__init__()
        self.annotation_label = annotation_label

    def pubsub_condition(self, pubsub_message: PubsubMessage) -> bool:
        raise ValueError(
            'annotation conditions not supported for streaming pipelines.')

    def data_points_condition(
        self,
        table: Any,
        include_annotation_conditions: bool = True,
    ) -> Optional[Any]:
        return None

    def annotations_condition(
        self,
        table: Any,
        include_annotation_conditions: bool = True
    ) -> Optional[Any]:
        if include_annotation_conditions:
            return table.annotation_label == self.annotation_label
        return None

    def data_point_row_condition(self, row: schemas.DataPointType,
                                 data_source_cache: DataSourceCache) -> bool:
        return (self.annotation_label
                in row.data_point_metadata.annotation_labels)

    def __repr__(self) -> str:
        return (
            f'{type(self).__name__}(annotation_label={self.annotation_label})')


class TimeRangeCondition(Condition):
    """Condition for ensuring a timestamp falls in a provided time range.

    If being used to filter annotations, the condition is:
        Annotation.Start >= time_range.start_time
        AND Annotation.End < time_range.end_time

    If being used to filter data points, the condition is:
        DataPoint.t >= time_range.start_time
        AND DataPoint.t < time_range.end_time
    """

    def __init__(self, time_range: TimeRange):
        """Constructs a TimeRangeCondition object.

        Args:
            time_range: The range for data points or annotations to fall within.
        """
        super().__init__()
        self._time_range = time_range

    @property
    def time_range(self):
        return self._time_range

    def pubsub_condition(self, pubsub_message: PubsubMessage) -> bool:
        start_millis = pubsub_message.attributes['startMillis']

        start_time = pd.Timestamp(int(start_millis), unit='ms', tz='UTC')

        return (self._time_range.start_time < start_time <=
                self._time_range.end_time)

    def data_points_condition(
        self,
        table: Any,
        include_annotation_conditions: bool = True,
    ) -> Optional[Any]:
        import ibis  # lazy import — ibis only needed for ibis-based queries
        timestamp_col = table.DataPointTime

        return (timestamp_col >= ibis.timestamp(  # type: ignore
            self._time_range.start_time.isoformat())) & (
                timestamp_col < ibis.timestamp(  # type: ignore
                    self._time_range.end_time.isoformat()))

    def annotations_condition(
        self,
        table: Any,
        include_annotation_conditions: bool = True
    ) -> Optional[Any]:
        import ibis  # lazy import — ibis only needed for ibis-based queries
        return (table.end_timestamp_utc > ibis.timestamp(  # type: ignore
            self._time_range.start_time.isoformat())) & (
                table.start_timestamp_utc <= ibis.timestamp(  # type: ignore
                    self._time_range.end_time.isoformat()))

    def data_point_row_condition(self, row: schemas.DataPointType,
                                 data_source_cache: DataSourceCache) -> bool:
        beam_start_time = timestamps.datetime_to_beam_timestamp(
            self._time_range.start_time, allow_null=True)
        beam_end_time = timestamps.datetime_to_beam_timestamp(
            self._time_range.end_time, allow_null=True)

        # Handle null start/end times (null means no limit in that direction)
        start_condition = (beam_start_time is None or
                          row.measurement_timestamp_utc >= beam_start_time)
        end_condition = (beam_end_time is None or
                        row.measurement_timestamp_utc < beam_end_time)

        return start_condition and end_condition

    def __repr__(self) -> str:
        return f'{type(self).__name__}(time_range={self._time_range})'


class WriteTimeRangeCondition(Condition):
    """Condition for ensuring a write timestamp falls in a provided time range.
    """

    def __init__(self, time_range: TimeRange):
        """Constructs a WriteTimeRangeCondition object.

        Args:
            time_range: The range for data points or annotations to fall within.
        """
        super().__init__()
        self._time_range = time_range

    @property
    def time_range(self):
        return self._time_range

    def pubsub_condition(self, pubsub_message: PubsubMessage) -> bool:
        raise ValueError(
            'Write time conditions not supported for streaming pipelines.')

    def data_points_condition(
        self,
        table: Any,
        include_annotation_conditions: bool = True,
    ) -> Optional[Any]:
        import ibis  # lazy import — ibis only needed for ibis-based queries
        timestamp_col = table.DataPointWriteTime

        return (timestamp_col >= ibis.timestamp(  # type: ignore
            self._time_range.start_time.isoformat())) & (
                timestamp_col < ibis.timestamp(  # type: ignore
                    self._time_range.end_time.isoformat()))

    def annotations_condition(
        self,
        table: Any,
        include_annotation_conditions: bool = True
    ) -> Optional[Any]:
        raise ValueError('Write time conditions not supported for annotations.')

    def data_point_row_condition(self, row: schemas.DataPointType,
                                 data_source_cache: DataSourceCache) -> bool:
        raise ValueError(
            'Write time conditions not supported for DataPoint rows.')

    def __repr__(self) -> str:
        return f'{type(self).__name__}(write_time_range={self._time_range})'


class AlgorithmCondition(Condition):
    """Filters derived data points to the provided algorithm name and version.

    Note this filter only gets applied to data that has an algorithm version,
    so it will never filter out raw data.
    """

    def __init__(self, algorithm_name: str = '', algorithm_version: str = ''):
        super().__init__()

        self._algorithm_name = algorithm_name
        self._algorithm_version = algorithm_version

    def pubsub_condition(self, pubsub_message: PubsubMessage) -> bool:
        raise ValueError(
            'algorithm conditions not supported for streaming pipelines.')

    def data_points_condition(
        self,
        table: Any,
        include_annotation_conditions: bool = True,
    ) -> Optional[Any]:
        # Since internal Echo tables do not contain the entire data souce we
        # cannot apply this condition when we read directly from BigQuery.
        return None

    def annotations_condition(
        self,
        table: Any,
        include_annotation_conditions: bool = True
    ) -> Optional[Any]:
        # Not relevant to annotations. Use AnnotationAlgorithmCondition instead.
        return None

    def data_point_row_condition(self, row: schemas.DataPointType,
                                 data_source_cache: DataSourceCache) -> bool:
        data_source = data_source_cache[row.data_point_metadata.data_source_id]
        # This should accept a list of algorithm_name and version.
        # Legacy and VO may have different strings
        if self._algorithm_name and self._algorithm_version:
            return (
                (data_source.algorithm.name == self._algorithm_name or
                 data_source.algorithm.name in self._algorithm_name) and
                (data_source.algorithm.version == self._algorithm_version or
                 data_source.algorithm.version in self._algorithm_version)
            )
        if self._algorithm_name:
            return (data_source.algorithm.name == self._algorithm_name or
                    data_source.algorithm.name in self._algorithm_name)
        if self._algorithm_version:
            return (data_source.algorithm.version == self._algorithm_version or
                    data_source.algorithm.version in self._algorithm_version)
        return True

    def __repr__(self) -> str:
        return (f'{type(self).__name__}(algorithm_name={self._algorithm_name}, '
                f'algorithm_version={self._algorithm_version})')


class AnnotationAlgorithmCondition(Condition):
    """Filters derived data points to the provided algorithm name and version.

    Note this filter only gets applied to data that has an algorithm version,
    so it will never filter out raw data.
    """

    def __init__(self,
                 annotation_algo_name: str = '',
                 annotation_algo_version: int = 0):
        super().__init__()

        self._annotation_algo_name = annotation_algo_name
        self._annotation_algo_version = annotation_algo_version

    def pubsub_condition(self, pubsub_message: PubsubMessage) -> bool:
        raise ValueError(
            'annotation algorithm conditions not supported for streaming '
            'pipelines.')

    def data_points_condition(
        self,
        table: Any,
        include_annotation_conditions: bool = True,
    ) -> Optional[Any]:
        # Not Applicable
        return None

    def annotations_condition(
        self,
        table: Any,
        include_annotation_conditions: bool = True
    ) -> Optional[Any]:
        is_null = (table.version_name.isnull() & table.version_number.isnull())
        conds = []
        if self._annotation_algo_name:
            conds.append(table.version_name == self._annotation_algo_name)
        if self._annotation_algo_version:
            conds.append(table.version_number == self._annotation_algo_version)
        cond = functools.reduce(operator.and_, conds)
        return is_null | cond

    def data_point_row_condition(self, row: schemas.DataPointType,
                                 data_source_cache: DataSourceCache) -> bool:
        return True

    def __repr__(self) -> str:
        return (f'{type(self).__name__}('
                f'annotation_algo_name={self._annotation_algo_name}, '
                f'annotation_algo_version={self._annotation_algo_version})')


class DeviceType(enum.Enum):
    """Enums for defining device type conditions."""
    COPPA = 1
    TEST_DEVICE = 2
    PICARD = 3
    PEP = 4
    RIALTO = 5
    MODUS = 6

    def device_id_pattern(self) -> str:
        if self == DeviceType.COPPA:
            return 'C2Q.*'
        if self == DeviceType.TEST_DEVICE:
            # Matches any device with 'test' in the name. (?i) == case
            # insensitive.
            return '(?i).*test.*'
        # TODO(b/203895514): Replace study-specific with CUSTOM device type
        # option.
        if self == DeviceType.PICARD:
            return 'PDV.*|com.verily.picard.*'
        if self == DeviceType.PEP:
            return 'pep.*'
        if self == DeviceType.RIALTO:
            return 'RLQ.*|QCI.*'
        if self == DeviceType.MODUS:
            return 'stepwatch.*'
        raise ValueError(f'No device ID pattern for DeviceType: {self}')


class DeviceTypeCondition(Condition):
    """Condition for filtering results to a specific device type."""

    def __init__(self, device_type: DeviceType):
        super().__init__()
        self._device_id_pattern = device_type.device_id_pattern()

    def data_points_condition(
        self,
        table: Any,
        include_annotation_conditions: bool = True,
    ) -> Optional[Any]:
        return table.DeviceID.re_search(self._device_id_pattern)

    def annotations_condition(
        self,
        table: Any,
        include_annotation_conditions: bool = True
    ) -> Optional[Any]:
        return self._device_type_cond(table)

    def _device_type_cond(
            self,
            table: Any) -> Optional[Any]:
        """Adds an or condition for each device.

        We do this as opposed to 'IS IN' so the sql query is deterministic,
        which makes testing much easier.

        Args:
        table: Table for which to apply conditions to.

        Returns:
        An ibis condition, where all devices are OR'd together.
        """
        return table.device_id.re_search(self._device_id_pattern)

    def data_point_row_condition(self, row: schemas.DataPointType,
                                 data_source_cache: DataSourceCache) -> bool:
        return re.match(self._device_id_pattern,
                        row.data_point_metadata.device_id) is not None

    def pubsub_condition(self, pubsub_message: PubsubMessage) -> bool:
        return re.match(self._device_id_pattern,
                        pubsub_message.attributes['deviceId']) is not None

    def __repr__(self) -> str:
        return (f'{type(self).__name__}(device_id_pattern='
                f'{self._device_id_pattern})')


class UserNamespaceCondition(Condition):
    """Condition for filtering data by user namespace."""

    def __init__(self,
                 user_namespace: enums_pb2.UserIdKeyspace,
                 negate: bool = False):
        """Creates a UserIdTypeCondition.

        Args:
        user_namespace: The enums_pb2.UserIdKeyspace to include in the
            condition.
        negate: Whether the condition should be negated. If True the condition
            will be: bq.user_namespace = self.user_namespace
            If False the condition will be:
                bq.user_namespace != self.User_namespace
        """
        super().__init__()

        self._user_namespace_enum_value = user_namespace
        self._user_namespace = enums_pb2.UserIdKeyspace.Name(
            user_namespace)  # type: ignore
        self._negate = negate

    def data_points_condition(
        self,
        table: Any,
        include_annotation_conditions: bool = True,
    ) -> Optional[Any]:
        col = table.UserNamespace
        if self._negate:
            return col != self._user_namespace
        else:
            return col == self._user_namespace

    def annotations_condition(
        self,
        table: Any,
        include_annotation_conditions: bool = True
    ) -> Optional[Any]:
        if self._negate:
            return table.user_namespace != self._user_namespace
        else:
            return table.user_namespace == self._user_namespace

    def data_point_row_condition(self, row: schemas.DataPointType,
                                 data_source_cache: DataSourceCache) -> bool:
        if self._negate:
            return (row.data_point_metadata.participant_namespace !=
                    self._user_namespace_enum_value)
        else:
            return (row.data_point_metadata.participant_namespace ==
                    self._user_namespace_enum_value)

    def pubsub_condition(self, pubsub_message: PubsubMessage) -> bool:
        raise ValueError(
            'user namespace conditions not supported for streaming pipelines.')

    def __repr__(self) -> str:
        return (f'{type(self).__name__}'
                f'(user_namespace={self._user_namespace}, '
                f'negate={self._negate})')


class FalseCondition(DevicesCondition):

    def __init__(self):
        super().__init__([])

    def __repr__(self) -> str:
        return 'FALSE'


class TrueCondition(NegateCondition):

    def __init__(self):
        super().__init__(FalseCondition())

    def __repr__(self) -> str:
        return 'TRUE'
