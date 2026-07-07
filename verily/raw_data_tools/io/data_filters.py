"""DataPoint filtering module

This module contains the DataFilter class which is used to filter out DataPoint
objects from BigQuery queries created via the sensors_io.SensorsIO methods:
    - `echo_data_point_rows`
    - (not yet supported) `custom_data_point_rows`

These filters are created from a table that maps a data label, such as data
quality annotations, to one or more DataPoint rows. These filter tabels
generally contain information about the device ID, time range and other
metadata that can be used to specify matching DataPoint rows. The filter can
either be exclusive (is_inclusive = False, filter out matches) or inclusive
(is_inclusive = True, only pass through matches).
"""

from collections import defaultdict
import itertools
import logging
from typing import (DefaultDict, Dict, Iterable, List, NamedTuple, Optional,
                    Tuple)

import apache_beam as beam
from google.cloud import bigquery  # type: ignore
import pandas as pd
from pandas.api.types import is_datetime64_any_dtype
from pandas.api.types import is_int64_dtype
from pandas.api.types import is_object_dtype

from verily.raw_data_tools.conditions import TimeRange
from verily.raw_data_tools import conditions
from verily.raw_data_tools.schemas import schemas
from verily.raw_data_tools.utils.data_source_cache import DataSourceCache

DEVICE_ID_FIELD: str = 'DeviceID'
DATA_POINT_TIME_FIELD: str = 'DataPointTime'
END_TIME_MILLIS_FIELD: str = 'DataPoint.end_time_millis'

QueryStr = str

ACCEPTABLE_FIELD_TYPES = ['INT64', 'INTEGER', 'STRING', 'TIMESTAMP']

SUPPORTED_DATA_POINT_FIELDS = [
    'supplemental_source_data_spec',
    'supplemental_source_sensor_id',
    'supplemental_source_algorithm_name',
    'supplemental_source_algorithm_version',
]


def _null_to_none(val):
    if pd.isnull(val):
        return None
    return val


def _convert_timestamp_column(time_column: pd.Series) -> pd.Series:
    """Converts a column of timestamp-like data to numpy datetime64[ns]

    In order to support custom start and end columns, this function provides
    the logic to convert three supported types (string, Timestamp, and int).

    Args:
        time_column (pd.Series): Pandas Series of timestamp-like values that
            will be converted

    Returns:
        pd.Series of converted timestamps of dtype datetime64[ns]

    Raises:
        ValueError if the column dtype is not one of STRING, INT64, or TIMESTAMP
    """

    if is_int64_dtype(time_column):

        min_timestamp = pd.Timestamp(time_column.min(), unit='ms', tz='UTC')
        if min_timestamp < pd.Timestamp('2010-01-01T00:00:00+0000'):
            raise ValueError('Got an out of range timestamp value: '
                             f'{time_column.min()} = {min_timestamp}. '
                             'Integer timestamps are expected to be in '
                             'milliseconds since epoch.')

        max_timestamp = pd.Timestamp(time_column.max(), unit='ms', tz='UTC')
        if max_timestamp >= pd.Timestamp.utcnow():
            raise ValueError('Got an out of range timestamp value: '
                             f'{time_column.max()} = {max_timestamp}. '
                             'Integer timestamps are expected to be in '
                             'milliseconds since epoch.')

        return pd.to_datetime(time_column, unit='ms').dt.tz_localize(None).astype('datetime64[ns]')  # pylint: disable=line-too-long
    elif is_object_dtype(time_column):
        return pd.to_datetime(time_column).dt.tz_localize(None).astype('datetime64[ns]')  # pylint: disable=line-too-long
    elif is_datetime64_any_dtype(time_column):
        return time_column.dt.tz_localize(None).astype('datetime64[ns]')  # pylint: disable=line-too-long
    else:
        raise ValueError('`start_time` and `end_time` fields must be one of '
                         'these types: INT64, STRING, TIMESTAMP; got '
                         f'{time_column.name} type is {time_column.dtype}')


class _TimeRangeTuple(NamedTuple):
    """Tuple representation of a time range"""
    start_time_ns: int
    end_time_ns: int

    def to_time_range_condition(self):
        return conditions.TimeRangeCondition(
            TimeRange(start_time=pd.Timestamp(self.start_time_ns, unit='ns'),
                      end_time=pd.Timestamp(self.end_time_ns, unit='ns')))


class _TimeRanges(tuple):
    """Tuple representation of a list of _TimeRangeTuples"""

    @classmethod
    def from_list(cls, time_range_list: List[List[int]]) -> '_TimeRanges':
        """
        Builds a _TimeRanges object from a list of time ranges

        Args:
            time_range_list (List[List]): A List of Lists, where each sub-list
                has exactly two elements: a start time and an end time

        Returns:
            _TimeRanges: A tuple of _TimeRangeTuple objects
        """
        return cls(
            _TimeRangeTuple(*time_range) for time_range in time_range_list)

    def to_or_condition(self) -> conditions.OrCondition:
        return conditions.OrCondition(
            [time_range.to_time_range_condition() for time_range in self])


class _CombinedTimeRanges(list):
    """List of _TimeRanges"""

    def to_or_condition(self) -> conditions.OrCondition:
        """Returns an OrCondition of OrConditions from _TimeRanges
        Essentially UNIONs multiple filters"""
        return conditions.OrCondition(
            [time_ranges.to_or_condition() for time_ranges in self])

    def to_and_condition(self) -> conditions.AndCondition:
        """Returns an ANDCondition of OrConditions from _TimeRanges
        Essentially INTERSECTs multiple filters"""
        return conditions.AndCondition(
            [time_ranges.to_or_condition() for time_ranges in self])


class _DataSourceFilterKey(NamedTuple):
    """Key for mapping DataFilter table rows to specific DataSources"""
    device_id: str
    is_inclusive: bool
    data_spec: Optional[str] = None
    sensor_id: Optional[str] = None
    algorithm_name: Optional[str] = None
    algorithm_version: Optional[str] = None


class DataFilter:
    """A mapping of data sources to a list of filter time ranges

    Args:
        table_id (str): The spec of the annotation type to create the filter
            from. It is assumed that the filter table lives in the same GCP
            project as the DataPoint tables in their respective
            internal_echo_dataset.
        is_inclusive (bool): [default False] Indicates whether the filter
            should be exclusive (False, filter out matching DataPoint rows) or
            inclusive (True, only pass through matches).
        device_id_field (str): [default 'DeviceID'] Name of the BigQuery field
            where the device IDs are defined.
        start_time_field (str): [default 'DataPointTime'] Name of the BigQuery
            field where the filter start times are defined. Must be a subfield
            of the DataPoint struct field if not 'DataPointTime'.
        end_time_field (str): [default 'end_time_millis'] Name of the BigQuery
            field where the filter end times are defined. Must be a subfield
            of the DataPoint struct field.
        annotation_label (Optional[str]): When this parameter is not None, the
            table is assumed to have an Annotation schema. Only the rows that
            match the annotation label are used. If None, the table is assumed
            to have a DataPoint schema.
        algorithm_version (Optional[str]): When not None, the filter table query
            will be restricted to rows matching the provided algorithm version.
        algorithm_version_field (Optional[str]): It should be provided when
            algorithm_version is not None. It specifies the BigQuery field name
            containing the algorithm version information.
    """

    def __init__(self,
                 table_id: str,
                 *,
                 is_inclusive: bool = False,
                 device_id_field: str = DEVICE_ID_FIELD,
                 start_time_field: str = DATA_POINT_TIME_FIELD,
                 end_time_field: str = END_TIME_MILLIS_FIELD,
                 annotation_labels: Optional[Tuple[str, ...]] = None,
                 algorithm_version: Optional[str] = None,
                 algorithm_version_field: Optional[str] = None):
        self._table_id = table_id
        self._is_inclusive = is_inclusive
        self._device_id_field = device_id_field
        self._data_point_columns = ''

        if start_time_field == end_time_field:
            raise ValueError('Start and end time fields cannot be '
                             f'the same ({start_time_field}).')
        self._start_time_field = start_time_field
        self._end_time_field = end_time_field

        if annotation_labels is None:
            self._where_annotation_label = ''
        else:
            joined_labels = "','".join(annotation_labels)
            self._where_annotation_label = (' AND annotation_label IN (\''
                                            f'{joined_labels}\')')

        self._algorithm_version = algorithm_version
        self._algorithm_version_field = algorithm_version_field
        self._where_algorithm_version = ''

        if ((algorithm_version is not None) !=
        (algorithm_version_field is not None)):
            raise ValueError(
                'Both `algorithm_version` and `algorithm_version_field` must '
                'be set together (or both left as None).'
            )

        if (algorithm_version is not None and
        algorithm_version_field is not None):
            self._where_algorithm_version = (
                f' AND {algorithm_version_field} = \'{algorithm_version}\'')

        self._validate_table_schema()

        self._time_range_map: Dict[_DataSourceFilterKey, _TimeRanges] = {}

    @property
    def is_inclusive(self):
        return self._is_inclusive

    def _validate_table_schema(self):
        """
        Checks if the DataFilter's fields match the BigQuery table schema.

        If a field is missing or has an incorrect type, an error is logged.

        Args:
            table_id (str): The fully qualified ID of the BigQuery table the
                DataFilter refers to.

        Raises:
            ValueError if given field names are not included in the actual
            BigQuery table schema or if the types are not supported
        """

        bq_client = bigquery.Client()
        bq_table = bq_client.get_table(self._table_id)

        given_schema: Dict[str, str] = {}
        data_point_columns: List[str] = []
        for field in bq_table.schema:
            if field.name == 'DataPoint':
                for subfield in field.fields:
                    data_point_subfield = f'DataPoint.{subfield.name}'
                    given_schema[data_point_subfield] = subfield.field_type
                    if (subfield.name in SUPPORTED_DATA_POINT_FIELDS and
                            subfield.name not in (self._device_id_field,
                                                  self._start_time_field,
                                                  self._end_time_field)):
                        data_point_columns.append(data_point_subfield)
            else:
                given_schema[field.name] = field.field_type

        if len(data_point_columns) > 0:
            self._data_point_columns = ', ' + ', '.join(data_point_columns)

        is_match: bool = True
        error_str: str = ''

        if given_schema.get(self._device_id_field, '') != 'STRING':
            error_str += (
                f'\nDevice ID field `{self._device_id_field}` of type '
                'STRING not found in the given schema.')
            is_match = False

        if self._start_time_field not in given_schema:
            error_str += (
                f'\nStart time field `{self._start_time_field}` not found in '
                'the given schema.')
            is_match = False
        elif given_schema.get(self._start_time_field,
                              '') not in ACCEPTABLE_FIELD_TYPES:
            error_str += (
                f'\nStart time field `{self._start_time_field}` type is not '
                'one of INTEGER (INT64), STRING, or TIMESTAMP.')
            is_match = False

        if self._end_time_field not in given_schema:
            error_str += (
                f'\nEnd time field `{self._end_time_field}` not found in the '
                'given schema.')
            is_match = False
        elif given_schema.get(self._end_time_field,
                              '') not in ACCEPTABLE_FIELD_TYPES:
            error_str += (
                f'\nEnd time field `{self._end_time_field}` type is not one of '
                'INTEGER (INT64), STRING, or TIMESTAMP.')
            is_match = False

        if not is_match:
            raise ValueError(f'The schema for {self._table_id} is does not '
                             'have a valid data filter schema or is missing '
                             f'the specified fields:{error_str}')

    def _get_table_query_str(self) -> QueryStr:
        """
        Builds a string for querying the DataFilter's BigQuery table

        Returns:
            QueryStr: A str object defining the query for getting the table
                rows for the DataFilter
        """
        return (f'SELECT {self._device_id_field} AS device_id, '
                f'{self._start_time_field} AS start_time, '
                f'{self._end_time_field} AS end_time{self._data_point_columns}'
                f' FROM {self._table_id} WHERE TRUE'
                f'{self._where_annotation_label}'
                f'{self._where_algorithm_version}')

    def get_filter_table(self) -> Dict[_DataSourceFilterKey, _TimeRanges]:
        """Returns a mapping of _DataSourceFilterKeys to _TimeRanges

        Builds and executes a SQL query to get the BigQuery table rows for the
        data filter. The mapping of data sources to time ranges is cached for
        re-use if using the same DataFilter for multiple dataspecs. If there are
        any changes to the DataFilter BQ Table between calls, the stale cached
        table will still be used.

        """

        if len(self._time_range_map) == 0:
            gcp_project = self._table_id.split('.')[0]

            filter_query = self._get_table_query_str()

            filter_table = pd.read_gbq(query=filter_query,
                                       project_id=gcp_project)

            filter_table['start_time'] = _convert_timestamp_column(
                filter_table['start_time'])
            filter_table['end_time'] = _convert_timestamp_column(
                filter_table['end_time'])

            print(filter_table.to_string())

            supp_source_rename = {
                col: col.replace('DataPoint.supplemental_source_', '')
                for col in filter_table.columns
                if col.startswith('DataPoint.supplemental_source_')
            }
            filter_table = filter_table.rename(columns=supp_source_rename)
            group_columns = ['device_id', *supp_source_rename.values()]

            for _, grouped_df in filter_table.groupby(group_columns,
                                                      dropna=False):
                filter_key_fields = {
                    col: _null_to_none(grouped_df.iloc[0][col])
                    for col in group_columns
                }
                filter_key = _DataSourceFilterKey(
                    **filter_key_fields, is_inclusive=self._is_inclusive)

                filter_time_ranges = _TimeRanges.from_list(
                    grouped_df[['start_time',
                                'end_time']].to_records(index=False).tolist())

                self._time_range_map[filter_key] = filter_time_ranges

            logging.info('Got %s filter table', self._table_id)

        return self._time_range_map


class MergedDataFilters(beam.DoFn):
    """
    A DataPoint filter built from a combination of DataFilters

    Args:
        data_filter_list (List[DataFilter]): A list of DataFilter objects to
            apply to the PCollection of DataPoints
    """

    def __init__(self, data_filter_list: List[DataFilter]):
        super().__init__()
        self._combined_data_filters: DefaultDict[
            _DataSourceFilterKey,
            _CombinedTimeRanges] = defaultdict(_CombinedTimeRanges)
        self._num_inclusive: int = 0

        for data_filter in data_filter_list:
            if data_filter.is_inclusive:
                self._num_inclusive += 1
            for source_key, time_range_list in data_filter.get_filter_table(
            ).items():
                self._combined_data_filters[source_key].append(time_range_list)

        self._filter_counter = beam.metrics.Metrics.counter(
            'data_filter_counter', 'rows_removed')

    def _get_filter_for_data_source(
            self, device_id: str, data_spec_name: str, sensor_id: str,
            algorithm_name: str,
            algorithm_version: str) -> conditions.AndCondition:
        """Gets all filter time ranges that match a device_id

        All time ranges are combined and converted to conditions.Condition
        objects depending on the mode of the filters (inclusive vs exclusive)

        Args:
            device_id (str): The DeviceID of the DataPoints being filtered
            data_spec_name (str): The dataspec name of the DataPoints being
                filtered
            sensor_id (str): The sensor_id of the DataPoints being filtered
            algorithm_name (str): The algorithm name of the DataPoints being
                filtered
            algorithm_version (str): The algorithm version of the DataPoints
                being filtered

        Returns:
            Tuple[bool,
                  Optional[conditions.AndCondition],
                  Optional[conditions.OrCondition]]: A boolean indicating
                  whether there is at least one inclusive filter in the cache,
                  an AndCondition of all the inclusive filters (or None), and
                  an OrCondition of all the exclusive filters (or None).
        """

        inclusive_bools = [False, True]
        data_spec_list = set([None, data_spec_name])
        sensor_id_list = set([None, sensor_id])
        algo_name_list = set([None, algorithm_name])
        algo_ver_list = set([None, algorithm_version])

        inclusive_time_ranges = _CombinedTimeRanges()
        exclusive_time_ranges = _CombinedTimeRanges()

        for (data_spec_name_, sensor_id_, algo_name_, algo_ver_,
             is_inclusive) in itertools.product(data_spec_list, sensor_id_list,
                                                algo_name_list, algo_ver_list,
                                                inclusive_bools):

            filter_key = _DataSourceFilterKey(device_id=device_id,
                                              data_spec=data_spec_name_,
                                              sensor_id=sensor_id_,
                                              algorithm_name=algo_name_,
                                              algorithm_version=algo_ver_,
                                              is_inclusive=is_inclusive)

            time_range_lists: _CombinedTimeRanges = (
                self._combined_data_filters.get(filter_key,
                                                _CombinedTimeRanges()))

            if is_inclusive:
                inclusive_time_ranges.extend(time_range_lists)
            else:
                exclusive_time_ranges.extend(time_range_lists)

        inclusive_filter: conditions.Condition = conditions.FalseCondition()
        if self._num_inclusive == 0:
            inclusive_filter = conditions.TrueCondition()

        elif len(inclusive_time_ranges) == self._num_inclusive:
            inclusive_filter = inclusive_time_ranges.to_and_condition()

        exclusive_filter: conditions.Condition = conditions.FalseCondition()
        if len(exclusive_time_ranges) == 0:
            exclusive_filter = conditions.TrueCondition()
        else:
            exclusive_filter = conditions.NegateCondition(
                exclusive_time_ranges.to_or_condition())

        return conditions.AndCondition((inclusive_filter, exclusive_filter))

    def process(
            self, element: schemas.DataPointType,
            data_source_cache: DataSourceCache
    ) -> Iterable[schemas.DataPointType]:
        """Filters DataPoint rows grouped by device_id

        Args:
            element (schemas.DataPoint): DataPoint element to run the filter on.
                This is inherently passed when used inside of a beam.ParDo
                PTransform.
            data_source_cache (DataSourceCache): Mapping of DataSourceID
                integers to DataSource objects.

        Returns:
            Iterable of filtered DataPoint objects
        """

        device_id = element.data_point_metadata.device_id
        data_source = data_source_cache.get_data_source(
            element.data_point_metadata.data_source_id)

        device_filter_condition = (self._get_filter_for_data_source(
            device_id=device_id,
            data_spec_name=data_source.data_spec.name,
            sensor_id=data_source.sensor.id,
            algorithm_name=data_source.algorithm.name,
            algorithm_version=data_source.algorithm.version))

        if device_filter_condition.data_point_row_condition(
                element, data_source_cache):
            yield element
        else:
            self._filter_counter.inc()
