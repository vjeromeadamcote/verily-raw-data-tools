"""Mock source that generates data for a given beam schema."""

import random
from typing import Any, Dict, Optional, Tuple, Union

import apache_beam as beam
from apache_beam.utils.timestamp import Timestamp
import pandas as pd

from verily.ds_sdk.core import conditions
from verily.ds_sdk.core import schemas
from verily.ds_sdk.core.io.bigquery import build_data_source_cache
from verily.ds_sdk.core.schemas import AnnotationMetadata
from verily.ds_sdk.core.schemas import DataPointMetadata
from verily.ds_sdk.protos import types_pb2

DEFAULT_BOUNDS = {
    'data_source_id': (123, 123),
    'sensor_id': (1, 1),
    'sampling_rate': (30, 30)
}

_TYPE_TO_DEFAULT_MAP = {
    str:
        lambda bounds: random.choice(bounds) if bounds else 'string',
    int:
        lambda bounds: random.randint(*bounds)
        if bounds else random.randint(0, 10),
    float:
        lambda bounds: float(
            random.randint(*bounds) if bounds else random.randint(0, 10)),
    bytes:
        lambda bounds: types_pb2.DataSource(name=random.choice(bounds) if bounds
                                            else 'name').SerializeToString(),
    Timestamp:
        lambda bounds: Timestamp(
            pd.Timestamp('2021-06-01 12:00:00 UTC').timestamp()),
    DataPointMetadata:
        lambda bounds: schemas.data_point_metadata_for_raw_data(
            123, 'C2Q123', '321', 1, None, None, set()),
    AnnotationMetadata:
        lambda bounds: AnnotationMetadata('C2Q123', '321', 1, 'version', 1, [])
}


def _get_default(python_type, bounds=None, list_size=5):
    # TODO(tanke): randomly drop values for Optional.
    is_optional = False  # pylint: disable=unused-variable
    if '__origin__' in python_type.__dict__:
        # TODO(tanke): support Union for more than 2 types.
        if python_type.__origin__ == Union:  # pylint: disable=comparison-with-callable
            a = list(python_type.__args__)
            if type(None) in a:
                is_optional = True
                a.remove(type(None))
            if len(a) != 1:
                raise ValueError(
                    'Non-Optional Unions are not supported at this time.')
            python_type = a[0]
            return _get_default(python_type, bounds=bounds, list_size=list_size)

        if python_type.__origin__ == list:
            fn = _TYPE_TO_DEFAULT_MAP[python_type.__args__[0]]
            return [fn(bounds) for _ in range(list_size)]

    fn = _TYPE_TO_DEFAULT_MAP[python_type]
    return fn(bounds)


# TODO(tanke): replace this implementation with the one described in
# go/sensors-sandwich.
class MockRowSource(beam.PTransform):
    """Mock source that generates data for a given beam schema."""

    def __init__(  # pylint: disable=dangerous-default-value
            self,
            *,
            row_schema: type,
            condition: Optional[conditions.Condition],
            for_annotation: bool,
            num_examples: int = 100,
            bounds: Dict[str, Tuple[Any, Any]] = DEFAULT_BOUNDS):
        super().__init__()

        self._row_schema = row_schema
        self._condition = condition
        self._num_examples = num_examples
        self._bounds = bounds

        # TODO(tanke): Parse this information out of the Condition object.
        self._start_time = pd.Timestamp('2021-06-01 12:00:00 UTC')
        self._end_time = pd.Timestamp('2021-06-01 12:10:00 UTC')
        self._cache_data_source = False
        self._data_spec_name = 'com.verily.pressure'
        self._for_annotation = for_annotation

    def expand(self, pcol):
        mock_rows_keyed_by_data_source = (pcol | beam.Create(
            self._generate_mock_data()).with_output_types(
                Tuple[bytes, self._row_schema]))

        data_source_cache = None
        if self._cache_data_source:
            data_source_cache = (
                mock_rows_keyed_by_data_source |
                f'Build DataSource Cache - {self._data_spec_name}' >>
                build_data_source_cache.BuildDataSourceCache())

        mock_rows = (mock_rows_keyed_by_data_source |
                     f'Drop DataSource - {self._data_spec_name}' >>
                     beam.Values().with_output_types(self._row_schema))  # pylint: disable=no-value-for-parameter

        if self._for_annotation:
            return mock_rows

        return mock_rows, data_source_cache

    def _generate_mock_data(self):
        rows = []
        for timestamp in pd.date_range(self._start_time, self._end_time,
                                       self._num_examples):
            args = {
                field_name:
                _get_default(field_type,
                             bounds=self._bounds.get(field_name, None)) for
                field_name, field_type in self._row_schema._field_types.items()  # pylint: disable=protected-access
            }
            if self._for_annotation:
                args['start_timestamp_utc'] = Timestamp(timestamp.timestamp())
                args['end_timestamp_utc'] = Timestamp(
                    (timestamp + pd.Timedelta('1s')).timestamp())
                rows.append((b'', self._row_schema(**args)))
            else:
                data_source_bytes = types_pb2.DataSource(
                    name='123').SerializeToString()
                args['measurement_timestamp_utc'] = Timestamp(
                    timestamp.timestamp())
                rows.append((data_source_bytes, self._row_schema(**args)))

        return rows
