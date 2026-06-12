"""Mock class for testing the SensorsIO object."""

from typing import Any, Dict, Iterable, List, Optional, Type, TypeVar

import apache_beam as beam
from apache_beam.testing.test_pipeline import TestPipeline
from apache_beam.testing.util import assert_that
from apache_beam.testing.util import equal_to
from google.cloud import bigquery  # type: ignore
import pandas as pd

from verily.ds_sdk.core import conditions
from verily.ds_sdk.core import data_filters
from verily.ds_sdk.core import options
from verily.ds_sdk.core import schemas
from verily.ds_sdk.core import sensors_io
from verily.ds_sdk.core import sensorsuite
from verily.ds_sdk.core.io.data_source_cache import DataSourceCache
from verily.ds_sdk.protos import types_pb2


class _AssertEqual(beam.PTransform):
    def __init__(self, expected: Any, equal_fn) -> None:
        super().__init__()
        self._expected = expected
        self._equal_fn = equal_fn

    def expand(self, pcol):
        return assert_that(pcol, self._equal_fn(self._expected))


K = TypeVar('K')
V = TypeVar('V')


def _none_to_empty_dict(input_dict: Optional[Dict[K, V]]) -> Dict[K, V]:
    return input_dict if input_dict is not None else {}


class MockSensorsIO(sensors_io.SensorsIO):
    """Mock class for testing the SensorsIO object."""

    def __init__(
        self,
        *,
        input_source_data_points: Optional[
            Dict[str, Iterable[schemas.DataPointType]]
        ] = None,
        expected_bigquery_data_points: Optional[
            Dict[str, Iterable[schemas.DataPointType]]
        ] = None,
        expected_sensor_store_data_points: Optional[
            Dict[str, Iterable[schemas.DataPointType]]
        ] = None,
        input_source_annotations: Optional[
            Dict[str, Iterable[schemas.Annotation]]
        ] = None,
        expected_sink_annotations: Optional[
            Dict[str, Iterable[schemas.DataPointType]]
        ] = None,
        data_source_cache: Optional[
            Dict[Optional[int], types_pb2.DataSource]
        ] = None,
        equal_fn=None,
    ):
        """Creates a MockSensorsIO object.

        Args:
          input_source_data_points: Mapping from data spec to data points. This
            is the data points that will be returned by a call to
            echo_data_point_rows(data_spec_name, ...) or
            custom_data_point_rows(data_point_table_id, ...)
          expected_bigquery_data_points: Mapping from BigQuery table to data
            points that are expected to be written for that table. These data
            points will be validated when write_data_points_to_big_query is
            called.
          expected_sensor_store_data_points: Mapping from data spec to data
            points that are expected to be written for that data spec. These
            data points will be validated when write_to_sensor_store is called.
          input_source_annotations: Mapping from BigQuery table ID to
            annnotations to return. These annotations will be returned when
            annotation_rows is called.
          expected_sink_annotations: Mapping from BigQuery table ID to expected
            annotations. These annotations will be validated when
          equal_fn: The equality function to use when comparing elements.
        """
        super().__init__(registry='DevTeam', runner='DirectRunner', env='prod')
        self._input_data_points = _none_to_empty_dict(input_source_data_points)
        self._sink_bq_data_points = _none_to_empty_dict(
            expected_bigquery_data_points
        )
        self._sink_sensor_store_data_points = _none_to_empty_dict(
            expected_sensor_store_data_points
        )
        self._input_annotations = _none_to_empty_dict(input_source_annotations)
        self._sink_annotations = _none_to_empty_dict(expected_sink_annotations)
        self._equal_fn = equal_fn
        if self._equal_fn is None:
            self._equal_fn = equal_to
        self._p = TestPipeline()

        device_ids = set()
        for data_points_list in self._input_data_points.values():
            device_ids.update(
                [dp.data_point_metadata.device_id for dp in data_points_list]
            )
        data_source_cache_obj = DataSourceCache(
            _none_to_empty_dict(data_source_cache)
        )
        self._data_source_cache_handler._pvalue = data_source_cache_obj
        self._data_source_cache_handler._pvalue_needs_refresh = False

    def echo_data_point_rows(
        self,
        *,
        data_spec_name: str,
        source_options: options.BatchSourceOptions,
        condition: Optional[conditions.Condition],
        annotation_inner_join_options: Optional[
            options.AnnotationInnerJoinOptions
        ],
        incremental_query_options: Optional[options.IncrementalQueryOptions],
        data_filter_list: Optional[List[data_filters.DataFilter]] = None,
        use_dedupe_in_query: bool = False,
    ):
        del (
            source_options,
            condition,
            annotation_inner_join_options,
            incremental_query_options,
            data_filter_list,
        )  # pylint: disable=line-too-long

        try:
            data_points = self._input_data_points[data_spec_name]
        except KeyError as e:
            raise RuntimeError(
                f'No input data points provided for {data_spec_name}'
            ) from e
        return self._p | data_spec_name >> beam.Create(data_points)

    def custom_data_point_rows(
        self,
        *,
        data_point_table_id: str,
        row_schema: Type[schemas.DataPointType],
        source_options: options.BatchSourceOptions,
        condition: Optional[conditions.Condition],
        annotation_inner_join_options: Optional[
            options.AnnotationInnerJoinOptions
        ],
    ):
        del source_options, condition, annotation_inner_join_options, row_schema

        try:
            data_points = self._input_data_points[data_point_table_id]
        except KeyError as e:
            raise RuntimeError(
                f'No input data points provided for {data_point_table_id}'
            ) from e  # pylint: disable=line-too-long
        return self._p | data_point_table_id >> beam.Create(data_points)

    def annotation_rows(
        self,
        *,
        bigquery_table: str,
        source_options: options.BatchSourceOptions,
        condition: Optional[conditions.Condition],
    ):
        del source_options, condition
        try:
            annotations = self._input_annotations[bigquery_table]
        except KeyError as e:
            raise RuntimeError(
                f'No input annotations provided for {bigquery_table}'
            ) from e
        return self._p | f'{bigquery_table}' >> beam.Create(annotations)

    def write_data_points_to_big_query(
        self,
        table_id: str,
        schema: Type[schemas.DataPointType],
        write_disposition: str = bigquery.WriteDisposition.WRITE_TRUNCATE,
    ) -> beam.PTransform:
        del schema, write_disposition
        try:
            expected = self._sink_bq_data_points[table_id]
        except KeyError as e:
            raise RuntimeError(
                f'No BigQuery sink data points provided for {table_id}'
            ) from e
        return _AssertEqual(expected, self._equal_fn)

    def write_to_sensor_store(
        self,
        schema: Type[schemas.DataPointType],
        algorithm_name: str,
        algorithm_version: str,
        overwrite_key_generator: sensorsuite.OverwriteKeyGeneratorType,
        api_key: str,
        global_qps_limit: int = 1,
        request_retry_timeout: pd.Timedelta = ...,
    ) -> beam.PTransform:
        del (
            algorithm_name,
            algorithm_version,
            overwrite_key_generator,
            api_key,
            request_retry_timeout,
            global_qps_limit,
        )  # pylint: disable=line-too-long
        data_spec = schema.data_spec_from_decorator  # type: ignore
        try:
            expected = self._sink_sensor_store_data_points[data_spec]
        except KeyError as e:
            raise RuntimeError(
                f'No sensor store sink data points provided for {data_spec}'
            ) from e
        return _AssertEqual(expected, self._equal_fn)

    def write_annotations_to_bigquery(self, table_id: str) -> beam.PTransform:
        try:
            expected = self._sink_annotations[table_id]
        except KeyError as e:
            raise RuntimeError(
                f'No BigQuery sink annotations provided for {table_id}'
            ) from e
        return _AssertEqual(expected, self._equal_fn)
