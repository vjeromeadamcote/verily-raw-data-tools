"""Transform to build a DataSource Cache as a PCollection."""

from typing import Any, Dict, Iterable, Optional, Tuple

import apache_beam as beam

from verily.ds_sdk.core import schemas
from verily.ds_sdk.core.gcp import bigquery_source_wrapper
from verily.ds_sdk.core.gcp import credentials
from verily.ds_sdk.core.io.bigquery import build_row_filters
from verily.ds_sdk.core.io.bigquery.utils import echo_utils
from verily.ds_sdk.core.io.data_source_cache import DataSourceCache
from verily.ds_sdk.protos import types_pb2


def _parse_data_source_bytes(b: bytes) -> types_pb2.DataSource:
    data_source = types_pb2.DataSource()
    data_source.ParseFromString(b)
    return data_source


class _MergeDataSourceCaches(beam.DoFn):
    """Merges multiple DataSourceCaches into a single cache."""

    def process(  # type: ignore[override]
        self,
        elem: Tuple[int,
                    Iterable[DataSourceCache]]) -> Iterable[DataSourceCache]:
        accumulator: Dict[Optional[int], types_pb2.DataSource] = {}
        for data_source_cache in elem[1]:
            accumulator.update(data_source_cache.data_source_mappings)
        return [DataSourceCache(accumulator)]


class MergeDataSourceCaches(beam.PTransform):
    """Transform for merging multiple caches into a single cache."""

    def expand(
        self, pcol: beam.PCollection[DataSourceCache]
    ) -> beam.PCollection[DataSourceCache]:
        return (pcol | beam.Map(lambda x: (1, x)) | beam.GroupByKey() |
                beam.ParDo(_MergeDataSourceCaches()))


class _BuildDataSourceCache(beam.DoFn):
    """Builds a DataSourceCache from tuple mappings."""

    def process(  # type: ignore[override]
        self, elem: Tuple[int,
                          Iterable[Tuple[Optional[int],
                                         bytes]]]) -> Iterable[DataSourceCache]:
        accumulator: Dict[Optional[int], types_pb2.DataSource] = {}
        for data_source_id, data_source_bytes in elem[1]:
            accumulator[data_source_id] = _parse_data_source_bytes(
                data_source_bytes)
        return [DataSourceCache(accumulator)]


def _build_data_source_mapping(
        elem: Tuple[bytes,
                    schemas.DataPointType]) -> Tuple[Optional[int], bytes]:
    data_source_bytes, data_point = elem
    return (data_point.data_point_metadata.data_source_id, data_source_bytes)


class BuildDataSourceCache(beam.PTransform):
    """Transform for build a DataSource Cache as a PCollection."""

    def expand(
        self, pcol: beam.PCollection[Tuple[bytes, schemas.DataPointType]]
    ) -> beam.PCollection[DataSourceCache]:
        return (pcol | beam.Map(_build_data_source_mapping) | beam.Distinct()  # pylint: disable=no-value-for-parameter
                | beam.Map(lambda x: (1, x)) | beam.GroupByKey() |
                beam.ParDo(_BuildDataSourceCache()))


def _parse_and_key_data_source_mappings(
        bigquery_row: Dict[str, Any]) -> Tuple[int, Tuple[int, bytes]]:
    data_source = echo_utils.parse_data_source(bigquery_row['DataSource'])
    return (1, (int(bigquery_row['DataSourceID']),
                data_source.SerializeToString()))


class BuildDataSourceCacheFromInternal(beam.PTransform):
    """Builds a DataSourceCache from internal Echo DataSource Mapping tables."""

    def __init__(self, bigquery_table_id: str, project_id: str,
                 service_account: str, creds: credentials.DsSdkCredentials,
                 bigquery_location: str, data_spec_to_filter_for: str):
        super().__init__()
        self._bigquery_table_id = bigquery_table_id
        self._project_id = project_id
        self._service_account = service_account
        self._creds = creds
        self._bigquery_location = bigquery_location
        self._data_spec_to_filter_for = data_spec_to_filter_for

    def expand(self, pcol) -> beam.PCollection[DataSourceCache]:
        return (pcol | bigquery_source_wrapper.GcpBigquerySourceWrapper(
            row_filter_builder=build_row_filters.PassThroughRowFilter(
                (self._bigquery_table_id,
                 f'DataSource.data_spec.name="{self._data_spec_to_filter_for}"'
                ), self._creds, self._project_id, self._bigquery_location),
            creds=self._creds,
            project_id=self._project_id,
            service_account=self._service_account) |
                beam.Map(_parse_and_key_data_source_mappings) |
                beam.GroupByKey() | beam.ParDo(_BuildDataSourceCache()))
