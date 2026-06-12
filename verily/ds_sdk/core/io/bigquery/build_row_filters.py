"""Helper transform for building row filters using Annotations."""

import time
from typing import Iterable, List, Optional, Set, Tuple

import apache_beam as beam
from google.cloud import bigquery  # type: ignore
import ibis
import ibis_bigquery
import pandas as pd

from verily import ds_sdk
from verily.ds_sdk.core import conditions
from verily.ds_sdk.core import schemas
from verily.ds_sdk.core.gcp import credentials
from verily.ds_sdk.core.io.bigquery.utils import echo_utils
from verily.ds_sdk.core.utils import timestamps


def build_row_filter_from_condition(
    condition: Optional[conditions.Condition],
    ibis_table: ibis.expr.types.TableExpr,
    as_annotation: bool = False,
    as_data_point: bool = False,
) -> Optional[str]:
    if as_annotation == as_data_point:
        raise ValueError(
            '`as_annotation` & `as_data_point` cannot both be set or unset.'
        )
    row_filter = None
    if condition is not None:
        cond = None
        if as_annotation:
            cond = condition.annotations_condition(
                ibis_table, include_annotation_conditions=True
            )
        else:
            cond = condition.data_points_condition(
                ibis_table, include_annotation_conditions=False
            )
        if cond is not None:
            query = ibis_bigquery.compile(ibis_table[cond])
            # The first index will be the actual condition we want to filter on.
            row_filter = query.split('WHERE', 1)[1]
    return row_filter


def _annotation_to_day_partitions(
    annotation: schemas.Annotation,
) -> Iterable[Tuple[pd.Timestamp, pd.Timestamp]]:
    start_timestamp = timestamps.beam_timestamp_to_pandas_timestamp(
        annotation.start_timestamp_utc
    ).floor('1d')
    end_timestamp = timestamps.beam_timestamp_to_pandas_timestamp(
        annotation.end_timestamp_utc
    ).ceil('1d')

    prev_timestamp = None
    for curr_timestamp in pd.date_range(start_timestamp, end_timestamp):
        if prev_timestamp is not None:
            yield (prev_timestamp, curr_timestamp)
        prev_timestamp = curr_timestamp


def _merge_time_range_partitions(
    time_range_partitions: Iterable[Tuple[pd.Timestamp, pd.Timestamp]],
) -> Iterable[conditions.TimeRangeCondition]:
    sorted_time_ranges = sorted(time_range_partitions, key=lambda pair: pair[0])
    if sorted_time_ranges:
        merged_time_range = sorted_time_ranges[0]
        for curr_time_range in sorted_time_ranges[1:]:
            if merged_time_range[1] == curr_time_range[0]:
                # time range partitions touch -> merge the two time ranges into
                # a one.
                merged_time_range = (merged_time_range[0], curr_time_range[1])
            else:
                # Partitions don't touch -> yield a TimeRange condition for the
                # interval
                yield conditions.TimeRangeCondition(
                    conditions.TimeRange(*merged_time_range)
                )
                merged_time_range = curr_time_range
        yield conditions.TimeRangeCondition(
            conditions.TimeRange(*merged_time_range)
        )


def _add_time_range_conditions_to_base_condition(
    time_range_conditions: Iterable[conditions.TimeRangeCondition],
    base_condition: Optional[conditions.Condition],
    table_id: str,
) -> Tuple[str, Optional[conditions.Condition]]:
    all_time_range = None
    for time_range_cond in time_range_conditions:
        if all_time_range is None:
            all_time_range = time_range_cond
        else:
            all_time_range |= time_range_cond

    merged_condition = base_condition
    if all_time_range is not None and base_condition is not None:
        merged_condition = base_condition & all_time_range
    elif all_time_range is not None:
        merged_condition = all_time_range

    return (table_id, merged_condition)


def _data_points_table_and_condition_to_table_and_row_filter(
    table_and_condition: Tuple[str, Optional[conditions.Condition]],
    ibis_table: ibis.expr.types.TableExpr,
) -> Tuple[str, Optional[str]]:
    table, condition = table_and_condition
    row_filter = build_row_filter_from_condition(
        condition, ibis_table, as_data_point=True
    )
    return (table, row_filter)


def _materialize_tables(
    bq_client: bigquery.Client,
    table_id_row_filter: Tuple[str, Optional[str]],
    bigquery_location: str,
    dedupe_unique_identifer: Optional[str],
    dedupe_column_to_keep_max_value: Optional[str],
) -> Tuple[str, Optional[str]]:
    dataset_name = echo_utils.get_temp_bigquery_dataset_for_location(
        bigquery_location
    )
    table_id, row_filter = table_id_row_filter
    table = bq_client.get_table(table_id)
    dedupe_by_query = (
        dedupe_unique_identifer is not None
        and dedupe_column_to_keep_max_value is not None
    )
    if table.table_type == 'TABLE' and not dedupe_by_query:
        return (table_id, row_filter)
    if table.table_type != 'TABLE':
        print(
            f'Table {table_id} was a view, temporarily materializing the table '
            'so it can be queried with the BigQuery storage API.'
        )
    else:
        print(f'Dedupe DataPoints in {table_id} by query.')
    project = table_id.split('.')[0]
    output_table = f'{project}.{dataset_name}.temp_materialized_view_{int(time.time() * 1000)}'  # pylint: disable=line-too-long
    job_config = bigquery.QueryJobConfig(destination=output_table)
    materialize_query = f'SELECT * FROM `{table_id}`'
    if row_filter is not None:
        materialize_query = materialize_query + f' WHERE {row_filter}'
    if dedupe_by_query:
        materialize_query = f"""SELECT * EXCEPT(row_num)
        FROM (
            SELECT *,
            ROW_NUMBER() OVER(
                PARTITION BY {dedupe_unique_identifer}
                ORDER BY {dedupe_column_to_keep_max_value} DESC
            ) as row_num
            FROM ({materialize_query})
        )
        WHERE row_num = 1"""
    bq_client.query(materialize_query, job_config=job_config).result()
    return (output_table, row_filter)


class BuildDataPointTableRowFilters(beam.PTransform):
    """Builds row filters for reading from DataPoint BigQuery tables."""

    def __init__(
        self,
        data_point_table_id: str,
        data_point_condition: Optional[conditions.Condition],
        ibis_table: ibis.expr.types.TableExpr,
        # TODO(tanke): Refactor the imports to allow us to set the proper
        # type annotation for `annotations`.
        annotations: Optional[
            List[
                'ds_sdk.core.io.bigquery.annotation_source.AnnotationRowSource'
            ]
        ],
        creds: credentials.DsSdkCredentials,
        billing_project: str,
        bigquery_location: str,
        dedupe_unique_identifer: Optional[str],
        dedupe_column_to_keep_max_value: Optional[str],
    ):
        super().__init__()

        self._data_point_table_id = data_point_table_id
        self._data_point_condition = data_point_condition
        self._ibis_table = ibis_table
        self._annotations = annotations if annotations is not None else []
        self._creds = creds
        self._billing_project = billing_project
        self._bigquery_location = bigquery_location
        self._dedupe_unique_identifer = dedupe_unique_identifer
        self._dedupe_column_to_keep_max_value = dedupe_column_to_keep_max_value

        self._rows = None

    def materialize(self) -> Tuple[str, Optional[str]]:
        creds, _ = self._creds.get_credentials()
        bq_client = bigquery.Client(
            credentials=creds,
            project=self._billing_project,
            location=self._bigquery_location,
        )
        # If no Annotation sources are provided, we can pass through the
        # user-defined conditions as row filters.
        if not self._annotations:
            row_filter = build_row_filter_from_condition(
                self._data_point_condition, self._ibis_table, as_data_point=True
            )
            return _materialize_tables(
                bq_client,
                (self._data_point_table_id, row_filter),
                self._bigquery_location,
                self._dedupe_unique_identifer,
                self._dedupe_column_to_keep_max_value,
            )

        # Returns the cached rows
        if self._rows is not None:
            return self._rows

        annotations: List[schemas.Annotation] = []
        for ann_source in self._annotations:
            annotations.extend(ann_source.get_raw_annotations())

        day_partitions: Set[Tuple[pd.Timestamp, pd.Timestamp]] = set()
        for ann in annotations:
            day_partitions.update(_annotation_to_day_partitions(ann))

        time_range_conditions = _merge_time_range_partitions(day_partitions)

        data_point_condition = _add_time_range_conditions_to_base_condition(
            time_range_conditions,
            self._data_point_condition,
            self._data_point_table_id,
        )
        table_and_row_filter = (
            _data_points_table_and_condition_to_table_and_row_filter(
                data_point_condition, self._ibis_table
            )
        )

        return _materialize_tables(
            bq_client,
            table_and_row_filter,
            self._bigquery_location,
            self._dedupe_unique_identifer,
            self._dedupe_column_to_keep_max_value,
        )


class BuildAnnotationTableRowFilters(beam.PTransform):
    """Builds row filters for reading from Annotation BigQuery tables."""

    def __init__(
        self,
        annotation_table_id: str,
        annotation_condition: conditions.Condition,
        ibis_table: ibis.expr.types.TableExpr,
        creds: credentials.DsSdkCredentials,
        billing_project: str,
        bigquery_location: str,
    ):
        super().__init__()

        self._annotation_table_id = annotation_table_id
        self._annotation_condition = annotation_condition
        self._ibis_table = ibis_table
        self._creds = creds
        self._billing_project = billing_project
        self._bigquery_location = bigquery_location

        self._rows = None

    def materialize(self) -> Tuple[str, Optional[str]]:
        row_filter = build_row_filter_from_condition(
            self._annotation_condition, self._ibis_table, as_annotation=True
        )

        creds, _ = self._creds.get_credentials()
        bq_client = bigquery.Client(
            credentials=creds,
            project=self._billing_project,
            location=self._bigquery_location,
        )
        return _materialize_tables(
            bq_client,
            (self._annotation_table_id, row_filter),
            self._bigquery_location,
            None,
            None,
        )


class PassThroughRowFilter(beam.PTransform):
    """Builds row filters for reading an entire BigQuery table."""

    def __init__(
        self,
        row_filter: Tuple[str, Optional[str]],
        creds: credentials.DsSdkCredentials,
        billing_project: str,
        bigquery_location: str,
    ):
        super().__init__()

        self._row_filter = row_filter
        self._creds = creds
        self._billing_project = billing_project
        self._bigquery_location = bigquery_location

    def materialize(self) -> Tuple[str, Optional[str]]:
        creds, _ = self._creds.get_credentials()
        bq_client = bigquery.Client(
            credentials=creds,
            project=self._billing_project,
            location=self._bigquery_location,
        )
        return _materialize_tables(
            bq_client, self._row_filter, self._bigquery_location, None, None
        )
