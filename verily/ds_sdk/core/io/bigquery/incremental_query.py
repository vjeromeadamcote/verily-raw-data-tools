"""Generate DS SDK conditions for incremental pipeline runs."""

import ibis
import ibis_bigquery
import pandas as pd

from verily.ds_sdk.core import options
from verily.ds_sdk.core import query_runner as qr

INCREMENTAL_ANNOTATION_LABEL = 'incremental'


def _buid_incremental_query(query_options: options.IncrementalQueryOptions,
                            ibis_data_points_table: ibis.expr.types.TableExpr,
                            paritipants_table: ibis.expr.types.TableExpr,
                            use_internal_echo: bool):
    """Builds a query for the data that is needed for the incremental query."""
    joined = ibis_data_points_table.left_join(
        paritipants_table,
        ((ibis_data_points_table.DeviceID == paritipants_table.DeviceId) &
         (ibis_data_points_table.DataPointTime >= paritipants_table.StartTime) &
         (ibis_data_points_table.DataPointTime < paritipants_table.EndTime)))
    ibis_data_points_table = joined[ibis_data_points_table,
                                    paritipants_table.ParticipantId]
    write_start_time = query_options.write_start_time
    if write_start_time is None:
        # No write start provided default to beginning of time
        write_start_time = pd.Timestamp.min.date()
    write_end_time = query_options.write_end_time
    if write_end_time is None:
        # No write end provided default to end of time
        write_end_time = pd.Timestamp.max.date()

    inner_join_view = ibis_data_points_table.view()
    if (query_options.incremental_query_mode ==
            options.IncrementQueryMode.EXPORT):
        start_timestamp_utc = inner_join_view.DataPointTime.name(
            'start_timestamp_utc')
        end_timestamp_utc = inner_join_view.DataPointTime.name(
            'end_timestamp_utc')

    value, unit = query_options.incremental_timestamp_part.to_ibis_add_arguments()  # pylint: disable=line-too-long
    if (query_options.incremental_query_mode ==
            options.IncrementQueryMode.EXPAND_INTERVALS):
        # Expanding intervals by +/- unit multiplied by value. Truncating
        # results to hours to reduce number of overlapping intervals.
        start_timestamp_utc = (inner_join_view.DataPointTime -
                               ibis.api.interval(value=value, unit=unit)
                              ).truncate('h').name('start_timestamp_utc')
        # Because there is no ceiling method, we are adding 2x the expand
        # interval to start_timestamp_utc and then tacking on 2 more hours due
        # to the hour truncation.
        end_timestamp_utc = (
            start_timestamp_utc +
            ibis.api.interval(value=(2 * value), unit=unit) +
            ibis.api.interval(value=2, unit='h')).name('end_timestamp_utc')
    elif (query_options.incremental_query_mode ==
          options.IncrementQueryMode.TRUNCATE_INTERVALS):
        start_timestamp_utc = inner_join_view.DataPointTime.truncate(unit).name(
            'start_timestamp_utc')
        end_timestamp_utc = (
            start_timestamp_utc +
            ibis.api.interval(value=value, unit=unit)).name('end_timestamp_utc')

    selects = [
        inner_join_view.ParticipantId.name('user_id'),
        inner_join_view.DeviceID.name('device_id'),
        ibis.literal(INCREMENTAL_ANNOTATION_LABEL).name('annotation_label'),
        start_timestamp_utc.name('start_timestamp_utc'),
        end_timestamp_utc.name('end_timestamp_utc'),
    ]

    bucket_write_time_cond = None
    if use_internal_echo:
        bucket_write_time_cond = (
            (inner_join_view.DataPointWriteTime >= ibis.timestamp(
                write_start_time.isoformat())) &
            (inner_join_view.DataPointWriteTime < ibis.timestamp(
                write_end_time.isoformat())))
    else:
        bucket_write_time_cond = (
            (inner_join_view.BucketWriteTime >= ibis.timestamp(
                write_start_time.isoformat())) &
            (inner_join_view.BucketWriteTime < ibis.timestamp(
                write_end_time.isoformat())))

    return inner_join_view[bucket_write_time_cond][selects].distinct()


def create_incremental_table(output_table: str,
                             query_options: options.IncrementalQueryOptions,
                             ibis_data_points_table: ibis.expr.types.TableExpr,
                             participants_table: ibis.expr.types.TableExpr,
                             query_runner: qr.QueryRunner,
                             use_internal_echo: bool) -> bool:
    """Creates a BigQuery table containing the data for incremental queries.

    Returns false if the incremental table is empty, and false otherwise.
    """

    inner_join_query = ibis_bigquery.compile(  # type: ignore
        _buid_incremental_query(query_options, ibis_data_points_table,
                                participants_table, use_internal_echo))
    print(f'Generating intermediate incremental table {output_table}.')
    results = query_runner.execute_query(inner_join_query,
                                         output_table=output_table)
    return results.total_rows != 0
