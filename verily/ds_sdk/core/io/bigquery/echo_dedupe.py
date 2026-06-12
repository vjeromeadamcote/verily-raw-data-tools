"""Transform for deduping data from Echo BigQuery."""

import dataclasses
import math
from typing import Any, Dict, Iterable, List, NamedTuple, Tuple

import apache_beam as beam
from apache_beam.utils.timestamp import Timestamp

from verily.ds_sdk.core import schemas


def _values_equal(a: Any, b: Any) -> bool:
    """Compare two values, handling NaN specially (NaN == NaN is True)."""
    # Handle NaN: float NaN != NaN, but we want them to be equal
    if isinstance(a, float) and isinstance(b, float):
        if math.isnan(a) and math.isnan(b):
            return True
    # Handle dicts recursively
    if isinstance(a, dict) and isinstance(b, dict):
        if a.keys() != b.keys():
            return False
        return all(_values_equal(a[k], b[k]) for k in a)
    # Handle lists/tuples recursively
    if isinstance(a, (list, tuple)) and isinstance(b, (list, tuple)):
        if len(a) != len(b):
            return False
        return all(_values_equal(x, y) for x, y in zip(a, b))
    # Default comparison
    return a == b


class _RowKey(NamedTuple):
    """Key used to group duplicate / stale rows together."""
    data_source_id: int
    # Beam has issues encoding this as a Timestamp, so we store as a string.
    bucket_start_str: str

    @classmethod
    def from_row(cls, row: schemas.DataPointType):
        dp_metadata = row.data_point_metadata
        key = cls(
            data_source_id=dp_metadata.data_source_id,  # type: ignore
            bucket_start_str=dp_metadata.echo_metadata.  # type: ignore
            bucket_start.to_rfc3339())
        return key


def _rows_equal_excluding_echo_metadata(a: schemas.DataPointType,
                                        b: schemas.DataPointType) -> bool:
    a_dict = dataclasses.asdict(a)
    b_dict = dataclasses.asdict(b)
    # Does not include echo metadata (i.e. snapshot time) in the comparison.
    # The other removed metadata fields will all be the same since the rows are
    # grouped by data_source_id.
    del a_dict['data_point_metadata']
    del b_dict['data_point_metadata']
    # Use _values_equal to handle NaN values properly
    # (NaN == NaN should be True)
    return _values_equal(a_dict, b_dict)


class _DedupeKeyedEchoRows(beam.DoFn):
    """Dedupes Echo rows that are identical and/or stale."""

    def __init__(self):
        super().__init__()

        self._stale_counter = beam.metrics.Metrics.counter(
            'echo_bq_dupes', 'stale_rows_removed')
        self._identical_counter = beam.metrics.Metrics.counter(
            'echo_bq_dupes', 'identical_rows_removed')

    def process(  # type: ignore[override]
        self, elem: Tuple[_RowKey, Iterable[schemas.DataPointType]]
    ) -> Iterable[schemas.DataPointType]:
        _, rows = elem
        rows = self.remove_stale_rows(rows)
        rows = self.remove_identical_rows(rows)
        return rows

    def remove_stale_rows(
        self, rows: Iterable[schemas.DataPointType]
    ) -> Iterable[schemas.DataPointType]:
        # These keep track of the number of rows in/out for logging beam
        # metrics.
        num_rows_in = 0
        num_rows_out = 0

        grouped_by_write_time: Dict[Timestamp, List[schemas.DataPointType]] = {}
        for row in rows:
            num_rows_in += 1
            echo_metadata = row.data_point_metadata.echo_metadata
            write_time = echo_metadata.bucket_write_time  # type: ignore
            if write_time in grouped_by_write_time:
                grouped_by_write_time[write_time].append(row)
            else:
                grouped_by_write_time[write_time] = [row]

        max_write_time_for_bundle = max(grouped_by_write_time.keys())

        for row in grouped_by_write_time[max_write_time_for_bundle]:
            # Filters out rows that are marked as deleted.
            echo_metadata = row.data_point_metadata.echo_metadata
            if (echo_metadata is not None and
                    echo_metadata.deleted_time is not None):
                continue
            num_rows_out += 1
            yield row

        num_stale_rows = num_rows_in - num_rows_out
        self._stale_counter.inc(num_stale_rows)

    def remove_identical_rows(
        self, rows: Iterable[schemas.DataPointType]
    ) -> Iterable[schemas.DataPointType]:
        # These keep track of the number of rows in/out for logging beam
        # metrics.
        num_rows_in = 0
        num_rows_out = 0

        measurement_time_to_row: Dict[Timestamp, schemas.DataPointType] = {}
        for row in rows:
            num_rows_in += 1
            if row.measurement_timestamp_utc in measurement_time_to_row:
                existing = measurement_time_to_row[
                    row.measurement_timestamp_utc]
                if not _rows_equal_excluding_echo_metadata(row, existing):
                    raise RuntimeError(
                        f'Found two `identical` rows with different values: {row} vs {existing}'  # pylint: disable=line-too-long
                    )
            else:
                measurement_time_to_row[row.measurement_timestamp_utc] = row

        for row in measurement_time_to_row.values():
            num_rows_out += 1
            yield row

        num_identical_rows = num_rows_in - num_rows_out
        self._identical_counter.inc(num_identical_rows)


class RemoveEchoDupes(beam.PTransform):
    """Removes duplicate Echo BigQuery rows.

  Removes two classes of dupes:
    1. Rows where a new version of the data exists.

      Essentially this removes rows that have the same DataSource ID, User,
      & Bucket Start Time but a newer write time.

    2. Rows that are identical, excluding Echo metadata fields
       (i.e. snapshot time)
  """

    def expand(
        self, pcol: beam.PCollection[schemas.DataPointType]
    ) -> beam.PCollection[schemas.DataPointType]:
        return (pcol | beam.Map(lambda row: (_RowKey.from_row(row), row)) |
                beam.GroupByKey() | beam.ParDo(_DedupeKeyedEchoRows()))
