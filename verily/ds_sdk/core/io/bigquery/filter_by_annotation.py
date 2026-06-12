"""Transform for joining / filtering DataPoints with Annotations."""

import dataclasses
from typing import Dict, Iterable, Iterator, Set, Tuple, Union

import apache_beam as beam
import pandas as pd

from verily.ds_sdk.core import options
from verily.ds_sdk.core import schemas
from verily.ds_sdk.core.transforms import key_by
from verily.ds_sdk.core.utils import timestamps


def _get_measurement_time_truncated_by_hour(
        data_point_row: schemas.DataPointType) -> str:
    pd_timestamp = timestamps.beam_timestamp_to_pandas_timestamp(
        data_point_row.measurement_timestamp_utc)
    return pd_timestamp.floor('1h').isoformat()


def _get_annotation_start_time_truncated_by_hour(
        annotation: schemas.Annotation) -> str:
    pd_timestamp = timestamps.beam_timestamp_to_pandas_timestamp(
        annotation.start_timestamp_utc)
    return pd_timestamp.floor('1h').isoformat()


def _split_annotations_by_hour(
        annotation: schemas.Annotation,
        round_to_second: bool = False) -> Iterator[schemas.Annotation]:
    start_ts = timestamps.beam_timestamp_to_pandas_timestamp(
        annotation.start_timestamp_utc)
    end_ts = timestamps.beam_timestamp_to_pandas_timestamp(
        annotation.end_timestamp_utc)

    # This builds the hour time range set, then updates the bounds with the
    # correct start & end times.
    # Ex 1: (12:15, 13:45) -> [12:00, 13:00, 14:00] -> [12:15, 13:00, 13:45]
    # Ex 2: (12:15, 12:45) -> [12:00, 13:00] -> [12:15, 12:45]

    # If round_to_second = True, it will round the start and end times to the
    # nearest second.

    if round_to_second:
        start_ts = timestamps.round_down_to_nearest_second(start_ts)
        end_ts = timestamps.round_up_to_nearest_second(end_ts)

    truncated_hours = list(
        pd.date_range(start_ts.floor('1h'), end_ts.ceil('1h'), freq='1h'))
    truncated_hours[0] = start_ts
    truncated_hours[-1] = end_ts

    for i in range(len(truncated_hours) - 1):
        new_start = timestamps.datetime_to_beam_timestamp(truncated_hours[i])
        new_end = timestamps.datetime_to_beam_timestamp(truncated_hours[i + 1])

        yield dataclasses.replace(annotation,
                                  start_timestamp_utc=new_start,
                                  end_timestamp_utc=new_end)


class _SplitAndKeyAnnotationsByHour(beam.PTransform):
    """Split and key Annotations by hour."""

    def __init__(self, join_on_participant: bool,
                round_to_second: bool = False):
        super().__init__()
        self._join_on_participant = join_on_participant
        self._round_to_second = round_to_second

    def expand(self, pcol):
        return (pcol | beam.FlatMap(_split_annotations_by_hour,
            round_to_second=self._round_to_second) |
            key_by.KeyAnnotationsBy(
                by_device=True,
                by_participant=self._join_on_participant,
                additional_key_fns={
                'hour': _get_annotation_start_time_truncated_by_hour
                }))


class _KeyDataPointsByHour(beam.PTransform):
    """Key DataPoints by device, participant, and hour."""

    def __init__(self, join_on_participant: bool):
        super().__init__()
        self._join_on_participant = join_on_participant

    def expand(self, pcol):
        return (pcol | key_by.KeyDataPointsBy(
            by_device=True,
            by_participant=self._join_on_participant,
            additional_key_fns={
                'hour': _get_measurement_time_truncated_by_hour
            }))


class _JoinDataPointsWithAnnotations(beam.DoFn):
    """Joins DataPoint rows overlapping with Annotations.

  Optionally filters out DataPoints without an overlapping Annotation.

  `combined_data_with_key` is the keyed / co-grouped DataPoints and
  Annotations dict. The data is keyed/grouped by device, participant, hour.
  """

    def __init__(self, filter_data_points: bool,
                 required_annotation_labels: Set[str], join_if: options.JoinIf):
        super().__init__()
        self._filter_data_points = filter_data_points
        self._required_annotation_labels = required_annotation_labels
        self._join_if = join_if

    def process(  # type: ignore[override]
        self, combined_data_with_key: Tuple[key_by.Key, Dict[str, Union[
            Iterable[schemas.DataPointType],
            Iterable[schemas.Annotation]]]]) -> Iterable[schemas.DataPointType]:
        _, combined_data = combined_data_with_key
        data_points: Iterable[schemas.DataPointType] = (
            combined_data['data_points'])  # type: ignore
        annotations: Iterable[schemas.Annotation] = sorted(
            combined_data['annotations'],  # type: ignore
            key=lambda x: x.start_timestamp_utc)

        for dp in data_points:
            for ann in annotations:
                t = dp.measurement_timestamp_utc
                if ann.start_timestamp_utc <= t <= ann.end_timestamp_utc:
                    # Adds the Annotation's label to the DataPoint's set of
                    # labels.
                    dp.data_point_metadata.annotation_labels.add(
                        ann.annotation_label)
                # annotations are sorted by start time, so once this check hits
                # we can
                # safely exit the second loop.
                if ann.start_timestamp_utc > t:
                    break

            if self._join_if == options.JoinIf.ANY:
                # DataPoints are yielded if ANY required labels are found or
                # `filter_data_points` is False
                if (dp.data_point_metadata.annotation_labels or
                        not self._filter_data_points):
                    yield dp
            elif self._join_if == options.JoinIf.ALL:
                # label_diff will be empty if all the required annotations are
                # present.
                label_diff = (self._required_annotation_labels -
                              dp.data_point_metadata.annotation_labels)
                # DataPoints are yielded if ALL required labels are found or
                # `filter_data_points` is False
                if not label_diff or not self._filter_data_points:
                    yield dp


class _GroupDataPointsAndAnnotations(beam.PTransform):
    """Internal transform that groups DataPoints and Annotations."""

    def __init__(self, join_on_participant: bool,
                round_to_second: bool = False):
        super().__init__()
        self._join_on_participant = join_on_participant
        self._round_to_second = round_to_second

    def expand(self, pcols):
        data_points, annotations = pcols
        keyed_data_points = data_points | _KeyDataPointsByHour(
            self._join_on_participant)
        keyed_annotations = annotations | _SplitAndKeyAnnotationsByHour(
            self._join_on_participant, self._round_to_second)
        return ({
            'data_points': keyed_data_points,
            'annotations': keyed_annotations
        } | beam.CoGroupByKey())


class FilterByAnnotations(beam.PTransform):
    """Transform for filtering DataPoints by Annotations.

  This transform filters DataPoints by discarding ones that do not have a
  timestamp that overlaps with an Annotation with the same user/device from the
  input Annotation collection.
  """

    def __init__(self,
                 required_annotation_labels: Set[str],
                 join_if: options.JoinIf,
                 join_on_participant: bool = True,
                 round_to_second: bool = False):
        super().__init__()
        self._required_annotation_labels = required_annotation_labels
        self._join_if = join_if
        self._join_on_participant = join_on_participant
        self._round_to_second = round_to_second

    def expand(
        self, pcols: Tuple[beam.PCollection[schemas.DataPointType],
                           beam.PCollection[schemas.Annotation]]
    ) -> beam.PCollection[schemas.DataPointType]:
        return (pcols | _GroupDataPointsAndAnnotations(
            self._join_on_participant, self._round_to_second) | beam.ParDo(
                _JoinDataPointsWithAnnotations(
                    filter_data_points=True,
                    required_annotation_labels=self._required_annotation_labels,
                    join_if=self._join_if)))


class JoinWithAnnotations(beam.PTransform):
    """Transform for joining DataPoints with Annotations.

  This transform joins DataPoints with Annotations by adding the label of the
  Annotations to any DataPoints that overlaps with an Annotation with the same
  user/device from the input Annotation collection.
  """

    def __init__(self,
                 required_annotation_labels: Set[str],
                 join_if: options.JoinIf,
                 join_on_participant: bool = True,
                 round_to_second: bool = False):
        super().__init__()
        self._required_annotation_labels = required_annotation_labels
        self._join_if = join_if
        self._join_on_participant = join_on_participant
        self._round_to_second = round_to_second

    def expand(self, pcols):
        return (pcols | _GroupDataPointsAndAnnotations(
            self._join_on_participant, self._round_to_second) | beam.ParDo(
                _JoinDataPointsWithAnnotations(
                    filter_data_points=False,
                    required_annotation_labels=self._required_annotation_labels,
                    join_if=self._join_if)))
