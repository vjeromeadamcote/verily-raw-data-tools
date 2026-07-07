"""Transform for grouping a keyed PCollection into a pandas DataFrame."""

import dataclasses
from functools import reduce
from typing import Any, Dict, Iterable, List, Optional, Tuple, Union

import apache_beam as beam
from apache_beam.utils.timestamp import Timestamp
import pandas as pd

from verily.raw_data_tools.schemas import schemas
from verily.raw_data_tools.transforms import key_by
from verily.raw_data_tools.utils import timestamps

_EXCLUDED_FIELDS = frozenset(['_state_key'])


# Note: This function modifies the existing DataFrame.
def _add_key_info_to_dataframe(df: pd.DataFrame, key: key_by.Key) -> None:
    df.attrs.update(dataclasses.asdict(key))


def _add_data_point_metadata_to_dataframe(
    df: pd.DataFrame, elements: Union[Iterable[schemas.Annotation],
                                      Iterable[schemas.DataPointType]]
) -> None:
    if elements and isinstance(next(iter(elements)), schemas.DataPoint):
        data_point_metadata = schemas.data_point_metadata_for_derived_data(  # pylint: disable=line-too-long
            elements)  # type: ignore
        df.attrs['data_point_metadata'] = data_point_metadata


def _parse_elements_into_dataframe(
    elements: Union[Iterable[schemas.Annotation],
                    Iterable[schemas.DataPointType]]
) -> pd.DataFrame:

    def convert_nested_fields_to_dict(elem):
        if dataclasses.is_dataclass(elem):
            return {
                k: convert_nested_fields_to_dict(v)
                for k, v in dataclasses.asdict(elem).items()
                if k not in _EXCLUDED_FIELDS
            }
        if isinstance(elem, dict):
            return {
                k: convert_nested_fields_to_dict(v)
                for k, v in elem.items()
                if k not in _EXCLUDED_FIELDS
            }

        if isinstance(elem, Timestamp):
            return timestamps.beam_timestamp_to_pandas_timestamp(elem)
        return elem

    return pd.DataFrame(
        [convert_nested_fields_to_dict(elem) for elem in elements])


class _ToDataFrame(beam.DoFn):
    """Creates a DataFrame from a single data type."""

    def process(  # type: ignore[override]
            self, keyed_elements) -> Iterable[pd.DataFrame]:
        group_key, elements = keyed_elements
        elements = list(elements)
        df = _parse_elements_into_dataframe(elements)
        _add_key_info_to_dataframe(df, group_key)
        _add_data_point_metadata_to_dataframe(df, elements)
        return [df]


class _ToDataFrameWithCombine(beam.DoFn):
    """Creates a DataFrame from multiple data types."""

    def __init__(self, combine_method: Optional[str]):
        super().__init__()
        self.combine_method = combine_method

    def process(  # type: ignore[override]
        self,
        keyed_elements_dict: Tuple[key_by.Key, Dict[str, Union[
            Iterable[schemas.Annotation], Iterable[schemas.DataPointType]]]],
    ) -> Iterable[Union[pd.DataFrame, Dict[str, pd.DataFrame]]]:
        group_key, elements_dict = keyed_elements_dict
        keyed_data_frames: Dict[str, pd.DataFrame] = {}
        all_data_points: List[Any] = []
        for key, elements in elements_dict.items():
            elements = list(elements)  # type: ignore
            df = _parse_elements_into_dataframe(elements)
            if not df.empty:
                _add_key_info_to_dataframe(df, group_key)
                if self.combine_method is None:
                    _add_data_point_metadata_to_dataframe(df, elements)
                else:
                    all_data_points.extend(elements)
                keyed_data_frames[key] = df

        if self.combine_method is None:
            to_return = keyed_data_frames

        elif len(keyed_data_frames) == 0:
            to_return = pd.DataFrame()

        elif len(keyed_data_frames) == 1:
            to_return = list(keyed_data_frames.values())[0]

        elif self.combine_method == 'concat':
            to_return = pd.concat(keyed_data_frames.values())

        elif self.combine_method == 'merge':
            try:
                to_return = reduce(
                    lambda left, right: pd.merge(
                        left[1],  # type: ignore
                        right[1],  # type: ignore
                        suffixes=(
                            f'_{left[0]}',  # type: ignore
                            f'_{right[0]}'  # type: ignore
                        ),
                        on='measurement_timestamp_utc',
                        # TODO(tanke): Add option to let user specify the
                        # `how` / any other pandas args for their combine
                        # method.
                        how='outer'),
                    keyed_data_frames.items())  # type: ignore
            # TODO(tanke): look into alternatives to catching the key error.
            except KeyError:
                to_return = reduce(
                    lambda left, right: pd.merge(
                        left[1],  # type: ignore
                        right[1],  # type: ignore
                        suffixes=(
                            f'_{left[0]}',  # type: ignore
                            f'_{right[0]}'  # type: ignore
                        ),
                        on=['start_timestamp_utc', 'end_timestamp_utc'],
                        how='outer'),
                    keyed_data_frames.items())  # type: ignore
        else:
            raise ValueError(
                f'Unsupported combine method: `{self.combine_method}`')

        if isinstance(to_return, pd.DataFrame):
            _add_key_info_to_dataframe(to_return, group_key)
            _add_data_point_metadata_to_dataframe(to_return, all_data_points)

        yield to_return


class GroupIntoDataFrames(beam.PTransform):
    """Groups keyed data into pandas DataFrames.

    The key and time range will be attached to the DataFrame as attributes, and
    can be accessed with df.attrs['device_id'] and
    df.attrs['additional_keys']['start_time_range_micros'].
    """

    def expand(
        self, pcol: beam.PCollection[Tuple[key_by.Key,
                                           Union[schemas.Annotation,
                                                 schemas.DataPointType]]]
    ) -> beam.PCollection[pd.DataFrame]:
        return pcol | beam.GroupByKey() | beam.ParDo(_ToDataFrame())


class CoGroupIntoDataFrames(beam.PTransform):
    """Groups / Joins keyed data into pandas DataFrames.

    The key and time range will be attached to the DataFrame as attributes, and
    can be accessed with df.attrs['device_id'] and
    df.attrs['additional_keys']['start_time_range_micros'].

    `combine_method` specifies how to combine the dataframes. Valid options are:
        - None: DataFrames will not be combined. Instead a dictionary will be
                returned mapping each key to the converted Dataframe..
        - concat: DataFrames for each input data point will be concated together
        - merge: DataFrames for each input data point will be merged together
    """

    ALLOWED_COMBINE_METHODS = {'concat', 'merge', None}

    def __init__(self, *, combine_method: Optional[str]):
        super().__init__()

        if combine_method not in self.ALLOWED_COMBINE_METHODS:
            raise ValueError(f'Unsupported combine_method: `{combine_method}`. '
                             f'Allowed methods: {self.ALLOWED_COMBINE_METHODS}')

        self._combine_method = combine_method

    def expand(
        self,
        # NOTE: THis type hint should actually be a Dict, but wrapping in a dict
        # causes beam to choose the wrong coder for key_by.Key.
        pcol_dict: beam.PCollection[Tuple[key_by.Key,
                                          Union[schemas.Annotation,
                                                schemas.DataPointType]]]
    ) -> beam.PCollection[Union[pd.DataFrame, Dict[str, pd.DataFrame]]]:

        return (pcol_dict | beam.CoGroupByKey() |
                beam.ParDo(_ToDataFrameWithCombine(self._combine_method)))
