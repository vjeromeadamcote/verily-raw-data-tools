"""Composite PTransforms for build pandas DataFrames."""

import datetime
from typing import Any, Dict, Optional, Union

import apache_beam as beam
import pandas as pd

from verily.raw_data_tools.schemas import schemas
from verily.raw_data_tools.transforms.group_into_data_frames import CoGroupIntoDataFrames
from verily.raw_data_tools.transforms.group_into_data_frames import GroupIntoDataFrames
from verily.raw_data_tools.transforms import key_by
from verily.raw_data_tools.utils import timestamps


def _key_helper(pcoll,
                key_fn: beam.PTransform,
                combine_method: Optional[str],
                for_data_points: bool = False,
                for_annotations: bool = False):

    if for_data_points == for_annotations:
        raise ValueError(
            'Only one of `for_data_points` or `for_annotations should be '
            'provided.`')

    transform_label = 'DataPoints' if for_data_points else 'Annotations'
    # User is building the DataFrame from multiple PCollections
    # --> We need to use a CoGroupBy in this case.
    if isinstance(pcoll, dict):
        keyed_pcols = {}
        for key, p in pcoll.items():
            keyed_pcols[key] = p | f'Key {key} {transform_label}' >> key_fn
        return keyed_pcols | CoGroupIntoDataFrames(
            combine_method=combine_method)

    # User is building the DataFrame from a single PCollection
    # --> We need to use a GroupBy in this case.
    else:
        return pcoll | key_fn | GroupIntoDataFrames()


def key_data_point_by_measurement_timestamp(data_point):
    return timestamps.beam_timestamp_to_pandas_timestamp(
        data_point.measurement_timestamp_utc).isoformat()


class BuildDataPointDataFrames(object):
    """Composite PTransforms for building DataPoint pandas DataFrames."""

    @classmethod
    def check_input_pcoll(cls, pcoll):
        if isinstance(pcoll, tuple):
            raise TypeError(
                'BuildDataPointDataFrames cannot use tuples. Pass your input '
                'in using this format: '
                '{"key1": pcol, "key2": pcol2} | BuildDataPointDataFrames')

    class PerParticipantDevice(beam.PTransform):
        """Builds DataFrames grouped by Participant & Device.

        `combine_method` specifies how to combine the dataframes. Valid options
        are:
        - None: DataFrames will not be combined. Instead a dictionary will be
                returned mapping each key to the converted Dataframe..
        - concat: DataFrames for each input data point will be concated together
        - merge: DataFrames for each input data point will be merged together
        """

        def __init__(self, *, combine_method: Optional[str] = None):
            super().__init__()

            self._combine_method = combine_method

        def expand(
            self, pcoll: Union[Dict[Any,
                                    beam.PCollection[schemas.DataPointType]],
                               beam.PCollection[schemas.DataPointType]]
        ) -> Union[beam.PCollection[pd.DataFrame], beam.PCollection[Dict[
                str, pd.DataFrame]]]:
            BuildDataPointDataFrames.check_input_pcoll(pcoll)

            key_fn = key_by.KeyDataPointsBy(by_device=True, by_participant=True)
            return _key_helper(pcoll,
                               key_fn,
                               self._combine_method,
                               for_data_points=True)

    class PerParticipantDeviceTimestamp(beam.PTransform):
        """Builds DataFrames grouped by Participant, Device, and timestamp.

        `combine_method` specifies how to combine the dataframes. Valid options
        are:
        - None: DataFrames will not be combined. Instead a dictionary will be
                returned mapping each key to the converted Dataframe..
        - concat: DataFrames for each input data point will be concated together
        - merge: DataFrames for each input data point will be merged together
        """

        def __init__(self, *, combine_method: Optional[str] = None):
            super().__init__()

            self._combine_method = combine_method

        def expand(
            self, pcoll: Union[Dict[Any,
                                    beam.PCollection[schemas.DataPointType]],
                               beam.PCollection[schemas.DataPointType]]
        ) -> Union[beam.PCollection[pd.DataFrame], beam.PCollection[Dict[
                str, pd.DataFrame]]]:
            BuildDataPointDataFrames.check_input_pcoll(pcoll)

            key_fn = key_by.KeyDataPointsBy(
                by_device=True,
                by_participant=True,
                additional_key_fns={
                    'ts': key_data_point_by_measurement_timestamp
                })
            return _key_helper(pcoll,
                               key_fn,
                               self._combine_method,
                               for_data_points=True)

    class PerParticipantDeviceWindow(beam.PTransform):
        """Builds DataFrames grouped by Participant & Device, with windowing.

        `combine_method` specifies how to combine the dataframes. Valid options
        are:
        - None: DataFrames will not be combined. Instead a dictionary will be
                returned mapping each key to the converted Dataframe..
        - concat: DataFrames for each input data point will be concated together
        - merge: DataFrames for each input data point will be merged together
        """

        def __init__(self,
                     *,
                     beam_window_fn: beam.transforms.window.WindowFn,
                     combine_method: Optional[str] = None):
            super().__init__()

            self._beam_window_fn = beam_window_fn
            self._combine_method = combine_method

        def expand(
            self, pcoll: Union[Dict[Any,
                                    beam.PCollection[schemas.DataPointType]],
                               beam.PCollection[schemas.DataPointType]]
        ) -> Union[beam.PCollection[pd.DataFrame], beam.PCollection[Dict[
                str, pd.DataFrame]]]:
            BuildDataPointDataFrames.check_input_pcoll(pcoll)

            key_fn = key_by.KeyDataPointsByParticipantDeviceTimeRange(
                beam_window_fn=self._beam_window_fn)
            return _key_helper(pcoll,
                               key_fn,
                               self._combine_method,
                               for_data_points=True)

    class PerParticipantDeviceWindowLocalTimezone(beam.PTransform):
        """Builds DataFrames by Participant & Device in users local timezone.

        `combine_method` specifies how to combine the dataframes. Valid options
        are:
        - None: DataFrames will not be combined. Instead a dictionary will be
                returned mapping each key to the converted Dataframe..
        - concat: DataFrames for each input data point will be concated together
        - merge: DataFrames for each input data point will be merged together
        """

        def __init__(self,
                     *,
                     beam_window_fn: beam.transforms.window.WindowFn,
                     data_source_cache: Optional[beam.pvalue.AsDict] = None,
                     combine_method: Optional[str] = None,
                     utc_offset_map: Optional[beam.pvalue.AsDict] = None):
            super().__init__()

            self._beam_window_fn = beam_window_fn
            self._combine_method = combine_method
            self._data_source_cache = data_source_cache
            self._utc_offset_map = utc_offset_map

        def expand(
            self, pcoll: Union[Dict[Any,
                                    beam.PCollection[schemas.DataPointType]],
                               beam.PCollection[schemas.DataPointType]]
        ) -> Union[beam.PCollection[pd.DataFrame], beam.PCollection[Dict[
                str, pd.DataFrame]]]:
            BuildDataPointDataFrames.check_input_pcoll(pcoll)

            key_fn = (
                key_by.KeyDataPointsByParticipantDeviceTimeRangeInLocalTimezone(
                    beam_window_fn=self._beam_window_fn,
                    data_source_cache=self._data_source_cache,  # type: ignore[arg-type] # pylint: disable=line-too-long
                    utc_offset_map=self._utc_offset_map  # type: ignore[arg-type] # pylint: disable=line-too-long
                ))

            return _key_helper(pcoll,
                               key_fn,
                               self._combine_method,
                               for_data_points=True)

    class PerParticipantDeviceWindowInTimezone(beam.PTransform):
        """Builds DataFrames by Participant & Device in specified timezone.

        `combine_method` specifies how to combine the dataframes. Valid options
        are:
        - None: DataFrames will not be combined. Instead a dictionary will be
                returned mapping each key to the converted Dataframe..
        - concat: DataFrames for each input data point will be concated together
        - merge: DataFrames for each input data point will be merged together
        """

        def __init__(
            self,
            *,
            beam_window_fn: beam.transforms.window.WindowFn,
            timezone: datetime.tzinfo,
            combine_method: Optional[str] = None,
        ):
            super().__init__()

            self._beam_window_fn = beam_window_fn
            self._combine_method = combine_method
            self._timezone = timezone

        def expand(
            self, pcoll: Union[Dict[Any,
                                    beam.PCollection[schemas.DataPointType]],
                               beam.PCollection[schemas.DataPointType]]
        ) -> Union[beam.PCollection[pd.DataFrame], beam.PCollection[Dict[
                str, pd.DataFrame]]]:
            BuildDataPointDataFrames.check_input_pcoll(pcoll)

            key_fn = (
                key_by.KeyDataPointsByParticipantDeviceTimeRangeInTimezone(
                    beam_window_fn=self._beam_window_fn,
                    timezone=self._timezone))

            return _key_helper(pcoll,
                               key_fn,
                               self._combine_method,
                               for_data_points=True)


class BuildAnnotationDataFrames(object):
    """Composite PTransforms for building Annotation pandas DataFrames."""

    @classmethod
    def check_input_pcoll(cls, pcoll):
        if isinstance(pcoll, tuple):
            raise TypeError(
                'BuildAnnotationDataFrames cannot use tuples. Pass your input '
                'in '
                'using this format: '
                '{"key1": pcol, "key2": pcol2} | BuildAnnotationDataFrames')

    class PerParticipantDevice(beam.PTransform):
        """Builds DataFrames grouped by Participant & Device.

        `combine_method` specifies how to combine the dataframes. Valid options
        are:
        - None: DataFrames will not be combined. Instead a dictionary will be
                returned mapping each key to the converted Dataframe..
        - concat: DataFrames for each input data point will be concated together
        - merge: DataFrames for each input data point will be merged together
        """

        def __init__(self, *, combine_method: Optional[str] = None):
            super().__init__()

            self._combine_method = combine_method

        def expand(
            self, pcoll: beam.PCollection[schemas.Annotation]
        ) -> Union[beam.PCollection[pd.DataFrame], beam.PCollection[Dict[
                str, pd.DataFrame]]]:
            BuildAnnotationDataFrames.check_input_pcoll(pcoll)

            key_fn = key_by.KeyAnnotationsBy(by_device=True,
                                             by_participant=True)
            return _key_helper(pcoll,
                               key_fn,
                               self._combine_method,
                               for_annotations=True)

    class PerParticipantDeviceWindow(beam.PTransform):
        """Builds Annotations grouped by Participant & Device, with windowing.

        `combine_method` specifies how to combine the dataframes. Valid options
        are:
        - None: DataFrames will not be combined. Instead a dictionary will be
                returned mapping each key to the converted Dataframe..
        - concat: DataFrames for each input data point will be concated together
        - merge: DataFrames for each input data point will be merged together
        """

        def __init__(self,
                     *,
                     beam_window_fn: beam.transforms.window.WindowFn,
                     window_by_start_time: bool = False,
                     window_by_end_time: bool = False,
                     combine_method: Optional[str] = None):
            super().__init__()

            self._beam_window_fn = beam_window_fn
            self._window_by_start_time = window_by_start_time
            self._window_by_end_time = window_by_end_time
            self._combine_method = combine_method

        def expand(
            self, pcoll: Union[Dict[Any, beam.PCollection[schemas.Annotation]],
                               beam.PCollection[schemas.Annotation]]
        ) -> Union[beam.PCollection[pd.DataFrame], beam.PCollection[Dict[
                str, pd.DataFrame]]]:
            BuildAnnotationDataFrames.check_input_pcoll(pcoll)

            key_fn = key_by.KeyAnnotationsByParticipantDeviceTimeRange(
                beam_window_fn=self._beam_window_fn,
                by_start_timestamp=self._window_by_start_time,
                by_end_timestamp=self._window_by_end_time)
            return _key_helper(pcoll,
                               key_fn,
                               self._combine_method,
                               for_annotations=True)


class BuildDataFrames(beam.PTransform):
    """Simplified facade for building DataFrames from DataPoints.

    Wraps BuildDataPointDataFrames with a simpler constructor matching
    the public API.

    Args:
        window_seconds: If provided, groups data into fixed time windows of
            this many seconds. If None, groups all data per participant/device.
        combine_method: How to combine data from multiple sources. Passed
            through to the underlying transform.
        include_metadata: Reserved for future use. Currently ignored.
        sort_by_time: Reserved for future use. Currently ignored.
    """

    def __init__(self,
                 window_seconds: Optional[int] = None,
                 combine_method: Optional[str] = None,
                 include_metadata: bool = True,
                 sort_by_time: bool = True):
        super().__init__()
        self._window_seconds = window_seconds
        self._combine_method = combine_method

    def expand(self, pcoll):
        if self._window_seconds is not None:
            return pcoll | BuildDataPointDataFrames.PerParticipantDeviceWindow(
                beam_window_fn=beam.transforms.window.FixedWindows(
                    self._window_seconds),
                combine_method=self._combine_method)
        return pcoll | BuildDataPointDataFrames.PerParticipantDevice(
            combine_method=self._combine_method)
