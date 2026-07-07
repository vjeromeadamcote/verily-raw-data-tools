"""Module for unpacking sensor data."""
from dataclasses import dataclass
import logging
import time
from typing import Any, Callable, Dict, Iterable, List, Optional, Union
import warnings

import apache_beam as beam
from apache_beam.utils.timestamp import Timestamp
import numpy as np
import pandas as pd

from verily.raw_data_tools.schemas.schemas.shared_schemas import DataPoint, DataPointType
from verily.raw_data_tools.schemas.schemas.schema_utils import data_point_metadata_for_derived_data_from_df
from verily.raw_data_tools.transforms.build_data_frames import BuildDataPointDataFrames
from verily.raw_data_tools.utils.data_source_cache import DataSourceCache
from verily.raw_data_tools.unpacking import data_unpacking_legacy
from verily.raw_data_tools.unpacking import data_unpacking_numba

# threshold for error from nominal sampling rate
_NOMINAL_FS_ERROR_THRESHOLD = 0.20


class DataUnpacker(object):
    """Class to unpack data from DS SDK."""

    def __init__(self,
                 *,
                 error_thresh: float = 0.05,
                 ignore_median_fs_error: bool = False,
                 fallback_to_legacy: bool = False):
        self.error_thresh = error_thresh
        self.ignore_median_fs_error = ignore_median_fs_error
        self.fallback_to_legacy = fallback_to_legacy
        self.median_fs_error_counter = beam.metrics.Metrics.counter(
            'data_unpacking', 'median_fs_error')

        if fallback_to_legacy and ignore_median_fs_error:
            raise ValueError(
                'Only one of fallback_to_legacy or ignore_median_fs_error '
                'should be True')

    def _calc_time_df(self):
        """Method to create a dataframe with intermediate timing information.

        In order to simplify the logic and to increase the speed of calculating
        the interpacket sampling rates, a global index for each true timestamp
        is calculated based on the sum of the packet lengths of the preceding
        packet and current true_timestamp_sample_index.
        """
        # create time data frame
        time_df = self.df[[
            'true_timestamp_millis', 'true_timestamp_sample_index', 'packet_len'
        ]].copy()
        time_df['true_timestamp_global_index'] = np.cumsum(np.insert(
            time_df['packet_len'].values[0:-1], 0, 0)) + \
            time_df['true_timestamp_sample_index']
        time_df = time_df[~pd.isna(time_df['true_timestamp_sample_index'])]  # pylint: disable=invalid-unary-operand-type
        time_df = time_df[~pd.isna(time_df['true_timestamp_millis'])]  # pylint: disable=invalid-unary-operand-type
        time_df['true_timestamp_global_index'] = time_df[
            'true_timestamp_global_index'].astype(int)
        if time_df['true_timestamp_millis'].dtype == np.float64:
            time_df['true_timestamp_ms'] = time_df['true_timestamp_millis']
        elif time_df['true_timestamp_millis'].dtype == np.int64:
            time_df['true_timestamp_ms'] = time_df[
                'true_timestamp_millis'].astype(float)
        elif time_df['true_timestamp_millis'].dtype == pd.Int64Dtype():
            time_df['true_timestamp_ms'] = time_df[
                'true_timestamp_millis'].astype(float)
        else:
            time_df['true_timestamp_ms'] = \
                time_df['true_timestamp_millis'].apply(
                    lambda x: x.timestamp() * 1000)
        time_df['dt'] = np.divide(
            np.diff(time_df['true_timestamp_ms'], prepend=[0]),
            np.diff(time_df['true_timestamp_global_index'], prepend=[0]))
        time_df['fs_est'] = np.divide(1000.0, time_df['dt'].values)
        self.time_df = time_df

    def _calc_nominal_fs(self):
        """Method to calculate nominal sampling rate. """
        self.fs_nom = self.df['sampling_rate'][0]

    def _calc_median_fs(self):
        """Method to calculate the median sampling rate.

        Raises a warning if the median sampling rate differs significantly
        from the nominal sampling rate.
        """
        fs_med = np.median(self.time_df['fs_est'])
        if abs((fs_med - self.fs_nom) / self.fs_nom) >= \
                _NOMINAL_FS_ERROR_THRESHOLD:
            self.median_fs_error_counter.inc()
            err_str = ('Stable median sampling rate could'
                       ' not be calculated. Window likely'
                       ' has a significant number of dropped'
                       f' packets. Data frame info: {self.df.attrs}')
            if not self.ignore_median_fs_error:
                raise ValueError(err_str)
            else:
                warnings.warn(err_str, UserWarning)
        # add valid flag for each esimated fs within the error tolerance
        self.time_df['fs_valid'] = abs(
            (self.time_df['fs_est'] - fs_med) / fs_med) <= self.error_thresh
        self.fs_med = fs_med

    def _calc_mean_fs(self):
        """Method to calculate mean sampling rate.

        The mean sampling rate is calculated only for packets that do
        not contain missing data. """
        fs_mean = np.mean(self.time_df[self.time_df['fs_valid']]['fs_est'])
        self.fs_mean = fs_mean

    def _unpack_first_packet(self):
        """ Method to unpack first packet (special case) """
        num_samples = self.true_timestamp_global_index[0]
        delta_t = 1000 / self.fs_mean
        t0 = self.true_timestamp_ms[0] - delta_t * num_samples
        timestamp_ms_segment = t0 + np.arange(num_samples) * delta_t
        # add data to upacked data
        for col in self.unpackable_cols:
            self.unpacked_data[col].extend(self.tmp_unpack[col][0:num_samples])
        # add time information
        self.unpacked_data['timestamp_ms_raw'].extend(timestamp_ms_segment)
        # For every timestamp copy over the additional columns that should be
        # kept.
        for _ in range(len(timestamp_ms_segment)):
            for col in self.additional_cols:
                self.unpacked_data[col].append(self.additional_col_data[col][0])

    def _unpack_last_packet(self):
        """ Method to unpack last packet (special case). """
        tm = self.true_timestamp_ms[-1]
        delta_t = 1000 / self.fs_mean
        data_index0 = self.true_timestamp_global_index[-1]
        total_samples = len(self.tmp_unpack[self.unpackable_cols[0]])
        num_samples = total_samples - data_index0
        timestamp_ms_segment = tm + np.arange(num_samples) * delta_t
        for col in self.unpackable_cols:
            self.unpacked_data[col].extend(self.tmp_unpack[col][data_index0:])
        # add time information
        self.unpacked_data['timestamp_ms_raw'].extend(timestamp_ms_segment)
        # For every timestamp copy over the additional columns that should be
        # kept.
        for _ in range(len(timestamp_ms_segment)):
            for col in self.additional_cols:
                self.unpacked_data[col].append(
                    self.additional_col_data[col][-1:])

    def _unpack_packet_interval(self, idx: int):
        """ Method of unpacking packet time interval. """
        # extract relevant parameters for time interval calculation
        t_cur = self.true_timestamp_ms[idx]
        t_prev = self.true_timestamp_ms[idx - 1]
        data_idx0 = self.true_timestamp_global_index[idx - 1]
        data_idx1 = self.true_timestamp_global_index[idx]
        if self.fs_valid[idx]:
            delta_t = 1000.0 / self.fs_est[idx]
            num_samples = data_idx1 - data_idx0
            timestamp_ms_segment = t_prev + np.arange(0, num_samples) * delta_t
            for col in self.unpackable_cols:
                self.unpacked_data[col].extend(
                    self.tmp_unpack[col][data_idx0:data_idx1])
        else:
            delta_t = 1000.0 / self.fs_med
            timestamp_ms_segment = np.arange(t_prev, t_cur, delta_t)
            for col in self.unpackable_cols:
                self.unpacked_data[col].extend(
                    len(timestamp_ms_segment) * [np.nan])
        # For every timestamp copy over the additional columns that should be
        # kept.
        for _ in range(len(timestamp_ms_segment)):
            for col in self.additional_cols:
                self.unpacked_data[col].append(
                    self.additional_col_data[col][idx])
        self.unpacked_data['timestamp_ms_raw'].extend(timestamp_ms_segment)

    def _unpack_data(self):
        """ Main method for unpacking data. """
        num_packet_intervals = len(self.time_df)
        for idx in range(num_packet_intervals + 1):
            if idx == 0:
                self._unpack_first_packet()
            elif idx == (num_packet_intervals):
                self._unpack_last_packet()
            else:
                self._unpack_packet_interval(idx)

    def _unpack_raw(self):
        """Method to unpack raw data packets. """
        for col in self.unpackable_cols:
            self.tmp_unpack[col] = np.hstack(self.df[col])

        for col in self.additional_cols:
            self.additional_col_data[col] = self.df[col].values

    def _interpolate_timestamps(self):
        """ Method to interpolate timestamps. """
        raw_timestamps = self.unpacked_data['timestamp_ms_raw']
        raw_indices = np.arange(len(raw_timestamps))
        data_mat = np.vstack([raw_indices, np.ones(len(raw_indices))]).T
        delta_t, t0 = np.linalg.lstsq(data_mat, raw_timestamps, rcond=None)[0]
        self.unpacked_data['timestamp_ms'] = t0 + \
            np.arange(0, len(raw_timestamps))*delta_t

    def _assign_last_dp_true_timestamp(self, row):
        """If true timestamp fields are empty, replace with the last sample.
        """
        row['true_timestamp_sample_index'] = row['packet_len'] - 1
        row['true_timestamp_millis'] = (
            row['measurement_timestamp_utc'] +
            pd.Timedelta(seconds=(row['true_timestamp_sample_index'] /
                                  row['sampling_rate'])))
        return row

    def _check_true_timestamps(self):
        """Method to check for true timestamps and assign if necessary."""
        num_true_timestamps = self.df['true_timestamp_millis'].notnull().sum()
        if num_true_timestamps == 0:
            # If true timestamps are missing, add this information
            self.df = self.df.apply(self._assign_last_dp_true_timestamp, axis=1)
            logging.info('True timestamps did not exist for data, '
                         'automatically assigned')

    def _check_sensor_id(self):
        """Method to check if only one sensor id is present."""
        num_sensor_ids = len(self.df['sensor_id'].unique())
        if num_sensor_ids > 1:
            raise ValueError('More than one sensor_id detected.')

    def _verify_packet_lengths(self, unpackable_cols: List[str]):
        drop_rows: List[int] = []
        for i, row in self.df.iterrows():
            column_lengths: Dict[str, int] = {
                col: len(row[col]) for col in unpackable_cols
            }
            length_set = set(column_lengths.values())
            max_len = max(length_set)
            min_len = min(length_set)

            if max_len == 0:
                drop_rows.append(i)
                continue
            if (len(length_set) > data_unpacking_legacy.MAX_LENGTH_OF_LENGTH_SET
                    or (min_len != max_len and min_len > 0)):
                #TODO: Should we raise an Exception or drop the packet with
                # a warning identifying the bad packet so that the rest of
                # the job can finish?
                raise ValueError(f'Unequal column lengths: {column_lengths}.'
                                 f'Row: {row}')

            for col, length in column_lengths.items():
                if length == 0:
                    row[col] = max_len * [np.nan]

        if len(drop_rows) > 0:
            self.df = self.df.drop(index=drop_rows).reset_index(drop=True)

    def unpack(self, raw_df: pd.DataFrame, unpackable_cols: List[str],
               additional_cols: List[str]):
        self.df = raw_df
        self.unpackable_cols = unpackable_cols
        self.additional_cols = additional_cols
        self.fs_med = 0.0
        self.fs_mean = 0.0
        # create temporary unpacked sensor data
        self.tmp_unpack: Dict[str, List[Any]] = {}
        self.unpacked_data: Dict[str, List[Any]] = {'timestamp_ms_raw': []}
        for col in unpackable_cols:
            self.unpacked_data[col] = []
            self.tmp_unpack[col] = []

        self.additional_col_data: Dict[str, List[Any]] = {}
        for col in additional_cols:
            self.unpacked_data[col] = []
            self.additional_col_data[col] = []

        # sort df
        self.df = self.df.sort_values(by='measurement_timestamp_utc')

        # add packet length to input data frame
        self.df['packet_len'] = self.df[unpackable_cols[0]].apply(len)
        self._verify_packet_lengths(unpackable_cols)
        if self.df.shape[0] == 0:
            return pd.DataFrame()

        # determine if appropriate fields have been populated
        self._check_true_timestamps()

        # check to see if only one sensor_id is present
        self._check_sensor_id()

        # unpack raw data
        self._unpack_raw()

        # calculate dataframe containing timing information
        self._calc_time_df()

        # extract nominal sampling rate
        self._calc_nominal_fs()

        # calculate median sampling rate
        try:
            self._calc_median_fs()
        except ValueError as e:
            if self.fallback_to_legacy:
                logging.warning('not enough data to use new unpacking, '
                                'falling back to legacy unpack method.')
                return data_unpacking_legacy.unpack_data_frame(
                    self.df, self.unpackable_cols, self.additional_cols)
            else:
                raise e

        if not self.time_df['fs_valid'].any():
            return pd.DataFrame()

        # calculate mean sampling rate
        self._calc_mean_fs()

        # extract numpy arrays from time_df (performance optimization)
        self.true_timestamp_global_index = self.time_df[
            'true_timestamp_global_index'].to_numpy()
        self.true_timestamp_ms = self.time_df['true_timestamp_ms'].to_numpy()
        self.fs_valid = self.time_df['fs_valid'].to_numpy()
        self.fs_est = self.time_df['fs_est'].to_numpy()
        # unpack data
        self._unpack_data()

        # interpolate timestamps
        self._interpolate_timestamps()

        unpacked_df = pd.DataFrame(self.unpacked_data)[[
            'timestamp_ms', *unpackable_cols, *additional_cols
        ]]

        # add attributes
        unpacked_df.attrs = self.df.attrs
        return unpacked_df


def unpack_data_frame(sensor_df: pd.DataFrame,
                      cols_to_unpack: List,
                      error_thresh: float = 0.05,
                      ignore_median_fs_error: bool = False,
                      fall_back_to_legacy: bool = False,
                      additional_cols_to_keep: Optional[List[str]] = None,
                      use_legacy: bool = False,
                      use_numba: bool = False,
                      drop_nan: bool = False) -> pd.DataFrame:
    """Function to unpack sensor data frame generated from DS SDK

    All input DataFrames are expected to be generated by the DS SDK.
    The output (unpacked) DataFrame will have the following columns:
    -timestamp_ms: evenly spaced (interpolated) timestamps for each
    sensor value
    -timestamp_ms_raw: timestamps for each sensor value based on local
    interpolation, which will vary between packets
    -user specified columns to unpack from the original dataframe

    Args:
        sensor_df: DataFrame of sensor data from DS SDK
        cols_to_unpack: columns of densely packed sensor data
        error_thresh: error threshold (relative error) from nominal
            sampling frequency to determine if a packet is dropped
        ignore_median_fs_error: flag to indicate if the error associated
            with a potentially unstable median sampling rate error should be
            ignored.
        fall_back_to_legacy: If true will fallback to using the legacy unpack
            function if there is not enough data to interpolate timestamps. If
            this is True than ignore_median_fs_error must be false.
        additional_cols_to_keep:
            Additional columns from the original data frame that should be
            kept on the new data frame. These are columns that don't need to
            be unpacked just copied over.
        use_legacy (bool): Indicate whether to use legacy unpacking instead of
            attempting to use the new unpacking algorithm.
        use_numba (bool): Indicate whether to use a compiled version of the
            unpacking algorithm.
        drop_nan (bool): Indicate whether to drop invalid (all NaN) rows. Only
            applicable when use_numba is True, otherwise it is ignored.


    Returns:
        DataFrame containing the unpacked sensor data and timestamps
    """
    if use_numba:
        unpacker: Union[data_unpacking_numba.DataUnpackerNumba,
                        DataUnpacker] = data_unpacking_numba.DataUnpackerNumba(
                            error_thresh=error_thresh,
                            ignore_median_fs_error=ignore_median_fs_error,
                            fallback_to_legacy=fall_back_to_legacy,
                            use_legacy=use_legacy,
                            drop_nan=drop_nan)
    elif use_legacy:
        return data_unpacking_legacy.unpack_data_frame(
            sensor_df=sensor_df,
            cols_to_unpack=cols_to_unpack,
            additional_cols=(additional_cols_to_keep or []))
    else:
        unpacker = DataUnpacker(error_thresh=error_thresh,
                                ignore_median_fs_error=ignore_median_fs_error,
                                fallback_to_legacy=fall_back_to_legacy)

    if additional_cols_to_keep is None:
        additional_cols_to_keep = []

    t_start = time.time()
    try:
        unpacked_df = unpacker.unpack(sensor_df, cols_to_unpack,
                                      additional_cols_to_keep)
    except Exception as e:
        logging.error('Unpacking failed for this dataframe: %s',
                      sensor_df.iloc[0]['data_point_metadata'])
        raise e
    t_end = time.time()
    logging.info('use_numba = %s: %d ms', str(use_numba),
                 int((t_end - t_start) * 1000))
    return unpacked_df


def _filter_to_sensor_id(data_point: DataPoint, sensor_id: str,
                         data_source_cache):
    data_source = data_source_cache[
        data_point.data_point_metadata.data_source_id]

    data_point_sensor_id: Any = getattr(data_point, 'sensor_id', None)

    if data_point_sensor_id is None:
        return data_source.sensor.id == sensor_id

    return (data_source.sensor.id == sensor_id or
            str(data_point_sensor_id) == sensor_id)


class PartitionCoppaDevice(beam.PartitionFn):

    def partition_for(  # type: ignore[override]
            self, element: DataPoint, num_partitions: int):
        del num_partitions
        return (0 if element.data_point_metadata.device_id.startswith('C2Q')
                else 1)


class _UnpackTransform(beam.PTransform):
    """Unpacks data returning one data point per unpacked data."""

    def __init__(
            self,
            *,
            # Defaults to one hour.
            cols_to_unpack: List[str],
            to_unpacked_fn: Callable,
            sensor_id: Optional[str] = None,
            data_source_cache: Optional[DataSourceCache] = None,
            time_window_seconds: int = 60 * 60,
            ignore_median_fs_error: bool = False,
            fall_back_to_legacy: bool = False,
            use_numba: bool = False,
            drop_nan: bool = False,
            cam2_use_legacy: bool = False) -> None:

        if sensor_id is not None and data_source_cache is None:
            raise ValueError('A DataSourceCache must be provided if sensor_id '
                             'is specified.')

        super().__init__()
        self._cols_to_unpack = cols_to_unpack
        self._to_unpacked_fn = to_unpacked_fn
        self._sensor_id = sensor_id
        self._data_source_cache = data_source_cache
        self._time_window_seconds = time_window_seconds
        self._ignore_median_fs_error = ignore_median_fs_error
        self._fall_back_to_legacy = fall_back_to_legacy
        self._use_numba = use_numba
        self._drop_nan = drop_nan
        self._cam2_use_legacy = cam2_use_legacy

    def expand_coppa(
        self, input_or_inputs: beam.PCollection[DataPointType]
    ) -> beam.PCollection[DataPointType]:

        return (input_or_inputs |
                ('Group Coppa by participant, device, hour' >> BuildDataPointDataFrames.PerParticipantDeviceWindow(
                     beam_window_fn=beam.transforms.window.FixedWindows(
                         self._time_window_seconds),
                     combine_method=None)) | 'Unpack Coppa device DataFrames' >>
                beam.Map(unpack_data_frame,
                         cols_to_unpack=self._cols_to_unpack,
                         additional_cols_to_keep=['data_point_metadata'],
                         use_legacy=True,
                         use_numba=self._use_numba) |
                'Flatten unpacked Coppa devices to single pcol' >> beam.FlatMap(
                    self._to_unpacked_fn))

    def expand(
        self, input_or_inputs: beam.PCollection[DataPointType]
    ) -> beam.PCollection[DataPointType]:

        if self._sensor_id is not None:
            input_or_inputs = (input_or_inputs |
                               'Filter to sensor_id' >> beam.Filter(
                                   _filter_to_sensor_id,
                                   self._sensor_id,
                                   self._data_source_cache,
                               ))

        other_device_pcol: beam.PCollection[DataPointType]
        coppa_device_pcol: beam.PCollection[DataPointType]
        if self._cam2_use_legacy:
            coppa_device_pcol, other_device_pcol = (
                input_or_inputs | 'Partition for is (not) Coppa device' >>
                beam.Partition(PartitionCoppaDevice(), 2))
        else:
            other_device_pcol = input_or_inputs

        unpacked_other = (
            other_device_pcol | 'Group by participant, device, hour' >>
            BuildDataPointDataFrames.PerParticipantDeviceWindow(
                beam_window_fn=beam.transforms.window.FixedWindows(
                    self._time_window_seconds),
                combine_method=None) |
            beam.Map(unpack_data_frame,
                     cols_to_unpack=self._cols_to_unpack,
                     additional_cols_to_keep=['data_point_metadata'],
                     ignore_median_fs_error=self._ignore_median_fs_error,
                     fall_back_to_legacy=self._fall_back_to_legacy,
                     use_numba=self._use_numba,
                     drop_nan=self._drop_nan) |
            beam.FlatMap(self._to_unpacked_fn))

        if self._cam2_use_legacy:
            unpacked_coppa = self.expand_coppa(coppa_device_pcol)
            return ((unpacked_coppa, unpacked_other) |
                    'Merge unpacked DataPoint collections' >> beam.Flatten())

        return unpacked_other


@dataclass
class UnpackedImu(DataPoint):
    acceleration_x: float
    acceleration_y: float
    acceleration_z: float
    gyro_x: Optional[float] = None
    gyro_y: Optional[float] = None
    gyro_z: Optional[float] = None


@dataclass
class UnpackedPpg(DataPoint):
    green: float


@dataclass
class UnpackedTwoChannelPpg(DataPoint):
    green: float
    green_2: float


@dataclass
class UnpackedEda(DataPoint):
    raw_adc: float


@dataclass
class UnpackedPicardEda(DataPoint):
    real_adc: float
    im_adc: float
    z2_adc: float


@dataclass
class UnpackedEcg(DataPoint):
    raw_adc: int


def _to_unpacked_imu(df: pd.DataFrame) -> Iterable[UnpackedImu]:
    unpacked_imu = []
    imu_rows = df.to_dict('records')
    for row in imu_rows:
        gyro_x = None
        gyro_y = None
        gyro_z = None
        if 'gyro_x' in row:
            gyro_x = row['gyro_x']
        if 'gyro_y' in row:
            gyro_y = row['gyro_y']
        if 'gyro_z' in row:
            gyro_z = row['gyro_z']

        unpacked_imu.append(
            UnpackedImu(
                data_point_metadata=(
                    data_point_metadata_for_derived_data_from_df(
                        df)),
                measurement_timestamp_utc=Timestamp.of(row['timestamp_ms'] /
                                                       1000),
                acceleration_x=row['acceleration_x'],
                acceleration_y=row['acceleration_y'],
                acceleration_z=row['acceleration_z'],
                gyro_x=gyro_x,
                gyro_y=gyro_y,
                gyro_z=gyro_z,
            ))
    return unpacked_imu


def _to_unpacked_ppg(df: pd.DataFrame) -> Iterable[UnpackedPpg]:
    unpacked_ppg = []
    ppg_rows = df.to_dict('records')
    for row in ppg_rows:
        unpacked_ppg.append(
            UnpackedPpg(
                data_point_metadata=(
                    data_point_metadata_for_derived_data_from_df(
                        df)),
                measurement_timestamp_utc=Timestamp.of(row['timestamp_ms'] /
                                                       1000),
                green=row['green'],
            ))
    return unpacked_ppg


def _to_unpacked_two_channel_ppg(df: pd.DataFrame) -> Iterable[
    UnpackedTwoChannelPpg]:
    unpacked_two_channel_ppg = []
    two_channel_ppg_rows = df.to_dict('records')
    for row in two_channel_ppg_rows:
        unpacked_two_channel_ppg.append(
            UnpackedTwoChannelPpg(
                data_point_metadata=(
                    data_point_metadata_for_derived_data_from_df(
                        df)),
                measurement_timestamp_utc=Timestamp.of(row['timestamp_ms'] /
                                                       1000),
                green=row['green'],
                green_2=row['green_2']
            ))
    return unpacked_two_channel_ppg


def _to_unpacked_eda(df: pd.DataFrame) -> Iterable[UnpackedEda]:
    unpacked_eda = []
    eda_rows = df.to_dict('records')
    for row in eda_rows:
        unpacked_eda.append(
            UnpackedEda(
                data_point_metadata=(
                    data_point_metadata_for_derived_data_from_df(
                        df)),
                measurement_timestamp_utc=Timestamp.of(row['timestamp_ms'] /
                                                       1000),
                raw_adc=row['raw_adc'],
            ))
    return unpacked_eda


def _to_unpacked_picard_eda(df: pd.DataFrame) -> Iterable[UnpackedPicardEda]:
    unpacked_picard_eda = []
    picard_eda_rows = df.to_dict('records')
    for row in picard_eda_rows:
        unpacked_picard_eda.append(
            UnpackedPicardEda(
                data_point_metadata=(
                    data_point_metadata_for_derived_data_from_df(
                        df)),
                measurement_timestamp_utc=Timestamp.of(row['timestamp_ms'] /
                                                       1000),
                real_adc=row['real_adc'],
                im_adc=row['im_adc'],
                z2_adc=row['z2_adc'],
            ))
    return unpacked_picard_eda


def _to_unpacked_ecg(df: pd.DataFrame) -> Iterable[UnpackedEcg]:
    unpacked_ecg = []
    ecg_rows = df.to_dict('records')
    for row in ecg_rows:
        unpacked_ecg.append(
            UnpackedEcg(  # pytype: disable=wrong-keyword-args # pylint: disable=unexpected-keyword-arg,line-too-long
                data_point_metadata=(
                    data_point_metadata_for_derived_data_from_df(
                        df)),
                measurement_timestamp_utc=(Timestamp.of(row['timestamp_ms'] /
                                                        1000)),
                raw_adc=int(row['raw_adc']),
            ))
    return unpacked_ecg


class UnpackImu(_UnpackTransform):
    """Unpacked IMU data returning one data point per unpacked data.

    Only one sensor ID can be unpacked at a time. To unpack multiple sensor IDs
    multiple transforms are needed.
    """

    def __init__(self, sensor_id: str,
                 data_source_cache: DataSourceCache, **kwargs) -> None:

        cols_to_unpack = [
            'acceleration_x',
            'acceleration_y',
            'acceleration_z',
        ]
        if sensor_id in ['0', '1']:
            cols_to_unpack.extend([
                'gyro_x',
                'gyro_y',
                'gyro_z',
            ])
        super().__init__(cols_to_unpack=cols_to_unpack,
                         to_unpacked_fn=_to_unpacked_imu,
                         sensor_id=sensor_id,
                         data_source_cache=data_source_cache,
                         **kwargs)


class UnpackPpg(_UnpackTransform):
    """Unpacks PPG data returning one data point per unpacked data."""

    def __init__(
        self,
        **kwargs,
    ) -> None:
        super().__init__(cols_to_unpack=['green'],
                         to_unpacked_fn=_to_unpacked_ppg,
                         **kwargs)


class UnpackTwoChannelPpg(_UnpackTransform):
    """Unpacks TwoChannelPPG data returning 1 data point/unpacked data."""

    def __init__(
        self,
        **kwargs,
    ) -> None:
        super().__init__(cols_to_unpack=['green', 'green_2'],
                         to_unpacked_fn=_to_unpacked_two_channel_ppg,
                         **kwargs)


class UnpackEda(_UnpackTransform):
    """Unpacks EDA data returning one data point per unpacked data."""

    def __init__(self, sensor_id: str = None,
                 data_source_cache: DataSourceCache = None,
                 **kwargs) -> None:
        kwargs['cam2_use_legacy'] = True
        super().__init__(cols_to_unpack=['raw_adc'],
                         to_unpacked_fn=_to_unpacked_eda,
                         sensor_id=sensor_id,
                         data_source_cache=data_source_cache,
                         **kwargs)

class UnpackPicardEda(_UnpackTransform):
    """Unpacks Picard EDA data returning 1 point per unpacked data."""

    def __init__(self, **kwargs) -> None:
        super().__init__(cols_to_unpack=[
            'real_adc',
            'im_adc',
            'z2_adc'
            ],
                         to_unpacked_fn=_to_unpacked_picard_eda,
                         **kwargs)


class UnpackEcg(_UnpackTransform):
    """Unpacks EDA data returning one data point per unpacked data."""

    def __init__(self, **kwargs) -> None:
        super().__init__(cols_to_unpack=['raw_adc'],
                         to_unpacked_fn=_to_unpacked_ecg,
                         **kwargs)
