"""Module for unpacking sensor data from DS SDK.
go/dssdk_sensor_data_unpacking
"""

import logging
from typing import Any, Dict, List, Optional, Tuple
import warnings

import apache_beam as beam
import numba
import numpy as np
import pandas as pd

# threshold for error from nominal sampling rate
_NOMINAL_FS_ERROR_THRESHOLD = 0.20

# numba time column indices
true_timestamp_ms_idx: int = 0
true_timestamp_sample_index_idx: int = 1
packet_len_idx: int = 2
true_timestamp_global_index_idx: int = 3
num_packet_samples_idx: int = 4
fs_est_idx: int = 5
fs_valid_idx: int = 6

legacy_sampling_rate_idx: int = 1
global_sample_index_idx: int = 3

# unpacked_data column indices
timestamp_ms_idx: int = 0
packet_num_idx: int = 1
unpackable_col_idx: int = 2

calc_time_sig = 'float64[:,::1](float64[:,::1])'
total_samples_sig = 'int64(float64[:,::1],int64,float64)'
unpack_sig = 'int64(float64[:,::1],float64[:,::1],float64[:,::1],float64,float64,int64[::1],boolean)'  # pylint: disable=line-too-long
unpack_legacy_sig = 'int64(float64[:,::1],float64[:,::1],float64[:,::1])'


class NumPyDataFrame:
    data: np.ndarray
    columns: List[str]
    dates: np.ndarray
    additional_df: Optional[pd.DataFrame]
    attrs: Dict[str, Any]


@numba.njit(calc_time_sig, cache=True, fastmath=True)
def _calc_time_array_numba(time_array: np.ndarray) -> np.ndarray:
    """ Performs a subset of calculations for needed metadata for each packet

    Args:
        time_array (np.ndarray): Pre-alocated array of float64 objects the
            array is expected to have a shape like (7, N), where N is the
            number of packets before unpacking. There are 7 rows of packet-
            specific metadata needed for the unpacking algorithm that are either
            given by the source DataPoints or calculated in this function or
            elsewhere

    Returns:
        np.ndarray: The given time_array with the true_timestamp_global_index,
            num_packet_samples, and fs_est calculated and populated for each
            packet.
    """

    time_array[true_timestamp_global_index_idx,
               1:] = np.cumsum(time_array[packet_len_idx, :-1])
    time_array[true_timestamp_global_index_idx] += (
        time_array[true_timestamp_sample_index_idx, :])

    time_array[
        num_packet_samples_idx,
        1:] = time_array[true_timestamp_global_index_idx,
                         1:] - time_array[true_timestamp_global_index_idx, 0:-1]

    time_array[fs_est_idx, 1:] = 1000.0 * np.divide(
        np.diff(time_array[true_timestamp_global_index_idx]),
        np.diff(time_array[true_timestamp_ms_idx]))
    time_array[fs_est_idx,
               0] = (1000 * time_array[true_timestamp_global_index_idx, 0] /
                     time_array[true_timestamp_ms_idx, 0])

    return time_array


@numba.njit(unpack_sig, cache=True, fastmath=True)
def _unpack_data_numba(unpacked_data: np.ndarray, time_array: np.ndarray,
                       tmp_unpack: np.ndarray, fs_med: float, fs_mean: float,
                       unpacked_indexes: np.ndarray, drop_nan: bool) -> int:
    """ Applies the unpacking algorithm a group of DataPoint packets

    Args:
        unpacked_data (np.ndarray): Pre-alocated empty array of float64 np.nan
            objects that will be populated by the unpacking algorithm. The shape
            of the array is (2 + num_columns_to_unpack, M), where M is an over-
            estimate of the number of unpacked DataPoints
        time_array (np.ndarray): Calculated array that provides packet metadata
            such as the measurement timestamp, packet size, and sampling rate
        tmp_unpack (np.ndarray): Array of unpacked measurements formed by
            naively stacking all packet measurements in order of measurement
            timestamp
        fs_med (float): Median sampling frequency for the group of DataPoints
        fs_mean (float): Average sampling frequency for the group of DataPoints
        unpacked_indexes (np.ndarray): Pre-alocated empty array of int64 objects
            that is populated with the actual indexes of the unpacked
            measurements. These indexes take into account any possible invalid
            or missing DataPoint packets that may occur between valid packets.
        drop_nan (bool): Indicate whether to drop invalid (all NaN) rows.

    Returns:
        int: The index after the last unpacked DataPoint
    """

    num_packets = time_array.shape[1]
    num_total_samples = tmp_unpack.shape[1]
    current_idx = int(0)
    unpacked_idx = int(0)

    def _unpack_first_packet_numba() -> Tuple[int, int]:
        """ Method to unpack first packet (special case) """
        num_samples = int(time_array[true_timestamp_global_index_idx, 0])
        delta_t = 1000 / fs_mean
        t0 = time_array[true_timestamp_ms_idx, 0] - delta_t * num_samples

        for i in range(num_samples):
            unpacked_data[timestamp_ms_idx, i] = t0 + i * delta_t
            unpacked_data[packet_num_idx, i] = 0.0
            unpacked_data[unpackable_col_idx:, i] = tmp_unpack[:, i]
            unpacked_indexes[i] = i

        return num_samples, num_samples

    def _unpack_last_packet_numba() -> Tuple[int, int]:
        """ Method to unpack last packet (special case). """
        idx = num_packets - 1
        tm = time_array[true_timestamp_ms_idx, idx]
        delta_t = 1000 / fs_mean
        data_idx = int(time_array[true_timestamp_global_index_idx, idx])
        num_samples = num_total_samples - data_idx

        for i in range(num_samples):
            unpacked_data[timestamp_ms_idx, current_idx + i] = tm + i * delta_t
            unpacked_data[packet_num_idx, current_idx + i] = float(idx)
            unpacked_data[unpackable_col_idx:,
                          current_idx + i] = tmp_unpack[:, data_idx + i]
            unpacked_indexes[current_idx + i] = unpacked_idx + i

        return current_idx + num_samples, unpacked_idx + num_samples

    def _unpack_packet_interval_numba(idx: int) -> Tuple[int, int]:
        """ Method of unpacking packet time interval. """
        # extract relevant parameters for time interval calculation
        t_cur = time_array[true_timestamp_ms_idx, idx]
        t_prev = time_array[true_timestamp_ms_idx, idx - 1]
        data_idx0 = int(time_array[true_timestamp_global_index_idx, idx - 1])

        if time_array[fs_valid_idx, idx] == 1.0:
            delta_t = 1000.0 / time_array[fs_est_idx, idx]
            num_samples = int(time_array[num_packet_samples_idx, idx])
            for i in range(num_samples):
                unpacked_data[timestamp_ms_idx,
                              current_idx + i] = t_prev + i * delta_t
                unpacked_data[packet_num_idx, current_idx + i] = float(idx)
                unpacked_data[unpackable_col_idx:,
                              current_idx + i] = tmp_unpack[:, data_idx0 + i]
                unpacked_indexes[current_idx + i] = unpacked_idx + i

            return current_idx + num_samples, unpacked_idx + num_samples

        else:
            if drop_nan:
                num_samples = int((t_cur - t_prev) * fs_med / 1000.0)
                return current_idx, unpacked_idx + num_samples
            else:
                delta_t = 1000.0 / fs_med
                timestamp_ms_segment = np.arange(t_prev, t_cur, delta_t)
                num_samples = len(timestamp_ms_segment)
                for i in range(num_samples):
                    unpacked_data[timestamp_ms_idx,
                                  current_idx + i] = timestamp_ms_segment[i]
                    unpacked_indexes[current_idx + i] = unpacked_idx + i
                return current_idx + num_samples, unpacked_idx + num_samples

    def _interpolate_timestamps():
        """ Method to interpolate timestamps. """
        raw_timestamps: np.ndarray = unpacked_data[0, :current_idx]
        raw_indexes = unpacked_indexes[:current_idx]
        data_mat = np.vstack(
            (raw_indexes.reshape(1, -1), np.ones((1, int(current_idx))))).T
        delta_t, t0 = np.linalg.lstsq(data_mat, raw_timestamps.T)[0]
        unpacked_data[0,:current_idx] = t0 + \
            raw_indexes*delta_t

    current_idx, unpacked_idx = _unpack_first_packet_numba()

    for idx in np.arange(1, num_packets):
        current_idx, unpacked_idx = _unpack_packet_interval_numba(idx)

    current_idx, unpacked_idx = _unpack_last_packet_numba()

    _interpolate_timestamps()
    return current_idx


@numba.njit(calc_time_sig, cache=True, fastmath=True)
def _calc_time_array_legacy_numba(time_array: np.ndarray):
    """ Performs a subset of calculations for needed metadata for each packet

    Args:
        time_array (np.ndarray): Pre-alocated array of float64 objects the
            array is expected to have a shape like (4, N), where N is the
            number of packets before unpacking. There are 4 rows of packet-
            specific metadata needed for the unpacking algorithm that are either
            given by the source DataPoints or calculated in this function or
            elsewhere

    Returns:
        np.ndarray: The given time_array with the global_sample_index and
            packet_len populated
    """

    time_array[global_sample_index_idx,
               1:] = np.cumsum(time_array[packet_len_idx, :-1])
    for i in range(time_array.shape[1]):
        dt = 1000. / time_array[legacy_sampling_rate_idx, i]
        t = time_array[timestamp_ms_idx,
                       i] + np.arange(time_array[packet_len_idx, i]) * dt
        if i + 1 < time_array.shape[1]:
            time_array[packet_len_idx,
                       i] = t[t < time_array[timestamp_ms_idx, i + 1]].shape[0]
    return time_array


@numba.njit(unpack_legacy_sig, cache=True, fastmath=True)
def _unpack_data_legacy_numba(unpacked_data: np.ndarray, time_array: np.ndarray,
                              tmp_unpack: np.ndarray) -> int:
    """ Applies the legacy unpacking algorithm a group of DataPoint packets

    Args:
        unpacked_data (np.ndarray): Pre-alocated empty array of float64 np.nan
            objects that will be populated by the unpacking algorithm. The shape
            of the array is (2 + num_columns_to_unpack, M), where M is an over-
            estimate of the number of unpacked DataPoints
        time_array (np.ndarray): Calculated array that provides packet metadata
            such as the measurement timestamp, packet size, and sampling rate
        tmp_unpack (np.ndarray): Array of unpacked measurements formed by
            naively stacking all packet measurements in order of measurement
            timestamp

    Returns:
        int: The index after the last unpacked DataPoint
    """
    current_idx = 0
    for i in range(time_array.shape[1]):
        dt = 1000. / time_array[legacy_sampling_rate_idx, i]
        t = time_array[timestamp_ms_idx,
                       i] + np.arange(time_array[packet_len_idx, i]) * dt

        packet_len = int(time_array[packet_len_idx, i])
        sample_index = int(time_array[global_sample_index_idx, i])

        if packet_len > 0:
            unpacked_data[timestamp_ms_idx,
                          current_idx:(current_idx +
                                       packet_len)] = t[:packet_len]
            unpacked_data[packet_num_idx,
                          current_idx:(current_idx + packet_len)] = float(i)
            unpacked_data[unpackable_col_idx:, current_idx:(
                current_idx +
                packet_len)] = tmp_unpack[:, sample_index:(sample_index +
                                                           packet_len)]
        current_idx += packet_len

    return current_idx


class DataUnpackerNumba(object):
    """Class to unpack data from DS SDK."""

    def __init__(self,
                 *,
                 error_thresh: float = 0.05,
                 ignore_median_fs_error: bool = False,
                 fallback_to_legacy: bool = False,
                 use_legacy: bool = False,
                 drop_nan: bool = False):
        self.error_thresh = error_thresh
        self.ignore_median_fs_error = ignore_median_fs_error
        self.fallback_to_legacy = fallback_to_legacy
        self.median_fs_error_counter = beam.metrics.Metrics.counter(
            'data_unpacking', 'median_fs_error')

        if fallback_to_legacy and ignore_median_fs_error:
            raise ValueError(
                'Only one of fallback_to_legacy or ignore_median_fs_error '
                'should be True')

        self.use_legacy = use_legacy
        self._drop_nan = drop_nan

    def _calc_time_array(self) -> np.ndarray:
        """Method to create a dataframe with intermediate timing information.

        In order to simplify the logic and to increase the speed of calculating
        the interpacket sampling rates, a global index for each true timestamp
        is calculated based on the sum of the packet lengths of the preceding
        packet and current true_timestamp_sample_index.
        """

        time_df_temp = self.df[[
            'true_timestamp_millis', 'true_timestamp_sample_index', 'packet_len'
        ]].dropna(how='any')

        time_df_temp['true_timestamp_ms'] = time_df_temp[
            'true_timestamp_millis'].apply(lambda x: x.timestamp() * 1000)

        return _calc_time_array_numba(
            np.ascontiguousarray(
                np.hstack((time_df_temp[[
                    'true_timestamp_ms', 'true_timestamp_sample_index',
                    'packet_len'
                ]].to_numpy().astype(float),
                           np.zeros((time_df_temp.shape[0], 4),
                                    dtype='float64'))).T))

    def _calc_time_array_legacy(self) -> np.ndarray:
        """Method to create a dataframe with intermediate timing information.

        In order to simplify the logic and to increase the speed of calculating
        the interpacket sampling rates, a global index for each true timestamp
        is calculated based on the sum of the packet lengths of the preceding
        packet and current true_timestamp_sample_index.
        """

        time_df_temp = self.df[[
            'measurement_timestamp_utc', 'sampling_rate', 'packet_len'
        ]].dropna(how='any')

        time_df_temp['measurement_timestamp_utc'] = time_df_temp[
            'measurement_timestamp_utc'].apply(lambda x: x.timestamp() * 1000)

        return _calc_time_array_legacy_numba(
            np.ascontiguousarray(
                np.vstack((time_df_temp.to_numpy().T,
                           np.zeros((1, self.df.shape[0]), dtype='float64')))))

    def _calc_median_fs(self) -> float:
        """Method to calculate the median sampling rate.

        Raises a warning if the median sampling rate differs significantly
        from the nominal sampling rate.
        """
        fs_med = np.median(self.time_array[fs_est_idx])
        if abs((fs_med - self.fs_nom) / self.fs_nom) >= \
                _NOMINAL_FS_ERROR_THRESHOLD:
            self.median_fs_error_counter.inc()
            err_str = ('Stable median sampling rate could'
                       ' not be calculated. Window likely'
                       ' has a significant number of dropped'
                       ' packets. Data frame info: '
                       f'{self.df["measurement_timestamp_utc"].min()}'
                       f' - {self.df["measurement_timestamp_utc"].max()}')
            if not self.ignore_median_fs_error:
                raise ValueError(err_str)
            else:
                warnings.warn(err_str, UserWarning)
        # add valid flag for each esimated fs within the error tolerance
        self.time_array[fs_valid_idx] = abs(
            (self.time_array[fs_est_idx] - fs_med) /
            fs_med) <= self.error_thresh
        return fs_med

    def _calc_mean_fs(self):
        """Method to calculate mean sampling rate.

        The mean sampling rate is calculated only for packets that do
        not contain missing data. """
        fs_mean = np.mean(
            self.time_array[fs_est_idx][self.time_array[fs_valid_idx] == 1])
        if pd.isna(fs_mean):
            raise ValueError('No valid fs_est exists.')
        return fs_mean

    def _unpack_raw(self) -> np.ndarray:
        """Method to unpack raw data packets. """
        return np.ascontiguousarray(np.vstack(
            [np.hstack(self.df[col]) for col in self.unpackable_cols]),
                                    dtype='float64')

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

    def _overestimate_num_unpacked_samples(self) -> int:
        """Estimates the final number of unpacked DataPoints

        During unpacking, the algorithm determines which packets have a 'valid'
        number of samples. Each packet must have an estimated sampling rate
        that is no more than 5% different from fs_med. Otherwise, it is
        marked invalid (see _calc_median_fs)

        - If it is 'invalid', it will be replaced with exactly NaN values such
        that the gap has enough samples to match fs_med or it is dropped
        - If it is 'valid', it will be assigned a number of samples that is
        associated with that packet in the `num_packet_samples` row of the
        self.time_array.
        - For the first and last packets, the number of samples is calculated
        based on the calculated true_timestamp_global_index

        Therefore, the max num_samples per row (whether valid or not) would be
        the maximum between the calculated number of samples in the first and
        last packets and the max `num_packet_samples`.
        """

        first_packet_samples = int(
            self.time_array[true_timestamp_global_index_idx, 0])

        last_idx = self.df.shape[0] - 1
        last_data_idx = int(self.time_array[true_timestamp_global_index_idx,
                                            last_idx])
        total_samples = self.tmp_unpack.shape[1]
        last_packet_samples = total_samples - last_data_idx

        max_packet_samples = self.time_array[num_packet_samples_idx].max()

        max_packet_samples = max(first_packet_samples, last_packet_samples,
                                 max_packet_samples)

        if self._drop_nan:
            return int(max_packet_samples * (self.df.shape[0]))
        else:
            n_seconds = (self.time_array[true_timestamp_ms_idx][-1] -
                         self.time_array[true_timestamp_ms_idx][0]) / 1000 + 1
            return int(max_packet_samples * n_seconds)

    def unpack(self, raw_df: pd.DataFrame, unpackable_cols: List[str],
               additional_cols: List[str]):
        self.df = raw_df
        self.unpackable_cols = unpackable_cols

        self.df = self.df.sort_values(by='measurement_timestamp_utc')
        self.df['packet_len'] = self.df[unpackable_cols[0]].apply(len)
        self._check_true_timestamps()
        self._check_sensor_id()
        self.tmp_unpack: np.ndarray = self._unpack_raw()
        self.time_array: np.ndarray = self._calc_time_array()
        self.fs_nom = self.df['sampling_rate'][0]

        # calculate median sampling rate
        use_legacy = self.use_legacy
        if not use_legacy:
            try:
                self.fs_med: float = self._calc_median_fs()
                # calculate mean sampling rate
            except ValueError as e:
                if self.fallback_to_legacy:
                    logging.warning('not enough data to use new unpacking, '
                                    'falling back to legacy unpack method.')
                    use_legacy = True
                else:
                    raise e

        unpacked_columns = ('timestamp_ms', 'packet_num', *unpackable_cols)
        unpacked_data: np.ndarray = np.array([], dtype='float64')

        if use_legacy:
            time_array = self._calc_time_array_legacy()
            ## Calculate the total number of samples (i.e., DataFrame rows)
            ## after unpacking so that we can pre-allocate the final array
            prealocated_size = int(np.sum(self.time_array[packet_len_idx]))
            unpacked_data = np.full((len(unpacked_columns), prealocated_size),
                                    np.nan,
                                    dtype='float64')

            last_index: int = _unpack_data_legacy_numba(unpacked_data,
                                                        time_array,
                                                        self.tmp_unpack)
            unpacked_data = unpacked_data[:, :last_index]

        else:
            try:
                self.fs_mean: float = self._calc_mean_fs()
            except ValueError:
                return pd.DataFrame()

            prealocated_size = self._overestimate_num_unpacked_samples()
            unpacked_data = np.full((len(unpacked_columns), prealocated_size),
                                    np.nan,
                                    dtype='float64')
            unpacked_indexes = np.ones((prealocated_size,), dtype='int64')

            last_index = _unpack_data_numba(unpacked_data, self.time_array,
                                            self.tmp_unpack, self.fs_med,
                                            self.fs_mean, unpacked_indexes,
                                            self._drop_nan)
            unpacked_data = unpacked_data[:, :last_index]

        unpacked_df = pd.DataFrame(unpacked_data.T, columns=unpacked_columns)

        if len(additional_cols) != 0:
            unpacked_df.set_index('packet_num', drop=True, inplace=True)
            additional_df = self.df[additional_cols]
            unpacked_df = unpacked_df.join(additional_df,
                                           how='left').reset_index(drop=True)
        else:
            unpacked_df.drop(columns=['packet_num'], inplace=True)

        unpacked_df.attrs = self.df.attrs

        return unpacked_df
