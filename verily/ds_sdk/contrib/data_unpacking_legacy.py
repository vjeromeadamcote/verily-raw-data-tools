"""Module for implementing legacy unpacking functions for DS SDK.

The unpacking behavior in this function should be equivalent to
    sdk.transforms.unpack_data_frame(
      df, overlap_behavior='combine').
"""
from typing import Any, Dict, List, Optional

import numpy as np
import pandas as pd

# The length set contains the lenghts of the individual columns of a row.
# The maximum acceptable legnth is 2 as long as one of the values is 0.
# If there are more than two length values, then there is a mismatch
MAX_LENGTH_OF_LENGTH_SET: int = 2


def unpack_data_frame(
    sensor_df: pd.DataFrame,
    cols_to_unpack: List,
    additional_cols: Optional[List[str]] = None,
    drop_overlapping_samples: bool = True,
) -> pd.DataFrame:
    """Function to unpack sensor data frame generated from DS SDK.

    The unpacking behavior in this function should be equivalent to
      sdk.transforms.unpack_data_packets(
        data_points, overlap_behavior='combine', clean_output=True).

    However, the output will be returned as a dataframe.

    Args:
      sensor_df: DataFrame of sensor data from DS SDK. The following
        columns are required:
          -measurement_timestamp_utc
          -sampling_rate
      cols_to_unpack: columns of densely packed sensor data
      additional_cols: columns to keep in the dataframe but not to unpack.
      drop_overlapping_samples: Whether to drop samples when they overlap
        across packets.

    Returns:
      DataFrame containing the unpacked sensor data and timestamps.
    """
    if additional_cols is None:
        additional_cols = []
    # sort by timestamps
    sensor_df = sensor_df.sort_values(
        by='measurement_timestamp_utc').reset_index(drop=True)
    timestamps: np.ndarray = np.array([])
    unpacked_data: Dict[Any, Any] = dict(
        zip(cols_to_unpack,
            len(cols_to_unpack) * [np.array([])]))
    sampling_rate_hz = np.nan
    for i, row in sensor_df.iterrows():
        column_lengths: Dict[str, int] = {
            col: len(row[col]) for col in cols_to_unpack
        }
        length_set = set(column_lengths.values())
        max_len = max(length_set)
        min_len = min(length_set)

        if max_len == 0:
            continue
        if len(length_set) > MAX_LENGTH_OF_LENGTH_SET or (min_len != max_len and
                                                          min_len > 0):
            #TODO: Should we raise an Exception or drop the packet with
            # a warning identifying the bad packet so that the rest of
            # the job can finish?
            raise ValueError(f'Unequal column lengths: {column_lengths}.'
                             f'Row: {row}')

        for col, length in column_lengths.items():
            if length == 0:
                row[col] = row[col] = max_len * [np.nan]

        packet_len = max_len

        sampling_rate_hz = row['sampling_rate']
        t = np.arange(packet_len) * 1000 / sampling_rate_hz
        t += row['measurement_timestamp_utc'].timestamp() * 1000
        cur_sensor_packet = {}
        for col in cols_to_unpack:
            cur_sensor_packet[col] = np.array(row[col])
        if (i + 1) < sensor_df.shape[0]:
            t_next = sensor_df.iloc[i + 1]['measurement_timestamp_utc']
            t_next = t_next.timestamp() * 1000
            if (t[-1] >= t_next) and drop_overlapping_samples:
                t_mask = t < t_next
                t = t[t_mask]
                # apply mask to correct sensor packet
                for col in cols_to_unpack:
                    cur_sensor_packet[col] = np.array(row[col])[t_mask]
        if t.size > 0:
            timestamps = np.concatenate((timestamps, np.array(t)))
            # concatenate with current sensor data packet
            for col in cols_to_unpack:
                unpacked_data[col] = np.concatenate(
                    [unpacked_data[col], cur_sensor_packet[col]])
        # add timestamps and sampling rate to unpacked data
        unpacked_data['timestamp_ms'] = timestamps
        unpacked_data['sampling_rate'] = np.array(
            len(timestamps) * [sampling_rate_hz])
        for col in additional_cols:
            unpacked_data[col] = [row[col]] * len(timestamps)
    unpacked_df = pd.DataFrame(unpacked_data)
    unpacked_df.attrs = sensor_df.attrs
    return unpacked_df
