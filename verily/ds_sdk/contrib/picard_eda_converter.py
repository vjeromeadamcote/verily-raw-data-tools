"""Module to convert data:
    from EDA dataspec (com.verily.eda)
    https://github.com/verily-src/sensorsuite-ds-sdk/blob/master/verily/ds_sdk/core/schemas/gen/eda.py
    to
    PicardEda : com.verily.picard.eda format:
    https://github.com/verily-src/sensorsuite-ds-sdk/blob/master/verily/ds_sdk/core/schemas/gen/picard_eda.py

    Original core algorithm (_adc_list_to_picard_eda_series)
     development and corresponding example by junjiading@.
"""

from typing import Any, Dict, Iterable, List

import apache_beam as beam
import pandas as pd

from verily import ds_sdk
from verily.ds_sdk.core import schemas


class PicardEDAConverter:
    """Converts a window of EDA data type (com.verily.eda) to the PicardEda
     format (com.verily.picard.eda)"""

    def __call__(self, eda_df: pd.DataFrame) -> pd.DataFrame:
        """Converts an EDA dataframe realized by DSSDK to PicardEda format."""
        picard_eda_df = pd.DataFrame(schemas.PicardEda.__dict__)
        eda_rows = eda_df.to_dict("records")
        for row in eda_rows:
            picard_eda_df = picard_eda_df.append(
                self.eda_row_to_picard_eda_row(row), ignore_index=True)
        picard_eda_df.attrs = eda_df.attrs
        return picard_eda_df

    def eda_row_to_picard_eda_row(self, eda_row: Dict[str,
                                                      Any]) -> Dict[str, Any]:
        """Converts a row of EDA (com.verily.eda) format to
        PicardEda (com.verily.picard.com) format.

            Args:
                eda_row: A row with EDA format loaded through
                DSDSK.
            Return:
                picard_eda_row: A row with PicardEda format.
        """
        picard_eda_fields = self.adc_list_to_picard_eda_series(
            eda_row["raw_adc"])
        picard_eda_row = eda_row.copy()
        del picard_eda_row["raw_adc"]
        picard_eda_row["real_adc"] = picard_eda_fields.real_adc
        picard_eda_row["im_adc"] = picard_eda_fields.im_adc
        picard_eda_row["abs_adc"] = picard_eda_fields.abs_adc
        picard_eda_row["more_info"] = picard_eda_fields.more_info
        return picard_eda_row

    # Static methods
    @staticmethod
    def adc_list_to_picard_eda_series(raw_adc_list: List[int]) \
     -> pd.Series:
        """Generate real_adc, im_adc, abs_adc and more_info from a raw_adc list.

            Args:
            raw_adc_list: list of raw_adc values
            Return:
            a pd.Series with "real_adc", "im_adc", "abs_adc" and "more_info"
            fields
        """
        real_adc_list = []
        im_adc_list = []
        abs_adc_list = []
        more_info_list = []
        for raw_adc in raw_adc_list:
            bin_raw_adc = bin(int(raw_adc) & 0xffffffff)[2:]
            bin_raw_adc = "0" * (32 - len(bin_raw_adc)) + bin_raw_adc
            more_info_list.append(int(bin_raw_adc[0]))
            bin_raw_adc = "0" + bin_raw_adc[1:]
            real_adc_list.append(int(bin_raw_adc[:16], 2) - 2048)
            im_adc_list.append(int(bin_raw_adc[16:], 2) - 2048)
            abs_adc_list.append(int((real_adc_list[-1]**2 + \
                im_adc_list[-1]**2)**0.5))
        return pd.Series({
            "real_adc": real_adc_list,
            "im_adc": im_adc_list,
            "abs_adc": abs_adc_list,
            "more_info": more_info_list
        })


def _picard_eda_df_to_dps(
        picard_eda_df: pd.DataFrame) -> Iterable[schemas.PicardEda]:
    """Converts a list of datapoints from dataframe."""

    picard_eda_dps = []
    picard_eda_rows = picard_eda_df.to_dict("records")
    for row in picard_eda_rows:
        print("row:", row)
        # To ensure that mandatory "measurement_timestamp_utc" attribute exists.
        if not "measurement_timestamp_utc" in row:
            raise ValueError("measurement_timestamp_utc must exist in "
                             "the EDA row")
        # Due to the bursty nature of EDA and the time windowing we do,
        # there might be rows with empty raw_adc values. We filter those rows.
        if isinstance(row["abs_adc"], list):
            picard_eda_dps.append(
                schemas.PicardEda(
                    data_point_metadata=(
                        ds_sdk.schemas.
                        data_point_metadata_for_derived_data_from_df(
                            picard_eda_df)),
                    measurement_timestamp_utc=row["measurement_timestamp_utc"],
                    real_adc=row["real_adc"],
                    im_adc=row["im_adc"],
                    abs_adc=row["abs_adc"],
                    more_info=row["more_info"]))
    return picard_eda_dps


class Eda2PicardPtransform(beam.PTransform):
    """Converts data window of window_len_sec length with
     EDA data spec to Picard EDA data spec; returns one data point per
    row of new data format."""

    def __init__(self, window_len_sec: int = 3) -> None:
        """Args:
                window_len_sec: Length of EDA window in seconds.
                The default value is 3 seconds.
            """
        super().__init__()
        self._window_len_sec = window_len_sec

    def expand(
        self, pcol: beam.PCollection[ds_sdk.schemas.Eda]
    ) -> beam.PCollection[schemas.PicardEda]:
        windowed_pcol = pcol | (
            ds_sdk.transforms.BuildDataPointDataFrames.
            PerParticipantDeviceWindow(
                beam_window_fn=beam.transforms.window.FixedWindows(
                    self._window_len_sec),
                combine_method=None))

        return (windowed_pcol | beam.Map(PicardEDAConverter()) |
                beam.FlatMap(_picard_eda_df_to_dps))
