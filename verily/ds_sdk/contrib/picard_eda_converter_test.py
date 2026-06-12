"""Tests for Picard EDA converter."""

from pathlib import Path
from typing import List, Optional
import unittest

import apache_beam as beam
from apache_beam.testing.test_pipeline import TestPipeline
from apache_beam.testing.util import assert_that
from apache_beam.testing.util import equal_to
import numpy as np
import pandas as pd

from verily.ds_sdk.contrib import picard_eda_converter as PEC
from verily.ds_sdk.core import schemas
from verily.ds_sdk.core.utils import timestamps

_TEST_FILE_DIR = Path(__file__).parent / "test_data"
_EDA_PATH = _TEST_FILE_DIR.joinpath("eda.csv")


def parse_list_string(list_string: str) -> List[int]:
    """Parses a string of list into a list of ints."""
    list_string = list_string.strip("[ ]").split(",")
    return [int(x) for x in list_string]


def create_eda_data_point(
        eda_len: int,
        true_timestamp: str,
        true_timestamp_sample_index: int,
        nominal_sampling_rate: int,
        sensor_id: int,
        measurement_timestamp_utc: Optional[str] = "2020-01-01") -> schemas.Eda:
    if true_timestamp is not None:
        true_timestamp = timestamps.datetime_to_beam_timestamp(
            pd.Timestamp(true_timestamp))
    return schemas.Eda(
        data_point_metadata=schemas.data_point_metadata_for_raw_data(
            data_source_id=3028052620409363825,
            device_id="123",
            participant_id="123",
            participant_namespace=1,
            echo_metadata=None,
            sensor_store_metadata=None,
            annotation_labels=set()),
        sensor_id=sensor_id,
        measurement_timestamp_utc=timestamps.datetime_to_beam_timestamp(
            pd.Timestamp(measurement_timestamp_utc)),
        raw_adc=list(np.arange(eda_len).astype(int)),
        true_timestamp_millis=true_timestamp,
        true_timestamp_sample_index=true_timestamp_sample_index,
        sampling_rate=nominal_sampling_rate)


class TestPicardEDAConverter(unittest.TestCase):
    """Class to test the Picard EDA converter algorithm."""

    def setUp(self):
        """Initiates the class instance."""
        super().setUp()
        # The path of pre-generated EDA data from
        with open(_EDA_PATH, encoding="utf-8") as f:
            gbq_eda_df = pd.read_csv(f, index_col=[0])
        raw_adc = []
        real_adc = []
        im_adc = []
        abs_adc = []
        more_info = []
        gbq_eda_rows = gbq_eda_df.to_dict("records")
        for row in gbq_eda_rows:
            raw_adc.append(parse_list_string(row["raw_adc"]))
            real_adc.append(parse_list_string(row["real_adc"]))
            im_adc.append(parse_list_string(row["im_adc"]))
            abs_adc.append(parse_list_string(row["abs_adc"]))
            more_info.append(parse_list_string(row["more_info"]))
        self.eda_df = pd.DataFrame({"raw_adc": raw_adc})
        self.picard_eda_df = pd.DataFrame({
            "real_adc": real_adc,
            "im_adc": im_adc,
            "abs_adc": abs_adc,
            "more_info": more_info
        })
        self.converter = PEC.PicardEDAConverter()

    def test_convert_to_picard_format(self):
        converted_eda_df = self.converter(self.eda_df)
        for col_name in self.picard_eda_df.columns:
            self.assertIn(col_name, converted_eda_df)

    def test_eda_row_to_picard_eda_row(self) -> None:
        for row_ind in range(self.picard_eda_df.shape[0]):
            eda_row = self.eda_df.iloc[row_ind]
            transformed_row = self.converter.eda_row_to_picard_eda_row(eda_row)
            for col_name in self.picard_eda_df.columns:
                if col_name not in transformed_row:
                    raise ValueError(
                        f"The transformed EDA row misses column {col_name}")
                np.testing.assert_array_almost_equal(
                    transformed_row[col_name],
                    self.picard_eda_df.iloc[row_ind][col_name])

    def test_adc_list_to_picard_eda_series(self) -> None:
        """Explanation:
        `com.veri.eda`: raw_adc = 2147683648
        Convert to binary:
        2147683648 -> 0b 1000000000000011 0000110101000000
        Most significant bit (MSB) is 1. Extract and use it in `more_info` in
        com.verily.picard.eda format, where 1 means LOD, 0 means OWD.
        Split the real and imaginary parts:
        I (real): 0b0000000000000011, Q (imaginary): 0b0000110101000000
        Convert to decimal:
        I (real): 3, Q (imaginary): 3392
        Subtract 2^11 = 2048:
        I (real): 3-2048 = -2045, Q (imaginary): 3392-2048 = 1344
        So the result for `com.verily.picard.eda` would be:
        real_adc: -2045, im_adc: 1344, and
        abs_adc: int( ( (-2045)**2 + 1344**2 ) ** 0.5 ) = 2447
        """
        adc_list = [2147683648]
        expected_eda_series = pd.Series({
            "abs_adc": [2447],
            "real_adc": [-2045],
            "im_adc": [1344],
            "more_info": [1]
        })
        transformed_eda_series = \
            self.converter.adc_list_to_picard_eda_series(adc_list)
        for key, value in expected_eda_series.iteritems():
            self.assertIn(key, transformed_eda_series)
            self.assertAlmostEqual(transformed_eda_series[key][0], value[0])

    def test_picard_eda_ptransform(self):
        data_points = [
            create_eda_data_point(
                3,
                "2022-01-01 00:00:01",
                10,
                10,
                "1",
                measurement_timestamp_utc="2022-01-01 00:00:01"),
            create_eda_data_point(
                3,
                "2022-01-01 00:00:02",
                10,
                10,
                "1",
                measurement_timestamp_utc="2022-01-01 00:00:02"),
            create_eda_data_point(
                10,
                "2022-01-01 01:00:00",
                10,
                10,
                "1",
                measurement_timestamp_utc="2022-01-01 01:00:00"),
            create_eda_data_point(
                20,
                "2022-01-01 01:00:05",
                10,
                10,
                "1",
                measurement_timestamp_utc="2022-01-01 01:00:10")
        ]
        expected_len = [len(data_points)]
        with TestPipeline(runner="DirectRunner") as p:
            input_data = p | beam.Create(data_points)
            output = input_data | PEC.Eda2PicardPtransform()
            counts = output | beam.combiners.Count.Globally()
            assert_that(counts, equal_to(expected_len))


if __name__ == "__main__":
    unittest.main()
