"""Tests for mock_sensors_io.py"""

import unittest

import apache_beam as beam
import pandas as pd

from verily.ds_sdk.core import options
from verily.ds_sdk.core import schemas
from verily.ds_sdk.core.testing import mock_sensors_io
from verily.ds_sdk.core.utils import timestamps


def create_pressure_data_point(pressure: int = 1):
    return schemas.Pressure(
        data_point_metadata=schemas.data_point_metadata_for_raw_data(
            data_source_id=3028052620409363825,
            device_id='123',
            participant_id='123',
            participant_namespace=1,
            echo_metadata=None,
            sensor_store_metadata=None,
            annotation_labels=set(),
        ),
        measurement_timestamp_utc=timestamps.datetime_to_beam_timestamp(
            pd.Timestamp('2020-01-01')
        ),
        pressure=pressure,
    )


def create_annotation(label: str = 'label'):
    return schemas.Annotation(
        annotation_label=label,
        start_timestamp_utc=timestamps.datetime_to_beam_timestamp(
            pd.Timestamp('2020-01-01')
        ),
        end_timestamp_utc=timestamps.datetime_to_beam_timestamp(
            pd.Timestamp('2020-01-01')
        ),
        annotation_metadata=schemas.AnnotationMetadata(
            device_id='123',
            participant_id='123',
            participant_namespace=1,
            version_name='v',
            version_number=1,
            input_data_info=[],
        ),
    )


class MockSensorsIoTest(unittest.TestCase):
    def test_data_points_bq(self):
        data_point = create_pressure_data_point()

        mock_io = mock_sensors_io.MockSensorsIO(
            input_source_data_points={'com.verily.pressure': [data_point]},
            expected_bigquery_data_points={'proj.data.table': [data_point]},
        )

        data_point_pcol = mock_io.echo_data_point_rows(
            data_spec_name='com.verily.pressure',
            source_options=options.BatchSourceOptions(),
            condition=None,
            incremental_query_options=None,
            annotation_inner_join_options=None,
        )

        _ = data_point_pcol | mock_io.write_data_points_to_big_query(
            'proj.data.table', schemas.Pressure
        )

        mock_io.run()

    def test_custom_data_points_bq(self):
        data_point = create_pressure_data_point()

        mock_io = mock_sensors_io.MockSensorsIO(
            input_source_data_points={'my.custom.table': [data_point]},
            expected_bigquery_data_points={'proj.data.table': [data_point]},
        )

        data_point_pcol = mock_io.custom_data_point_rows(
            data_point_table_id='my.custom.table',
            row_schema=schemas.Pressure,
            source_options=options.BatchSourceOptions(),
            condition=None,
            annotation_inner_join_options=None,
        )

        _ = data_point_pcol | mock_io.write_data_points_to_big_query(
            'proj.data.table', schemas.Pressure
        )

        mock_io.run()

    def test_data_points_bq_failed_assert(self):
        data_point = create_pressure_data_point()
        bad_data_point = create_pressure_data_point(pressure=2)

        mock_io = mock_sensors_io.MockSensorsIO(
            input_source_data_points={'com.verily.pressure': [data_point]},
            expected_bigquery_data_points={'proj.data.table': [bad_data_point]},
        )

        data_point_pcol = mock_io.echo_data_point_rows(
            data_spec_name='com.verily.pressure',
            source_options=options.BatchSourceOptions(),
            condition=None,
            incremental_query_options=None,
            annotation_inner_join_options=None,
        )

        _ = data_point_pcol | mock_io.write_data_points_to_big_query(
            'proj.data.table', schemas.Pressure
        )

        with self.assertRaises(beam.testing.util.BeamAssertException):
            mock_io.run()

    def test_data_points_ss(self):
        data_point = create_pressure_data_point()

        mock_io = mock_sensors_io.MockSensorsIO(
            input_source_data_points={'com.verily.pressure': [data_point]},
            expected_sensor_store_data_points={
                'com.verily.pressure': [data_point]
            },
        )

        data_point_pcol = mock_io.echo_data_point_rows(
            data_spec_name='com.verily.pressure',
            source_options=options.BatchSourceOptions(),
            condition=None,
            incremental_query_options=None,
            annotation_inner_join_options=None,
        )

        _ = data_point_pcol | mock_io.write_to_sensor_store(
            schema=schemas.Pressure,
            algorithm_name='unused',
            algorithm_version='unused',
            overwrite_key_generator=None,
            api_key='unused',
        )

        mock_io.run()

    def test_data_points_ss_failed_assert(self):
        data_point = create_pressure_data_point()
        bad_data_point = create_pressure_data_point(pressure=2)

        mock_io = mock_sensors_io.MockSensorsIO(
            input_source_data_points={'com.verily.pressure': [data_point]},
            expected_sensor_store_data_points={
                'com.verily.pressure': [bad_data_point]
            },
        )

        data_point_pcol = mock_io.echo_data_point_rows(
            data_spec_name='com.verily.pressure',
            source_options=options.BatchSourceOptions(),
            condition=None,
            incremental_query_options=None,
            annotation_inner_join_options=None,
        )

        _ = data_point_pcol | mock_io.write_to_sensor_store(
            schema=schemas.Pressure,
            algorithm_name='unused',
            algorithm_version='unused',
            overwrite_key_generator=None,
            api_key='unused',
        )

        with self.assertRaises(beam.testing.util.BeamAssertException):
            mock_io.run()

    def test_annotations_bq(self):
        annotation = create_annotation()

        mock_io = mock_sensors_io.MockSensorsIO(
            input_source_annotations={'input.dataset.table': [annotation]},
            expected_sink_annotations={'output.dataset.table': [annotation]},
        )

        data_point_pcol = mock_io.annotation_rows(
            bigquery_table='input.dataset.table',
            source_options=options.BatchSourceOptions(),
            condition=None,
        )

        _ = data_point_pcol | mock_io.write_annotations_to_bigquery(
            'output.dataset.table'
        )

        mock_io.run()

    def test_annotations_bq_failed_assert(self):
        annotation = create_annotation()
        bad_annotation = create_annotation('diff')

        mock_io = mock_sensors_io.MockSensorsIO(
            input_source_annotations={'input.dataset.table': [annotation]},
            expected_sink_annotations={
                'output.dataset.table': [bad_annotation]
            },
        )

        data_point_pcol = mock_io.annotation_rows(
            bigquery_table='input.dataset.table',
            source_options=options.BatchSourceOptions(),
            condition=None,
        )

        _ = data_point_pcol | mock_io.write_annotations_to_bigquery(
            'output.dataset.table'
        )

        with self.assertRaises(beam.testing.util.BeamAssertException):
            mock_io.run()


if __name__ == '__main__':
    unittest.main()
