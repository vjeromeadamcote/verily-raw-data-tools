"""Tests for filter_by_annotation."""

import copy
from typing import List, NamedTuple
import unittest

import apache_beam as beam
from apache_beam.testing.test_pipeline import TestPipeline
from apache_beam.testing.util import assert_that
from apache_beam.testing.util import equal_to
from apache_beam.utils.timestamp import Timestamp
import pandas as pd

from verily.ds_sdk.core import options
from verily.ds_sdk.core import schemas
from verily.ds_sdk.core.io.bigquery import filter_by_annotation


def build_timestamp(timestamp_str: str) -> Timestamp:
    return Timestamp.from_utc_datetime(pd.Timestamp(timestamp_str, tz='UTC'))


def build_annotation(annotation_label: str, device_id: str, participant_id: str,
                     start_timestamp_str: str,
                     end_timestamp_str: str) -> schemas.Annotation:

    annotation_metadata = schemas.AnnotationMetadata(
        device_id=device_id,
        participant_id=participant_id,
        participant_namespace=1,
        version_name=None,
        version_number=None,
        input_data_info=[])

    return schemas.Annotation(
        annotation_label=annotation_label,
        start_timestamp_utc=build_timestamp(start_timestamp_str),
        end_timestamp_utc=build_timestamp(end_timestamp_str),
        annotation_metadata=annotation_metadata)


def build_data_point(device_id: str, participant_id: str,
                     timestamp_str: str) -> schemas.Pressure:

    data_point_metadata = schemas.data_point_metadata_for_raw_data(
        data_source_id=hash(device_id + participant_id),
        device_id=device_id,
        participant_id=participant_id,
        participant_namespace=1,
        echo_metadata=None,
        sensor_store_metadata=None,
        annotation_labels=set())

    return schemas.Pressure(
        data_point_metadata=data_point_metadata,
        measurement_timestamp_utc=build_timestamp(timestamp_str),
        pressure=1)


def add_annotations_to_data_point(data_point: NamedTuple,
                                  annotations: List[schemas.Annotation]):
    data_point_copy = copy.deepcopy(data_point)
    for ann in annotations:
        data_point_copy.data_point_metadata.annotation_labels.add(
            ann.annotation_label)
    return data_point_copy


class FilterByAnnotationTest(unittest.TestCase):

    def test_annotation_join_no_filter(self):
        # Only data_point_1 should join with the annotation
        data_point_1 = build_data_point(device_id='123',
                                        participant_id='456',
                                        timestamp_str='12:30:00')
        data_point_2 = build_data_point(device_id='123',
                                        participant_id='456',
                                        timestamp_str='13:30:00')

        annotation = build_annotation(annotation_label='label',
                                      device_id='123',
                                      participant_id='456',
                                      start_timestamp_str='12:00:00',
                                      end_timestamp_str='13:00:00')

        expected = [
            add_annotations_to_data_point(data_point_1, [annotation]),
            data_point_2
        ]

        with TestPipeline() as p:
            data_points = p | 'Create DataPoints' >> beam.Create(
                [data_point_1, data_point_2])
            annotations = p | 'Create Annotations' >> beam.Create([annotation])

            output = (data_points,
                      annotations) | filter_by_annotation.JoinWithAnnotations(
                          required_annotation_labels=set(['label']),
                          join_if=options.JoinIf.ANY)

            assert_that(output, equal_to(expected))

    def test_annotation_join_with_filter(self):
        # Only data_point_1 should join with the annotation
        data_point_1 = build_data_point(device_id='123',
                                        participant_id='456',
                                        timestamp_str='12:30:00')
        # data_point_2 should be filtered out since it does not overlap with the
        # annotation
        data_point_2 = build_data_point(device_id='123',
                                        participant_id='456',
                                        timestamp_str='13:30:00')

        annotation = build_annotation(annotation_label='label',
                                      device_id='123',
                                      participant_id='456',
                                      start_timestamp_str='12:00:00',
                                      end_timestamp_str='13:00:00')

        expected = [add_annotations_to_data_point(data_point_1, [annotation])]

        with TestPipeline() as p:
            data_points = p | 'Create DataPoints' >> beam.Create(
                [data_point_1, data_point_2])
            annotations = p | 'Create Annotations' >> beam.Create([annotation])

            output = (data_points,
                      annotations) | filter_by_annotation.FilterByAnnotations(
                          required_annotation_labels=set(['label']),
                          join_if=options.JoinIf.ANY)

            assert_that(output, equal_to(expected))

    def test_annotation_join_multiple_devices(self):
        # Only data_point_1 should join with the annotation
        data_point_1 = build_data_point(device_id='123',
                                        participant_id='456',
                                        timestamp_str='12:30:00')
        # data_point_2 should not include the annotation since it has a
        # different device id
        data_point_2 = build_data_point(device_id='321',
                                        participant_id='456',
                                        timestamp_str='12:30:00')
        # data_point_3 should not include the annotation since it has a
        # different participant id
        data_point_3 = build_data_point(device_id='123',
                                        participant_id='654',
                                        timestamp_str='12:30:00')

        annotation = build_annotation(annotation_label='label',
                                      device_id='123',
                                      participant_id='456',
                                      start_timestamp_str='12:00:00',
                                      end_timestamp_str='13:00:00')

        expected = [
            add_annotations_to_data_point(data_point_1, [annotation]),
            data_point_2, data_point_3
        ]

        with TestPipeline() as p:
            data_points = p | 'Create DataPoints' >> beam.Create(
                [data_point_1, data_point_2, data_point_3])
            annotations = p | 'Create Annotations' >> beam.Create([annotation])

            output = (data_points,
                      annotations) | filter_by_annotation.JoinWithAnnotations(
                          required_annotation_labels=set(['label']),
                          join_if=options.JoinIf.ANY)

            assert_that(output, equal_to(expected))

    def test_ann_filter_multiple_devices_multiple_annotations_with_join_any(
            self):
        # data_point_1 should have 2 annotation labels
        data_point_1 = build_data_point(device_id='123',
                                        participant_id='456',
                                        timestamp_str='12:30:00')
        # data_point_2 should have 1 annotation label
        data_point_2 = build_data_point(device_id='321',
                                        participant_id='456',
                                        timestamp_str='12:30:00')
        # data_point_3 should be filtered out because it has no annotations
        data_point_3 = build_data_point(device_id='123',
                                        participant_id='654',
                                        timestamp_str='12:30:00')

        # label 1 for data_point_1
        annotation_1_1 = build_annotation(annotation_label='label_one_one',
                                          device_id='123',
                                          participant_id='456',
                                          start_timestamp_str='12:00:00',
                                          end_timestamp_str='13:00:00')
        # label 2 for data_point_1
        annotation_1_2 = build_annotation(annotation_label='label_one_two',
                                          device_id='123',
                                          participant_id='456',
                                          start_timestamp_str='12:30:00',
                                          end_timestamp_str='14:00:00')
        # label 1 for data_point_2
        annotation_2_1 = build_annotation(annotation_label='label_two_one',
                                          device_id='321',
                                          participant_id='456',
                                          start_timestamp_str='12:15:00',
                                          end_timestamp_str='12:40:00')

        expected = [
            add_annotations_to_data_point(data_point_1,
                                          [annotation_1_1, annotation_1_2]),
            add_annotations_to_data_point(data_point_2, [annotation_2_1]),
        ]

        with TestPipeline() as p:
            data_points = p | 'Create DataPoints' >> beam.Create(
                [data_point_1, data_point_2, data_point_3])
            annotations = p | 'Create Annotations' >> beam.Create(
                [annotation_1_1, annotation_1_2, annotation_2_1])

            output = (data_points,
                      annotations) | filter_by_annotation.FilterByAnnotations(
                          required_annotation_labels=set([
                              'label_one_one', 'label_one_two', 'label_two_one'
                          ]),
                          join_if=options.JoinIf.ANY)

            assert_that(output, equal_to(expected))

    def test_ann_filter_multiple_devices_multiple_annotations_with_join_all(
            self):
        # data_point_1 should have 2 annotation labels
        data_point_1 = build_data_point(device_id='123',
                                        participant_id='456',
                                        timestamp_str='12:30:00')
        # data_point_2 should be filtered out because it only have 1 annotation.
        data_point_2 = build_data_point(device_id='321',
                                        participant_id='456',
                                        timestamp_str='12:30:00')
        # data_point_3 should be filtered out because it has no annotations
        data_point_3 = build_data_point(device_id='123',
                                        participant_id='654',
                                        timestamp_str='12:30:00')

        # label 1 for data_point_1
        annotation_1_1 = build_annotation(annotation_label='label_one_one',
                                          device_id='123',
                                          participant_id='456',
                                          start_timestamp_str='12:00:00',
                                          end_timestamp_str='13:00:00')
        # label 2 for data_point_1
        annotation_1_2 = build_annotation(annotation_label='label_one_two',
                                          device_id='123',
                                          participant_id='456',
                                          start_timestamp_str='12:30:00',
                                          end_timestamp_str='14:00:00')
        # label 1 for data_point_2
        annotation_2_1 = build_annotation(annotation_label='label_two_one',
                                          device_id='321',
                                          participant_id='456',
                                          start_timestamp_str='12:15:00',
                                          end_timestamp_str='12:40:00')

        expected = [
            add_annotations_to_data_point(data_point_1,
                                          [annotation_1_1, annotation_1_2]),
        ]

        with TestPipeline() as p:
            data_points = p | 'Create DataPoints' >> beam.Create(
                [data_point_1, data_point_2, data_point_3])
            annotations = p | 'Create Annotations' >> beam.Create(
                [annotation_1_1, annotation_1_2, annotation_2_1])

            output = (data_points,
                      annotations) | filter_by_annotation.FilterByAnnotations(
                          required_annotation_labels=set(
                              ['label_one_one', 'label_one_two']),
                          join_if=options.JoinIf.ALL)

            assert_that(output, equal_to(expected))

    def test_annotation_join_not_using_participant(self):
        # Only data_point_1 should join with the annotation
        data_point_1 = build_data_point(device_id='123',
                                        participant_id='456',
                                        timestamp_str='12:30:00')
        # data_point_2 should be filtered out since it does not overlap with the
        # annotation
        data_point_2 = build_data_point(device_id='123',
                                        participant_id='456',
                                        timestamp_str='13:30:00')

        # data_point_3 should be filtered out since it has a different device_id
        data_point_3 = build_data_point(device_id='321',
                                        participant_id='456',
                                        timestamp_str='13:00:00')

        # Annotation should still be joined, even thought the participant is
        # different.
        annotation = build_annotation(annotation_label='label',
                                      device_id='123',
                                      participant_id='321',
                                      start_timestamp_str='12:00:00',
                                      end_timestamp_str='13:00:00')

        expected = [add_annotations_to_data_point(data_point_1, [annotation])]

        with TestPipeline() as p:
            data_points = p | 'Create DataPoints' >> beam.Create(
                [data_point_1, data_point_2, data_point_3])
            annotations = p | 'Create Annotations' >> beam.Create([annotation])

            output = (data_points,
                      annotations) | filter_by_annotation.FilterByAnnotations(
                          required_annotation_labels=set(['label']),
                          join_if=options.JoinIf.ANY,
                          join_on_participant=False)

            assert_that(output, equal_to(expected))

    def test_annotation_join_rounded_time(self):
        # data_point_1 should always join with the annotation
        data_point_1 = build_data_point(device_id='123',
                                        participant_id='456',
                                        timestamp_str='2025-01-01 12:00:00.123')

        # data_point_2 should join with the annotation if time_rounded is True
        # but not if it is False
        data_point_2 = build_data_point(device_id='123',
                                        participant_id='456',
                                        timestamp_str='2025-01-01 12:00:00')

        # data_point_3 should be filtered out since it does not overlap with the
        # annotation with or without time rounding
        data_point_3 = build_data_point(device_id='123',
                                        participant_id='456',
                                        timestamp_str='2025-01-01 11:59:00')


        start_ts_str = '2025-01-01 12:00:00.123'
        end_ts_str = '2025-01-01 13:00:00'
        annotation = build_annotation(annotation_label='label',
                                      device_id='123',
                                      participant_id='456',
                                      start_timestamp_str=start_ts_str,
                                      end_timestamp_str=end_ts_str)

        expected_time_rounded_true = [
            add_annotations_to_data_point(data_point_1, [annotation]),
            add_annotations_to_data_point(data_point_2, [annotation])]

        with TestPipeline() as p:
            data_points = p | 'Create DataPoints' >> beam.Create(
                [data_point_1, data_point_2, data_point_3])
            annotations = p | 'Create Annotations' >> beam.Create([annotation])

            output_rounded_true = (data_points,
                      annotations) | filter_by_annotation.FilterByAnnotations(
                          required_annotation_labels=set(['label']),
                          join_if=options.JoinIf.ANY,
                          round_to_second=True)

            assert_that(output_rounded_true,
                        equal_to(expected_time_rounded_true))

    def test_annotation_join_unrounded_time(self):
        # data_point_1 should always join with the annotation
        data_point_1 = build_data_point(device_id='123',
                                        participant_id='456',
                                        timestamp_str='2025-01-01 12:00:00.123')

        # data_point_2 should join with the annotation if time_rounded is True
        # but not if it is False
        data_point_2 = build_data_point(device_id='123',
                                        participant_id='456',
                                        timestamp_str='2025-01-01 12:00:00')

        # data_point_3 should be filtered out since it does not overlap with the
        # annotation with or without time rounding
        data_point_3 = build_data_point(device_id='123',
                                        participant_id='456',
                                        timestamp_str='2025-01-01 11:59:00')

        start_ts_str = '2025-01-01 12:00:00.123'
        end_ts_str = '2025-01-01 13:00:00'
        annotation = build_annotation(annotation_label='label',
                                      device_id='123',
                                      participant_id='456',
                                      start_timestamp_str=start_ts_str,
                                      end_timestamp_str=end_ts_str)

        expected_time_rounded_false = [
            add_annotations_to_data_point(data_point_1, [annotation])]

        with TestPipeline() as p:
            data_points = p | 'Create DataPoints' >> beam.Create(
                [data_point_1, data_point_2, data_point_3])
            annotations = p | 'Create Annotations' >> beam.Create([annotation])

            output_rounded_false = (data_points,
                      annotations) | filter_by_annotation.FilterByAnnotations(
                          required_annotation_labels=set(['label']),
                          join_if=options.JoinIf.ANY,
                          round_to_second=False)

            assert_that(output_rounded_false,
                        equal_to(expected_time_rounded_false))


if __name__ == '__main__':
    unittest.main()
