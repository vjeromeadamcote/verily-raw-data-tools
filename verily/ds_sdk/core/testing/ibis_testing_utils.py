"""Utils for creating Ibis table w/ schemas for testing."""

from typing import List, Optional

import ibis

DATA_POINTS_TABLE_SCHEMA = [
    ('UserID', 'string'), ('DeviceID', 'string'), ('UserNamespace', 'string'),
    ('BucketWriteTime', 'timestamp'), ('DataPointTime', 'timestamp'),
    ('DataPoint',
     ibis.expr.datatypes.Struct(['t', 'awesome_data'],
                                ['timestamp', 'string'])),
    ('DataSource',
     ibis.expr.datatypes.Struct(
         ['name', 'sensor', 'algorithm', 'application', 'device', 'data_spec'],
         [
             'string',
             ibis.expr.datatypes.Struct(['id'], ['string']),
             ibis.expr.datatypes.Struct(
                 ['name', 'version'], ['string', 'string']), 'string', 'string',
             ibis.expr.datatypes.Struct(['field_specs'], ['string'])
         ])), ('SnapshotTime', 'timestamp')
]


def data_points_table(table_name: str):
    return ibis.table(DATA_POINTS_TABLE_SCHEMA, name=table_name)


INTERNAL_DATA_POINTS_TABLE_SCHEMA = [
    ('DeviceID', 'string'),
    ('DataPointWriteTime', 'timestamp'),
    ('DataPointTime', 'timestamp'),
    ('SnapshotTime', 'timestamp'),
    ('DeletedTime', 'timestamp'),
    ('DataSourceID', 'int64'),
]


def internal_data_points_table(
        table_name: str,
        data_point_fields: Optional[List[str]] = None,
        data_point_field_types: Optional[List[str]] = None):

    if data_point_fields is None:
        data_point_fields = ['t', 'awesome_data']
    if data_point_field_types is None:
        data_point_field_types = ['timestamp', 'string']
    data_point_entry = ('DataPoint',
                        ibis.expr.datatypes.Struct(data_point_fields,
                                                   data_point_field_types))
    return ibis.table([*INTERNAL_DATA_POINTS_TABLE_SCHEMA, data_point_entry],
                      name=table_name)


ANNOTATIONS_TABLE_SCHEMA = [
    ('user_id', 'string'), ('device_id', 'string'),
    ('user_namespace', 'string'), ('start_timestamp_utc', 'timestamp'),
    ('end_timestamp_utc', 'timestamp'), ('annotation_label', 'string'),
    ('version_name', 'string'), ('version_number', 'int64'),
    ('input_data_info',
     ibis.expr.datatypes.Struct(
         ['version_number', 'version_name', 'metric_type'], [
             'int64',
             'string',
             ibis.expr.datatypes.Struct(
                 ['stream_item_type', 'derived_data_type', 'annotation_type'], [
                     'int64',
                     'int64',
                     'int64',
                 ]),
         ]))
]


def annotations_table(table_name: str):
    return ibis.table(ANNOTATIONS_TABLE_SCHEMA, name=table_name)


PARTICIPANTS_TABLE_SCHEMA = [
    ('ParticipantId', 'string'),
    ('DeviceId', 'string'),
    ('StartTime', 'timestamp'),
    ('EndTime', 'timestamp'),
]


def paricipant_mapping_table(table_name: str):
    return ibis.table(PARTICIPANTS_TABLE_SCHEMA, name=table_name)
