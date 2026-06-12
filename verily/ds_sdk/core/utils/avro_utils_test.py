"""Tests for ds_sdk.core.utils.avro_utils."""

import os
import tempfile
import unittest

import avro.datafile as avro_datafile
import avro.io as avro_io
import avro.schema as avro_schema_lib
from google.cloud import bigquery

from verily.ds_sdk.core.utils import avro_utils


class AvroUtilsTest(unittest.TestCase):

    def test_merge_primitive_schemas(self):
        schema_dict1 = {
            'namespace':
                'testing',
            'name':
                'Schema1',
            'type':
                'record',
            'fields': [{
                'name': 'field1',
                'type': 'int'
            }, {
                'name': 'common_field',
                'type': 'string'
            }]
        }
        schema1 = avro_schema_lib.SchemaFromJSONData(
            schema_dict1)  # type: ignore

        schema_dict2 = {
            'namespace':
                'testing',
            'name':
                'Schema2',
            'type':
                'record',
            'fields': [{
                'name': 'field2',
                'type': 'int'
            }, {
                'name': 'common_field',
                'type': 'string'
            }]
        }

        schema2 = avro_schema_lib.SchemaFromJSONData(
            schema_dict2)  # type: ignore

        expected_schema_data = {
            'field1': 1,
            'field2': 2,
            'common_field': 'test'
        }

        merged_schema = avro_utils.merge_schemas(schema1, schema2)

        temp_dir = tempfile.gettempdir()
        path = os.path.join(temp_dir, 'testing.avro')
        with avro_datafile.DataFileWriter(open(path,
                                               'wb'), avro_io.DatumWriter(),
                                          merged_schema) as file:
            file.append(expected_schema_data)

        with avro_datafile.DataFileReader(open(path, 'rb'),
                                          avro_io.DatumReader()) as reader:
            for row in reader:
                self.assertEqual(row['field1'], 1)
                self.assertEqual(row['field2'], 2)
                self.assertEqual(row['common_field'], 'test')

    def test_merge_schemas_array(self):
        schema_dict1 = {
            'namespace':
                'testing',
            'name':
                'Schema1',
            'type':
                'record',
            'fields': [{
                'name': 'field1',
                'type': {
                    'type': 'array',
                    'items': 'long'
                }
            }, {
                'name': 'common_field',
                'type': {
                    'type': 'array',
                    'items': 'null'
                }
            }]
        }
        schema1 = avro_schema_lib.SchemaFromJSONData(
            schema_dict1)  # type: ignore

        schema_dict2 = {
            'namespace':
                'testing',
            'name':
                'Schema2',
            'type':
                'record',
            'fields': [{
                'name': 'field2',
                'type': {
                    'type': 'array',
                    'items': 'string'
                }
            }, {
                'name': 'common_field',
                'type': {
                    'type': 'array',
                    'items': 'float'
                }
            }]
        }

        schema2 = avro_schema_lib.SchemaFromJSONData(
            schema_dict2)  # type: ignore

        expected_schema_data = {
            'field1': [1, 2],
            'field2': ['a', 'b'],
            'common_field': [3.0, None]
        }

        merged_schema = avro_utils.merge_schemas(schema1, schema2)

        temp_dir = tempfile.gettempdir()
        path = os.path.join(temp_dir, 'testing.avro')
        with avro_datafile.DataFileWriter(open(path,
                                               'wb'), avro_io.DatumWriter(),
                                          merged_schema) as file:
            file.append(expected_schema_data)

        with avro_datafile.DataFileReader(open(path, 'rb'),
                                          avro_io.DatumReader()) as reader:
            for row in reader:
                self.assertEqual(row['field1'], [1, 2])
                self.assertEqual(row['field2'], ['a', 'b'])
                self.assertEqual(row['common_field'], [3.0, None])

    def test_merge_schemas_nested(self):
        schema1_names = avro_schema_lib.Names()
        schema2_names = avro_schema_lib.Names()

        common_with_differences_schema1 = {
            'namespace':
                'testing',
            'name':
                'CommonWithDiffs',
            'type':
                'record',
            'fields': [{
                'name': 'common_with_diffs_field',
                'type': ['null', 'string']
            }]
        }

        common_with_differences_schema1_schema = avro_schema_lib.SchemaFromJSONData(  # pylint: disable=line-too-long
            common_with_differences_schema1, schema1_names)

        common_with_differences_schema2 = {
            'namespace':
                'testing',
            'name':
                'CommonWithDiffs',
            'type':
                'record',
            'fields': [{
                'name': 'common_with_diffs_field',
                'type': ['null', 'int']
            }]
        }

        common_with_differences_schema2_schema = avro_schema_lib.SchemaFromJSONData(  # pylint: disable=line-too-long
            common_with_differences_schema2, schema2_names)

        common_nested_dict = {
            'namespace': 'testing',
            'name': 'CommonField',
            'type': 'record',
            'fields': [{
                'name': 'common_field',
                'type': 'string'
            }]
        }
        common_nested_schema1 = avro_schema_lib.SchemaFromJSONData(
            common_nested_dict, schema1_names)

        schema1_nested_dict = {
            'namespace': 'testing',
            'name': 'Schema1Nested',
            'type': 'record',
            'fields': [{
                'name': 'nested_field1',
                'type': 'long'
            }]
        }
        schema1_nested = avro_schema_lib.SchemaFromJSONData(
            schema1_nested_dict, schema1_names)
        schema_dict1 = {
            'namespace':
                'testing',
            'name':
                'Schema1',
            'type':
                'record',
            'fields': [{
                'name': 'schema1_nested',
                'type': schema1_nested.fullname,
                'default': None
            }, {
                'name': 'common_nested',
                'type': ['null', common_nested_schema1.fullname],
                'default': None
            }, {
                'name':
                    'common_with_diff',
                'type': [
                    'null', common_with_differences_schema1_schema.fullname
                ]
            }]
        }

        schema1 = avro_schema_lib.SchemaFromJSONData(
            schema_dict1, schema1_names)  # type: ignore

        schema2_nested_dict = {
            'namespace':
                'testing',
            'name':
                'Schema2Nested',
            'type':
                'record',
            'fields': [{
                'name': 'nested_field2',
                'type': {
                    'type': 'array',
                    'items': 'string'
                }
            }]
        }
        schema2_nested = avro_schema_lib.SchemaFromJSONData(
            schema2_nested_dict, schema2_names)

        schema_dict2 = {
            'namespace':
                'testing',
            'name':
                'Schema2',
            'type':
                'record',
            'fields': [{
                'name': 'schema2_nested',
                'type': schema2_nested.fullname,
                'default': None,
            }, {
                'name': 'common_nested',
                'type': ['null', 'string'],
                'default': None
            }, {
                'name':
                    'common_with_diff',
                'type': [
                    'null', common_with_differences_schema2_schema.fullname
                ]
            }]
        }

        schema2 = avro_schema_lib.SchemaFromJSONData(
            schema_dict2, schema2_names)  # type: ignore

        expected_schema_data = {
            'schema1_nested': {
                'nested_field1': 123
            },
            'schema2_nested': {
                'nested_field2': ['a', 'b']
            },
            'common_nested': {
                'common_field': 'testing'
            },
            'common_with_diff': {
                'common_with_diffs_field': 1
            }
        }

        merged_schema = avro_utils.merge_schemas(schema1, schema2)

        temp_dir = tempfile.gettempdir()
        path = os.path.join(temp_dir, 'testing.avro')
        with avro_datafile.DataFileWriter(open(path,
                                               'wb'), avro_io.DatumWriter(),
                                          merged_schema) as file:
            file.append(expected_schema_data)

        with avro_datafile.DataFileReader(open(path, 'rb'),
                                          avro_io.DatumReader()) as reader:
            for row in reader:
                self.assertEqual(row['schema1_nested']['nested_field1'], 123)
                self.assertEqual(row['schema2_nested']['nested_field2'],
                                 ['a', 'b'])
                self.assertEqual(row['common_nested']['common_field'],
                                 'testing')
                self.assertEqual(
                    row['common_with_diff']['common_with_diffs_field'], 1)

    def test_merge_schemas_unions(self):
        schema_dict1 = {
            'namespace': 'testing',
            'name': 'Schema1',
            'type': 'record',
            'fields': [{
                'name': 'field',
                'type': ['null', 'int']
            }]
        }
        schema1 = avro_schema_lib.SchemaFromJSONData(
            schema_dict1)  # type: ignore

        schema_dict2 = {
            'namespace':
                'testing',
            'name':
                'Schema2',
            'type':
                'record',
            'fields': [{
                'name': 'field',
                # This can happen if we were unable to determine the type of
                # 'field' so we default to string, this will be overridden when
                # we merge.
                'type': ['null', 'string']
            }]
        }

        schema2 = avro_schema_lib.SchemaFromJSONData(
            schema_dict2)  # type: ignore

        expected_schema_data = [{'field': None}, {'field': 1}, {'field': 2}]

        merged_schema = avro_utils.merge_schemas(schema1, schema2)

        temp_dir = tempfile.gettempdir()
        path = os.path.join(temp_dir, 'testing.avro')
        with avro_datafile.DataFileWriter(open(path,
                                               'wb'), avro_io.DatumWriter(),
                                          merged_schema) as file:
            for datum in expected_schema_data:
                file.append(datum)

        with avro_datafile.DataFileReader(open(path, 'rb'),
                                          avro_io.DatumReader()) as reader:
            rows = list(reader)
            self.assertEqual(rows[0]['field'], None)
            self.assertEqual(rows[1]['field'], 1)
            self.assertEqual(rows[2]['field'], 2)

    def test_to_json(self):
        schema_dict = {
            'namespace':
                'testing',
            'name':
                'Schema1',
            'type':
                'record',
            'fields': [{
                'name': 'timestamp',
                'type': {
                    'type': 'long',
                    'logicalType': 'timestamp-millis'
                }
            }]
        }
        schema = avro_schema_lib.SchemaFromJSONData(schema_dict)  # type: ignore

        data = {'timestamp': 1591366846000}

        # Verify that the JSON output can be reparsed by the avro schema parser.
        reparse = avro_schema_lib.SchemaFromJSONData(avro_utils.to_json(schema))

        temp_dir = tempfile.gettempdir()
        path = os.path.join(temp_dir, 'testing.avro')
        with avro_datafile.DataFileWriter(open(path,
                                               'wb'), avro_io.DatumWriter(),
                                          reparse) as file:
            file.append(data)

        with avro_datafile.DataFileReader(open(path, 'rb'),
                                          avro_io.DatumReader()) as reader:
            rows = list(reader)
            self.assertEqual(rows[0]['timestamp'], 1591366846000)
            self.assertEqual(len(rows), 1)

    def test_merge_invalid_schemas(self):
        # Merge non-primitive with primitive
        schema_dict1 = {
            'namespace': 'testing',
            'name': 'Schema1',
            'type': 'record',
            'fields': [{
                'name': 'field',
                'type': ['null', 'int']
            }]
        }
        schema1 = avro_schema_lib.SchemaFromJSONData(
            schema_dict1)  # type: ignore

        schema_dict2 = {
            'namespace': 'testing',
            'name': 'Schema2',
            'type': 'record',
            'fields': [{
                'name': 'field',
                'type': 'int'
            }]
        }

        schema2 = avro_schema_lib.SchemaFromJSONData(
            schema_dict2)  # type: ignore

        with self.assertRaises(ValueError):
            avro_utils.merge_schemas(schema1, schema2)

        # Merge differing primitive types
        schema_dict1 = {
            'namespace': 'testing',
            'name': 'Schema1',
            'type': 'record',
            'fields': [{
                'name': 'field',
                'type': 'int'
            }]
        }
        schema1 = avro_schema_lib.SchemaFromJSONData(
            schema_dict1)  # type: ignore

        schema_dict2 = {
            'namespace': 'testing',
            'name': 'Schema2',
            'type': 'record',
            'fields': [{
                'name': 'field',
                'type': 'float'
            }]
        }

        schema2 = avro_schema_lib.SchemaFromJSONData(
            schema_dict2)  # type: ignore

        with self.assertRaises(ValueError):
            avro_utils.merge_schemas(schema1, schema2)

    def test_avro_schema_to_bq_schema(self):

        names = avro_schema_lib.Names('testing')
        avro_schema_json = {
            'namespace':
                'ds_sdk.avro',
            'name':
                'DsSdkAvroSchema',
            'type':
                'record',
            'fields': [{
                'name': 'DeviceID',
                'type': 'string'
            }, {
                'name': 'DataPointTime',
                'type': {
                    'type': 'long',
                    'logicalType': 'timestamp-millis'
                }
            }]
        }

        avro_schema = avro_schema_lib.SchemaFromJSONData(
            avro_schema_json, names)

        want = [
            bigquery.SchemaField('DeviceID', 'STRING', 'NULLABLE'),
            bigquery.SchemaField('DataPointTime', 'TIMESTAMP', 'NULLABLE'),
        ]
        got = avro_utils.avro_schema_to_bq_schema(avro_schema)

        self.assertEqual(want, got)


if __name__ == '__main__':
    unittest.main()
