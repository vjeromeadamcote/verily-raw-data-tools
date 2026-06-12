"""Utils for working with Avro."""

import collections
from typing import Any, Dict, Iterable, List

import avro.schema as avro_schema_lib
from google.cloud import bigquery  # type: ignore
from google.protobuf import descriptor

from verily.ds_sdk.protos import types_pb2

RECORD = 'record'
STRING = 'string'
ARRAY = 'array'
UNION = 'union'
NULL = 'null'
NON_PRIMITIVE_TYPES = [RECORD, STRING, ARRAY, UNION]

AVRO_TYPE_TO_BQ_TYPE = {
    'boolean': 'BOOLEAN',
    'int': 'INTEGER',
    'long': 'INTEGER',
    'float': 'FLOAT',
    'double': 'FLOAT',
    'bytes': 'BYTES',
    'string': 'STRING',
}

_PROTO_TO_AVRO_PRIMITIVE = {
    descriptor.FieldDescriptor.TYPE_INT32: 'int',
    descriptor.FieldDescriptor.TYPE_INT64: 'long',
    descriptor.FieldDescriptor.TYPE_UINT32: 'int',
    descriptor.FieldDescriptor.TYPE_UINT64: 'long',
    descriptor.FieldDescriptor.TYPE_DOUBLE: 'double',
    descriptor.FieldDescriptor.TYPE_FLOAT: 'float',
    descriptor.FieldDescriptor.TYPE_BOOL: 'boolean',
    descriptor.FieldDescriptor.TYPE_ENUM: 'int',
    descriptor.FieldDescriptor.TYPE_STRING: 'string',
}


class _EmptyDefault:
    """Holder class to indicate no default value."""


def update_default_fields(to_write: Dict[str, Any],
                          avro_schema: avro_schema_lib.Schema):
    """Inplace updates to_write to have default values set.

    For some reason avro doesn't automatically populate the default values.
    This will add a field to to_write if it is missing and has a default value
    in avro_schema.

    Args:
        to_write: the dictionary to update.
        avro_schema:  the avro schema to get fields from.

    Returns:
        The same dictionary that was updated (to_write)
    """

    for field in avro_schema.fields:  # type: ignore
        if field.name not in to_write:
            if field.has_default:  # type: ignore
                to_write[field.name] = field.default  # type: ignore
            else:
                raise ValueError(f'Missing required field: {field.name}')

    return to_write


def merge_schemas(schema1: avro_schema_lib.Schema,
                  schema2: avro_schema_lib.Schema) -> avro_schema_lib.Schema:
    """Merges schema1 and schema2 into one avro schema."""

    if schema1 is None:
        return schema2

    if schema2 is None:
        return schema1

    if schema1 == schema2:
        return schema1

    type1 = schema1.type
    type2 = schema2.type

    if type1 == RECORD and type2 == RECORD:
        field_schema_dict = collections.OrderedDict()
        field_default_dict = collections.OrderedDict()

        for field in schema1.fields:  # type: ignore
            field_schema_dict[field.name] = field.type
            if field.has_default:
                field_default_dict[field.name] = field.default

        for field in schema2.fields:  # type: ignore
            name = field.name
            curr_schema = field.type
            prev_schema = field_schema_dict.get(name, None)
            if prev_schema is None:
                field_schema_dict[name] = curr_schema
                if field.has_default:
                    field_default_dict[name] = field.default
            else:
                field_schema_dict[name] = merge_schemas(curr_schema,
                                                        prev_schema)
                curr_default = field.default if field.has_default else None
                prev_default = field_default_dict.get(name, None)
                if prev_default is not None and curr_default != prev_default:
                    raise ValueError(
                        f'default value mismatch, cannot merge schemas.  field: {name}. curr_default: {curr_default} prev_default: {prev_default}'  # pylint: disable=line-too-long
                    )
                elif curr_default is not None:
                    field_default_dict[name] = curr_default
        merged_fields = []
        for idx, (name, schema) in enumerate(field_schema_dict.items()):
            merged_fields.append(
                avro_schema_lib.Field(type=schema,
                                      name=name,
                                      index=idx,
                                      has_default=(name in field_default_dict),
                                      default=field_default_dict.get(
                                          name, object())))
        namespace = schema1.namespace  # type: ignore
        name = schema1.name  # type: ignore
        if schema2.namespace and schema1.namespace != schema2.namespace:  # type: ignore # pylint: disable=line-too-long
            namespace += f'.{schema2.namespace}'  # type: ignore
        if schema2.name and schema1.name != schema2.name:  # type: ignore
            name += f'.{schema2.name}'  # type: ignore
        return avro_schema_lib.RecordSchema(namespace=namespace,
                                            name=name,
                                            fields=merged_fields,
                                            names=avro_schema_lib.Names())
    elif type1 == ARRAY and type2 == ARRAY:
        return avro_schema_lib.ArraySchema(
            merge_schemas(schema1.items, schema2.items))  # type: ignore
    elif type1 == UNION and type2 == UNION:
        union_types = {}
        schema1_records = {}
        for schema in schema1.schemas:  # type: ignore
            if schema.type == RECORD:
                schema1_records[schema.fullname] = schema
            else:
                union_types[schema.fullname] = schema

        schema2_records = {}
        for schema in schema2.schemas:  # type: ignore
            if schema.type == RECORD:
                schema2_records[schema.fullname] = schema
            else:
                union_types[schema.fullname] = schema

        keys = set(schema1_records.keys()) | set(schema2_records.keys())
        for key in keys:
            if key in schema1_records and key in schema2_records:
                union_types[key] = merge_schemas(schema1_records[key],
                                                 schema2_records[key])
            elif key in schema1_records:
                union_types[key] = schema1_records[key]
            else:
                union_types[key] = schema2_records[key]

        if 'string' in union_types and len(union_types) > 2:
            # Remove the default string.
            # String gets added when we can't determine the actually type.
            # But since a record is present we can remove the default.
            del union_types['string']
        return avro_schema_lib.UnionSchema(list(union_types.values()))
    else:
        if type1 in NON_PRIMITIVE_TYPES or type2 in NON_PRIMITIVE_TYPES:
            raise ValueError(
                f'type mismatch, cannot merge schemas: type1: {type1} type2: '
                f'{type2}')
        # Primitive type
        if NULL in (type1, type2):
            return avro_schema_lib.UnionSchema([schema1, schema2])
        if type1 != type2:
            raise ValueError(
                f'type mismatch, cannot merge schemas: type1: {type1} type2: '
                f'{type2}')
        return avro_schema_lib.PrimitiveSchema(type1)


def to_json(avro_schema: avro_schema_lib.Schema) -> Dict[str, Any]:
    """Converts the avro schema to a json dictionary.

    The to_json method on the avro schema is bugged and doesn't convert
    ImmutableDicts back to normal dicts, this means you can't reparse the schema
    that is outputs.  This method simply converts the ImmutableDicts to a
    regular python dictionary.

    Args:
        avro_schema: The schema to convert to JSON.

    Returns:
        A JSON dictionary representing the schema.
    """

    def make_new_type_dict(orig_dict):
        new_type_dict = {}
        for key, value in orig_dict.items():
            new_type_dict[key] = value
        return new_type_dict

    def convert_imutable_dicts(avro_dict: Dict[str, Any]) -> Dict[str, Any]:
        for key, value in avro_dict.items():
            if isinstance(value, dict):
                avro_dict[key] = make_new_type_dict(value)
                avro_dict[key] = convert_imutable_dicts(
                    make_new_type_dict(value))
            if isinstance(value, list):
                new_list = []
                for v in value:
                    # This doesn't handle nested lists, but those shouldn't
                    # appear in an avro schema.
                    if isinstance(v, dict):
                        new_list.append(
                            convert_imutable_dicts(make_new_type_dict(v)))
                    else:
                        new_list.append(v)
                avro_dict[key] = new_list
        return avro_dict

    return convert_imutable_dicts(avro_schema.to_json(names=None))


def avro_schema_to_bq_schema(avro_schema) -> Iterable[bigquery.SchemaField]:

    def _schema_helper(
        name,
        avro_schema,
    ) -> List[bigquery.SchemaField]:

        schema_type = avro_schema.type

        if schema_type == ARRAY:
            avro_items = avro_schema.items
            if avro_items.type == RECORD:
                return [
                    bigquery.SchemaField(name,
                                         'RECORD',
                                         'REPEATED',
                                         fields=_schema_helper(
                                             None, avro_items))
                ]
            elif avro_items.type == UNION:
                union_schema = _schema_helper(None, avro_items)
                if len(union_schema) > 1:
                    raise ValueError('multiple types not supported for lists.')
                item_type = union_schema[0].field_type
            else:
                item_type = AVRO_TYPE_TO_BQ_TYPE[avro_items.type]
            return [bigquery.SchemaField(name, item_type, 'REPEATED')]
        elif schema_type == UNION:
            union_schemas = avro_schema.schemas
            if len(union_schemas) != 2:
                raise ValueError(
                    f'only nullable unions are supported: {union_schemas}')
            schema1 = union_schemas[0]
            schema2 = union_schemas[1]
            if schema1.type == 'null':
                other_schema = schema2
            elif schema2.type == 'null':
                other_schema = schema1
            else:
                raise ValueError(
                    f'only nullable unions are supported: {union_schemas}')
            if other_schema.type == RECORD:
                return [
                    bigquery.SchemaField(name,
                                         'RECORD',
                                         'NULLABLE',
                                         fields=_schema_helper(
                                             None, other_schema))
                ]
            elif other_schema.type == ARRAY:
                return _schema_helper(name, other_schema)
            return [
                bigquery.SchemaField(name,
                                     AVRO_TYPE_TO_BQ_TYPE[other_schema.type],
                                     'NULLABLE')
            ]

        elif schema_type == RECORD:
            sub_fields: List[bigquery.SchemaField] = []
            for field in avro_schema.fields:
                sub_fields.extend(_schema_helper(field.name, field.type))
            if name is None:
                return sub_fields
            return [bigquery.SchemaField(name, 'RECORD', fields=sub_fields)]
        else:
            # Primitive types
            bq_type = AVRO_TYPE_TO_BQ_TYPE[schema_type]
            if 'logicalType' in avro_schema.other_props:
                if avro_schema.other_props['logicalType'] == 'timestamp-millis':
                    return [bigquery.SchemaField(name, 'TIMESTAMP')]
            return [bigquery.SchemaField(name, bq_type)]

    avro_type = avro_schema.type

    if avro_type != RECORD:
        raise ValueError('Top level avro schema should be a record.')

    bq_schema_fields: List[bigquery.SchemaField] = []

    for field in avro_schema.fields:
        bq_schema_fields.extend(_schema_helper(field.name, field.type))

    return bq_schema_fields


def data_source_avro_schema(names) -> str:  # type: ignore
    """Creates an avro schema associated with the data source proto.

    Registers the type in `names` and returns the string representation of that
    type.

    Args:
        names: Avro schema names.

    Returns:
        The full name of the created data source schema.
    """

    def avro_schema_helper(msg_descriptor):
        fields = []
        for field in msg_descriptor.fields:
            avro_type = ['null']
            default_val = _EmptyDefault()
            if field.message_type is not None:
                sub_fields = avro_schema_helper(field.message_type)
                sub_schema = avro_schema_lib.SchemaFromJSONData(  # type: ignore
                    {
                        'name': field.name,
                        'namespace': 'data_source_schema',
                        'type': 'record',
                        'fields': sub_fields,
                    }, names)
                if field.label == descriptor.FieldDescriptor.LABEL_REPEATED:
                    avro_type.append({
                        'type': 'array',
                        'items': sub_schema.fullname,
                    })
                    default_val = None
                else:
                    avro_type.append(sub_schema.fullname)
                    default_val = None
            elif field.type in _PROTO_TO_AVRO_PRIMITIVE:
                primitive_type = _PROTO_TO_AVRO_PRIMITIVE[field.type]
                if field.label == descriptor.FieldDescriptor.LABEL_REPEATED:
                    avro_type.append({'type': 'array', 'items': primitive_type})
                else:
                    avro_type.append(primitive_type)
            elif field.enum_type is not None:
                avro_type.append('int')
            field_dict = {'name': field.name, 'type': avro_type}
            if not isinstance(default_val, _EmptyDefault):
                field_dict['default'] = default_val
            fields.append(field_dict)
        return fields

    fields = avro_schema_helper(types_pb2.DataSource().DESCRIPTOR)
    schema = {
        'name': 'DataSource',
        'namespace': 'data_source_schema',
        'type': 'record',
        'fields': fields,
    }
    avro_schema = avro_schema_lib.SchemaFromJSONData(schema,
                                                     names)  # type: ignore
    return avro_schema.fullname
