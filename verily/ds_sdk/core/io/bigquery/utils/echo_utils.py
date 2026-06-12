"""Util functions for parsing Echo data."""

import copy
import re
from typing import Any, Dict, List, Union

from google.protobuf import json_format
from google.protobuf import text_format
import inflection

from verily.ds_sdk.protos import types_pb2


def to_snake_case(dictionary: Dict[str, Any]):
    """Converts the camel case keys in dictionary to snake case.

  Echo stores the columns as camel case, but the DataSource proto uses
  snake_case.
  Args:
    dictionary: The dictionary whose keys will be converted to snake case.

  Returns:
    A new dictionary containing the same values as dictionary, but with snake
    case keys.
  """
    snake_dict = {}
    for key, value in dictionary.items():
        new_key = inflection.underscore(key)
        new_value = value
        if isinstance(value, dict):
            new_value = to_snake_case(value)
        snake_dict[new_key] = new_value
    return snake_dict


def parse_field_specs(
    input_field_specs: Union[str, List[Dict[str, Any]]]
) -> List[types_pb2.DataFieldSpec]:
    """Parses a BigQuery row dictionary into types_pb2.DataFieldSpec protos."""
    if isinstance(input_field_specs, str):
        split_field_specs = input_field_specs.split(',')
        field_specs = []
        for spec in split_field_specs:
            field_spec_proto = types_pb2.DataFieldSpec()
            text_format.Parse(spec, field_spec_proto)
            field_specs.append(field_spec_proto)
        return field_specs
    elif isinstance(input_field_specs, list):
        return [
            json_format.ParseDict(field_spec, types_pb2.DataFieldSpec())
            for field_spec in input_field_specs
        ]
    else:
        raise ValueError(
            f'field specs must either be a serialized proto string or a list: {type(input_field_specs)}'  # pylint: disable=line-too-long
        )


def parse_data_source(
        echo_data_source_dict: Dict[str, Any]) -> types_pb2.DataSource:
    """Creates a DataSource proto from a json Dict stored in an EchoBQ row."""
    # We need to work with a copy because we mutate the dictionary below.
    echo_data_source_dict = copy.deepcopy(echo_data_source_dict)

    # json_format.ParseDict cannot parse the Echo field_specs. They need to be
    # parsed separately.
    data_spec = types_pb2.DataSpec()
    data_spec_dict = echo_data_source_dict.get('data_spec', None)
    if data_spec_dict is not None:
        # We may not be using data_specs going forward so this prevents a crash
        # if field_specs is not present.
        # field_specs also needs to be removed from data_spec_dict
        # before ParseDict is called.

        field_specs = []
        if 'field_specs' in data_spec_dict:
            field_specs_obj = data_spec_dict.pop('field_specs')
            if field_specs_obj is not None:
                field_specs = parse_field_specs(field_specs_obj)
        try:
            data_spec = json_format.ParseDict(data_spec_dict, types_pb2.DataSpec()) # pylint: disable=line-too-long
        except json_format.ParseError as e:
            raise ValueError(f'Could not parse DataSpec: {data_spec_dict} from echo_data_source_dict: {echo_data_source_dict}') from e # pylint: disable=line-too-long

        data_spec.field_specs.extend(field_specs)

    # TODO(b/162772644): need to fix how we parse android metadata.
    # Although I don't believe it used by anyone ATM.
    if (echo_data_source_dict.get('device', None) is not None and
            echo_data_source_dict['device'].get('android_metadata',
                                                None) is not None):
        del echo_data_source_dict['device']['android_metadata']
    if (echo_data_source_dict.get('device', None) is not None and
            echo_data_source_dict['device'].get('hardware_version',
                                                None) is not None):
        # There's some legacy tables that have the hardware version as an int.
        # Cast it to a string so it can be converted to the proto.
        echo_data_source_dict['device']['hardware_version'] = str(
            echo_data_source_dict['device']['hardware_version'])
    data_source = json_format.ParseDict(to_snake_case(echo_data_source_dict),
                                        types_pb2.DataSource())
    data_source.data_spec.CopyFrom(data_spec)
    return data_source


def get_temp_bigquery_dataset_for_location(bigquery_location: str) -> str:
    bq_region_prefix_to_dataset_name = {
        'us': 'datascience_sdk_temp',
        'europe': 'datascience_sdk_temp_eu',
    }
    bq_location_to_dataset_name = {
        'US': 'datascience_sdk_temp',
        'EU': 'datascience_sdk_temp_eu',
    }
    # case1: US | EU
    if bigquery_location in bq_location_to_dataset_name:
        return bq_location_to_dataset_name[bigquery_location]
    # case2: us-central-1 | europe-west1 | ...
    match = re.compile(r'\w+-\w+').match(bigquery_location)
    if match:
        bq_region_prefix = match.string.split('-')[0]
        if bq_region_prefix in bq_region_prefix_to_dataset_name:
            return bq_region_prefix_to_dataset_name[bq_region_prefix]
    raise ValueError(
        f'There are no temp BigQuery datasets for: {bigquery_location}')
