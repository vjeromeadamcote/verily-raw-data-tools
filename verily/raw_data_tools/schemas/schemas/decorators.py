"""Decorator for assigning dataspecs to a NamedTuple schema object."""

import importlib
from typing import Type

from verily.raw_data_tools.schemas.schemas import schema_utils
from verily.raw_data_tools.schemas.schemas import shared_schemas

_GEN_SCHEMA_MODULE = 'verily.raw_data_tools.schemas.schemas.gen'


def dataspec(data_spec_name: str):

    def validate_data_spec_class(
            input_schema_class: Type[shared_schemas.DataPointType]):
        """Decorator for assigning dataspecs to a NamedTuple schema object."""

        input_schema_module = input_schema_class.__module__
        # Modules in the gen module are generated directly from SensorStore so
        # no need to validate them.
        if not input_schema_module.startswith(_GEN_SCHEMA_MODULE):
            schema_utils.validate_required_fields(input_schema_class)

            # TODO(dyke): this should really validate that the types are
            # consistent for each property as well.
            input_schema_properties = schema_utils.get_schema_fields(
                input_schema_class)

            sensors_data_spec_class_name = schema_utils.get_class_name(
                data_spec_name)

            gen_module = importlib.import_module(_GEN_SCHEMA_MODULE)

            sensors_data_spec_class = getattr(gen_module,
                                              sensors_data_spec_class_name)

            sensors_data_spec_properties = schema_utils.get_schema_fields(
                sensors_data_spec_class)

            missing_from_input = []
            for sensors_prop in sensors_data_spec_properties:
                if sensors_prop not in input_schema_properties:
                    missing_from_input.append(sensors_prop)

            missing_from_sensors = []
            for input_schema_prop in input_schema_properties:
                if input_schema_prop not in sensors_data_spec_properties:
                    missing_from_sensors.append(input_schema_prop)

            if missing_from_input:
                raise ValueError(
                    'Input schema was missing fields that are present on the '
                    'data spec. Ensure your schema has all fields that are on '
                    f'the data spec. Missing fields: {missing_from_input}')

            if missing_from_sensors:
                raise ValueError(
                    'Sensors data spec was missing fields that are present on '
                    'the input schema. If you added additional fields make sure'
                    ' you update the data spec in go/sensordataspec and '
                    'regenerate the beam schemas. Extra fields: '
                    f'{missing_from_sensors}')

        # Attach the data spec name to the schema class. This allows the
        # SensorStore sink to know what data spec corresponds to this class.
        input_schema_class.data_spec_from_decorator = data_spec_name  # type: ignore # pylint: disable=line-too-long
        return input_schema_class

    return validate_data_spec_class
