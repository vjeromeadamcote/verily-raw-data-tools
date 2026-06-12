"""Imports shared & generated NamedTuple classes."""

from .annotation_schema import *
from .shared_schemas import *
from .decorators import dataspec
from .schema_utils import data_point_metadata_for_raw_data
from .schema_utils import data_point_metadata_for_derived_data
from .schema_utils import data_point_metadata_for_derived_data_from_df

# NOTE: This import needs to be last to ensure all dependencies of the gen
# schemas are imported.
from .gen import *

SCHEMA_CLASS_NAME_TO_DATA_SPEC_NAME = {
    schema_class.__name__: name
    for name, schema_class in DATA_SPEC_NAME_TO_SCHEMA_CLASS.items()
}
