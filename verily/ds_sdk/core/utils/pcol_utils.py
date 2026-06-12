"""Utils for extracting metadata from PCollections."""

from typing import Any
import uuid

import apache_beam as beam

from verily.ds_sdk.core import schemas


def get_data_spec_name_from_pcol(pcoll: beam.PCollection[Any],
                                 default: str = uuid.uuid4().hex):
    if pcoll.element_type is None:
        return default
    return schemas.SCHEMA_CLASS_NAME_TO_DATA_SPEC_NAME.get(
        pcoll.element_type.__name__, default)
