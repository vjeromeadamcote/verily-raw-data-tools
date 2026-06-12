"""Custom DataPoint classes for testing."""

import dataclasses

from verily.ds_sdk.core import schemas


@dataclasses.dataclass
class CustomDataPoint(schemas.DataPoint):
    """Custom DataPoint type."""
    custom_field: int
