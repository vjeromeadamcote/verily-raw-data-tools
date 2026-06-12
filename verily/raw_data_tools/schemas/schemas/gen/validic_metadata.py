# Lint as: python3
# pylint: disable=invalid-name
"""Registers the schema for com.verily.validic.metadata with Beam."""

import dataclasses

from verily.ds_sdk.core.schemas import DataPoint
from verily.ds_sdk.core.schemas import dataspec


@dataspec('com.verily.validic.metadata')
@dataclasses.dataclass
class ValidicMetadata(DataPoint):
    """Beam RowSchema for com.verily.validic.metadata."""
    validic_checksum: str
    validic_created_at: str
    validic_log_id: str
    validic_record_id: str
    validic_record_type: str
