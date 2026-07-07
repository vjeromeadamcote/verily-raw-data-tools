"""Minimal DataSourceCache stub for the externalized raw_data_tools package.

The internal DS SDK's DataSourceCache depends on redis and protobuf
types_pb2.DataSource. This stub preserves the type interface so callers
compile, but raises NotImplementedError on any actual device-mapping query.
Timezone-aware KeyBy variants that need device mappings will fail loudly
rather than silently returning wrong timezones.
"""

from typing import Any, Dict, Optional


class DataSourceCache:
    """Stub cache for DataSource mappings.

    Accepts the same constructor signature as the internal DS SDK version
    but raises NotImplementedError when queried.  Use this as a type-compatible
    placeholder; full device-mapping support requires the internal DS SDK.
    """

    def __init__(self,
                 data_source_mappings: Optional[Dict[Optional[int], Any]] = None,
                 redis_end_point: Optional[str] = None):
        if redis_end_point is not None:
            raise NotImplementedError(
                'Redis-backed DataSourceCache is not available in '
                'verily-raw-data-tools. Use the internal DS SDK for '
                'redis-backed device-mapping support.')
        self.data_source_mappings = data_source_mappings or {}

    def get_data_source(self, data_source_id: Optional[int]) -> Any:
        raise NotImplementedError(
            'DataSourceCache.get_data_source() is not available in '
            'verily-raw-data-tools v1.0. Timezone-aware key-by transforms '
            'that require device mappings are not supported in the '
            'externalized package. Use the internal DS SDK for this '
            'functionality.')

    def get(self, data_source_id: Optional[int],
            default: Optional[Any] = None) -> Any:
        raise NotImplementedError(
            'DataSourceCache.get() is not available in '
            'verily-raw-data-tools v1.0. See get_data_source() for details.')

    def __getitem__(self, data_source_id: Optional[int]) -> Any:
        raise NotImplementedError(
            'DataSourceCache[id] is not available in '
            'verily-raw-data-tools v1.0. See get_data_source() for details.')

    def __eq__(self, other):
        if isinstance(other, self.__class__):
            return self.data_source_mappings == other.data_source_mappings
        return False

    def __repr__(self) -> str:
        return str(self.data_source_mappings)
