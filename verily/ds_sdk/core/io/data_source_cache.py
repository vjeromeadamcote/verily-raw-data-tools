"""DataSourceCache container class."""

from typing import Any, Dict, Optional

import frozendict
import redis

from verily.ds_sdk.protos import types_pb2


def _parse_data_source_bytes(b: bytes) -> types_pb2.DataSource:
    data_source = types_pb2.DataSource()
    data_source.ParseFromString(b)
    return data_source


class DataSourceCache(object):
    """Cache for storing DataSource mappings."""

    def __init__(self,
                 data_source_mappings: Dict[Optional[int],
                                            types_pb2.DataSource],
                 redis_end_point: Optional[str] = None):
        self.data_source_mappings = frozendict.FrozenOrderedDict(
            data_source_mappings)

        self.redis_client = None
        if redis_end_point is not None:
            host, port = redis_end_point.split(':')
            self.redis_client = redis.Redis(host=host, port=int(port))

    def get_data_source(self,
                        data_source_id: Optional[int]) -> types_pb2.DataSource:
        data_source = self.data_source_mappings.get(data_source_id, None)
        # Lookup DataSource in Redis if not found locally.
        if data_source is None and self.redis_client is not None:
            data_source_bytes = self.redis_client.get(
                data_source_id)  # type: ignore
            if data_source_bytes is not None:
                data_source = _parse_data_source_bytes(data_source_bytes)
                self.data_source_mappings = self.data_source_mappings.set(
                    data_source_id, data_source)

        if data_source is None:
            raise RuntimeError(
                f'Cache does not contain a DataSource for {data_source_id}')
        return data_source

    def get(self,
            data_source_id: Optional[int],
            default: Optional[Any] = None) -> Any:
        try:
            return self.get_data_source(data_source_id)
        except RuntimeError:
            return default

    def __getitem__(self,
                    data_source_id: Optional[int]) -> types_pb2.DataSource:
        return self.get_data_source(data_source_id)

    def __eq__(self, other):
        if isinstance(other, self.__class__):
            return self.data_source_mappings == other.data_source_mappings
        return False

    def __repr__(self) -> str:
        return str(self.data_source_mappings)
