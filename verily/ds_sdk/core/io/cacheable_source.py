"""Utils for building a DataSource cache."""
import abc
import glob
import hashlib
import json
import os

import apache_beam as beam
from google.protobuf import json_format

from verily.ds_sdk.protos import types_pb2


class _JsonSink(beam.io.fileio.TextSink):
    """Sink for writing JSON DataSources."""

    def write(self, record):
        data_source_bytes, row = record
        d = row._asdict()

        data_source = types_pb2.DataSource()
        data_source.ParseFromString(data_source_bytes)
        d['data_source_json'] = json.loads(
            json_format.MessageToJson(data_source))

        self._fh.write(json.dumps(d).encode('utf8'))
        self._fh.write('\n'.encode('utf8'))


def _parse_json(record, schema):
    d = json.loads(record)
    data_source = json_format.ParseDict(d['data_source_json'],
                                        types_pb2.DataSource())
    del d['data_source_json']
    return (data_source.SerializeToString(), schema(**d))


class CacheablePTransform(beam.PTransform, abc.ABC):
    """PTransform that is Cachable."""

    def __init__(self,
                 disable_cache: bool = False,
                 state_file_path: str = '/tmp/ds_sdk_cached_pcol_state.json'):
        super().__init__()
        self._disable_cache = disable_cache
        self._state_file_path = state_file_path

        if not self._disable_cache:
            # Create the state file if it doesn't exist.
            if not os.path.isfile(self._state_file_path):
                with open(self._state_file_path, 'w', encoding='UTF-8') as f:
                    # Json parsing fails if the file is empty.
                    json.dump({'TODO': 'Remove This'}, f)

            with open(self._state_file_path, 'r', encoding='UTF-8') as f:
                self._state = json.load(f)

            self._remove_stale_state()

    def expand(self, pcol):
        if self._disable_cache:
            print('\nCache Disabled. Building From Source...\n')
            return self.expand_fn(pcol)

        encoded_key_str = self.get_instance_key().encode('utf-8')
        instance_key = hashlib.sha512(encoded_key_str).hexdigest()
        if instance_key in self._state:
            print('\nBuilding Source From Cache...\n')
            path = self._state[instance_key]
            result = (pcol | beam.io.fileio.MatchFiles(
                beam.io.filesystems.FileSystems.join(path, '*')) |
                      beam.io.fileio.ReadMatches() |
                      beam.FlatMap(lambda f: f.read_utf8().strip().split('\n'))
                      | beam.Map(_parse_json, schema=self.get_row_schema()))
        else:
            print('\nSource Cache Not Found. Building From Source...\n')
            path = f'/tmp/{instance_key}'
            result = self.expand_fn(pcol)
            _ = (result | beam.io.fileio.WriteToFiles(
                path=path, sink=lambda dest: _JsonSink()))
            self._state[instance_key] = path

        self._persist_state_to_disk()

        return result

    def _remove_stale_state(self):
        for instance_key, path in list(self._state.items()):
            if not glob.glob(f'{path}/*'):
                del self._state[instance_key]

        self._persist_state_to_disk()

    def _persist_state_to_disk(self):
        with open(self._state_file_path, 'w', encoding='UTF-8') as f:
            json.dump(self._state, f)

    @abc.abstractmethod
    def expand_fn(self, pcol):
        raise ValueError('expand_fn was not implemented. for: ', self.__class__)

    @abc.abstractmethod
    def get_row_schema(self):
        raise ValueError('get_row_schema was not implemented. for: ',
                         self.__class__)

    @abc.abstractmethod
    def get_instance_key(self) -> str:
        raise ValueError('get_instance_key was not implemented. for: ',
                         self.__class__)
