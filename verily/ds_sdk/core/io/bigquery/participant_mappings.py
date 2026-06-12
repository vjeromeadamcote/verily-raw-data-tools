"""Helper transform for building Participant Mappings."""
import logging
from typing import Any, Dict, Iterable, List, NamedTuple, Optional, Tuple

import apache_beam as beam
from apache_beam.utils.timestamp import Timestamp

from verily.ds_sdk.core.gcp import bigquery_source_wrapper
from verily.ds_sdk.core.gcp import credentials
from verily.ds_sdk.core.io.bigquery import build_row_filters
from verily.ds_sdk.core.utils import timestamps
from verily.ds_sdk.protos import management_resources_pb2


class ParticipantInfo(NamedTuple):
    device_id: str
    participant_id: str
    participant_namespace: int
    start_timestamp: Timestamp
    end_timestamp: Timestamp


def get_participant_info_at_timestamp(
    device_id: str, measurement_timestamp: Timestamp,
    device_participant_associations: List[ParticipantInfo]
) -> Optional[ParticipantInfo]:
    """Searches all device associations for the participant at the timestamp."""
    for association in device_participant_associations:
        if association.start_timestamp <= measurement_timestamp < association.end_timestamp:  # pylint: disable=line-too-long
            if association.device_id != device_id:
                raise ValueError(
                    'Encountered association that does not match target '
                    f'device: {device_id} != {association}')
            return association
    logging.warning('Could not find participant association for %s @ %s',
                    device_id, measurement_timestamp)
    return None


def get_participant_info_within_time_range(
    device_id: str, start_timestamp: Timestamp, end_timestamp: Timestamp,
    device_participant_associations: List[ParticipantInfo]
) -> Optional[List[ParticipantInfo]]:
    """Searches all device associations for the participant within the range."""

    device_participant_associations.sort(key=lambda pa: pa.start_timestamp)

    def is_overlapping(a_start: Timestamp, a_end: Timestamp, b_start: Timestamp,
                       b_end: Timestamp) -> bool:
        latest_start = max(a_start, b_start)
        earliest_end = min(a_end, b_end)
        # Two ranges overlap if the earliest end is greater than the latest
        # start.
        return earliest_end > latest_start

    candidate_association: List[ParticipantInfo] = []
    for association in device_participant_associations:
        if is_overlapping(start_timestamp, end_timestamp,
                          association.start_timestamp,
                          association.end_timestamp):
            if association.device_id != device_id:
                raise ValueError(
                    'Encountered participant association that does not '
                    f'match target device: {device_id} != {association}')
            if len(candidate_association) > 0:
                association_diff = (
                    association.start_timestamp.seconds() -
                    candidate_association[-1].end_timestamp.seconds())
                if (association_diff <= 60 and association.participant_id
                        == candidate_association[-1].participant_id):
                    # This can happen when a device is unassigned and reassigned
                    # between data being reported. The annotation generation
                    # code sees no gap in data so it assumes the watch was still
                    # being worn (which it likely was). So ignore this case and
                    # just log a warning.
                    logging.warning(
                        'Encounted multiple particpant associations for '
                        '%s that were less than one minute apart for '
                        'the same participant.', device_id)
                    candidate_association[-1] = association
                    continue
                elif (association_diff < 0 and association.participant_id !=
                      candidate_association[-1].participant_id):
                    raise ValueError(
                        f'Encountered overlapping participant associations for '
                        f'{device_id} between {start_timestamp} -> '
                        f'{end_timestamp}')

            candidate_association.append(association)

    if len(candidate_association) == 0:
        logging.warning(
            'Could not find participant association for %s between %s -> %s',
            device_id, start_timestamp, end_timestamp)
        return None
    return candidate_association


def _parse_bigquery_rows(
        bigquery_row: Dict[str, Any]) -> Tuple[str, ParticipantInfo]:
    participant_info = ParticipantInfo(
        bigquery_row['DeviceId'], bigquery_row['ParticipantId'],
        management_resources_pb2.Participant.ParticipantNamespace.Value(
            bigquery_row['ParticipantNamespace']),
        timestamps.parse_bigquery_timestamp(bigquery_row['StartTime']),
        timestamps.parse_bigquery_timestamp(bigquery_row['EndTime']))
    return (bigquery_row['DeviceId'], participant_info)


def _merge_consecutive_rows(
    participant_associations: Tuple[str, Iterable[ParticipantInfo]]
) -> Tuple[str, Iterable[ParticipantInfo]]:
    device_id, participant_infos = participant_associations
    participant_keyed_infos: Dict[str, List[ParticipantInfo]] = {}
    for info in participant_infos:
        if info.participant_id in participant_keyed_infos:
            participant_keyed_infos[info.participant_id].append(info)
        else:
            participant_keyed_infos[info.participant_id] = [info]
    merged_infos = []
    for _, participant_infos in participant_keyed_infos.items():
        participant_infos = sorted(participant_infos,
                                   key=lambda info: info.start_timestamp.micros)
        prev_info = None
        for info in participant_infos:
            if prev_info is None:
                prev_info = info
            else:
                if info.start_timestamp == prev_info.end_timestamp:
                    prev_info = ParticipantInfo(
                        device_id=prev_info.device_id,
                        participant_id=prev_info.participant_id,
                        participant_namespace=prev_info.participant_namespace,
                        start_timestamp=prev_info.start_timestamp,
                        end_timestamp=info.end_timestamp)
                else:
                    merged_infos.append(prev_info)
                    prev_info = info
        if prev_info is not None:
            merged_infos.append(prev_info)
    return (device_id, merged_infos)


class BuildParticipantMappings(beam.PTransform):
    """Transform for building participant mappings as a PCollection."""

    def __init__(self, bigquery_table_id: str, project_id: str,
                 service_account: str, creds: credentials.DsSdkCredentials,
                 bigquery_location: str):
        super().__init__()
        self._bigquery_table_id = bigquery_table_id
        self._project_id = project_id
        self._service_account = service_account
        self._creds = creds
        self._bigquery_location = bigquery_location

    def expand(self,
               pcol) -> beam.PCollection[Tuple[str, Iterable[ParticipantInfo]]]:
        return (
            pcol |
            f'Read Participant Mappings: {self._bigquery_table_id}, {self._project_id}'  # pylint: disable=line-too-long
            >> bigquery_source_wrapper.GcpBigquerySourceWrapper(
                # Passes through an empty row filter: (bq_table_id, None)
                row_filter_builder=build_row_filters.PassThroughRowFilter(
                    (self._bigquery_table_id, None), self._creds,
                    self._project_id, self._bigquery_location),
                creds=self._creds,
                project_id=self._project_id,
                service_account=self._service_account) |
            beam.Map(_parse_bigquery_rows) | beam.GroupByKey() |
            beam.Map(_merge_consecutive_rows))
