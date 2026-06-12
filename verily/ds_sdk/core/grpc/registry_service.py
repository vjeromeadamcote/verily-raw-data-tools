"""gRPC calls to call the Sensorsuite private API"""

from typing import List

import google.auth
import google.auth.transport.grpc
import google.auth.transport.requests

from verily.ds_sdk.core.studies.study_info import StudyInfo
from verily.ds_sdk.protos import management_resources_pb2
from verily.ds_sdk.protos import management_service_pb2
from verily.ds_sdk.protos import management_service_pb2_grpc

_PORT_NUMBER = '443'
_ENV_TO_API_ENDPOINT = {
    'prod':
        f'sensorsuite-pa.googleapis.com:{_PORT_NUMBER}',
    'preprod':
        f'preprod-sensorsuite-pa.sandbox.googleapis.com:{_PORT_NUMBER}',
    'autopush':
        f'autopush-sensorsuite-pa.sandbox.googleapis.com:{_PORT_NUMBER}',
    'qa':
        f'qa-sensorsuite-pa.sandbox.googleapis.com:{_PORT_NUMBER}',
    'prod-batch':
        f'sensorsuite-pa.googleapis.com:{_PORT_NUMBER}',
}

def list_registries(
        env: str = 'prod') -> List[management_resources_pb2.Registry]:
    """Makes a gRPC call to the Sensorsuite private API to list the registries.

    Args:
        env: The environment to make the gRPC call with.

    Returns:
        The list of registries from the gRPC call.
    """
    creds, _ = google.auth.default(
        scopes=['https://www.googleapis.com/auth/lifescience.sensorsuite'])
    channel_url = _ENV_TO_API_ENDPOINT[env]
    channel = google.auth.transport.grpc.secure_authorized_channel(
        creds,
        google.auth.transport.requests.Request(),
        channel_url
    )
    stub = management_service_pb2_grpc.ManagementServiceStub(channel)

    request = management_service_pb2.ListRegistriesRequest()
    response = stub.ListRegistries(request)

    return response.registries

def verify_registry_fields_for_study_info(
        registry: management_resources_pb2.Registry) -> bool:
    """Verifies that the registry has all the required fields.

    Args:
        registry: The registry to verify the fields for,

    Returns:
        Boolean indicating if the registry has all the required fields.
    """
    policy = getattr(registry, 'policy', False)
    if not policy or not hasattr(policy, 'data_export_configs'):
        return False

    # this cannot be set to default False due to type checking errors
    export_configs = getattr(policy, 'data_export_configs')
    if export_configs:
        export_config = export_configs[0]
        big_query_export_config = getattr(
            export_config, 'big_query_export_config', False)
        gcp_project = getattr(
            big_query_export_config, 'google_cloud_project_id', False)
        dataset = getattr(big_query_export_config, 'dataset_name', False)
        return all([export_config, big_query_export_config,
                    gcp_project, dataset])

    # if there is no export_configs, return false as well
    return False

def format_registry_into_study_info(
        registry: management_resources_pb2.Registry,
        registry_name: str) -> StudyInfo:
    """Formats the registry into a StudyInfo object.

    Args:
        registry: The registry to verify the fields for,
        registry_name: Formatted name that replaced spaces with underscores.

    Returns:
        Boolean indicating if the registry has all the required fields.
    """
    if not verify_registry_fields_for_study_info(registry):
        raise AttributeError(
            f'Attributes missing in the \
                registry with the name: {registry_name}')

    policy = getattr(registry, 'policy')
    export_configs = getattr(policy, 'data_export_configs')
    export_config = export_configs[0]

    registry_id = getattr(registry, 'name').replace('registries/', '')
    big_query_export_config = getattr(
        export_config, 'big_query_export_config')
    gcp_project = getattr(
        big_query_export_config, 'google_cloud_project_id')
    dataset = getattr(big_query_export_config, 'dataset_name')
    if dataset.startswith('sensor_store_'):
        internal_dataset = dataset.replace(
            'sensor_store_', 'internal_')
    else:
        internal_dataset = 'internal_' + dataset

    return StudyInfo(
        gcp_project=gcp_project,
        internal_echo_dataset=internal_dataset,
        external_echo_dataset=dataset,
        cloud_service_account=
        f'projects/-/serviceAccounts/ds-sdk-readers@{gcp_project}.iam.gserviceaccount.com', # pylint: disable=line-too-long
        registry_id=registry_id)

def find_study_info_for_registry(registry_name: str, env: str) -> StudyInfo:
    """Finds the study info for the registry name.

    Args:
    registry_name: The name of the registry to find the study info for.
    env: The environment to make the gRPC call with.

    Returns:
    The study info given the registry name.
    """
    registries = list_registries(env)

    for registry in registries:
        name = getattr(registry, 'display_name').replace(' ', '_')
        if name == registry_name:
            return format_registry_into_study_info(registry, name)

    raise ValueError(
        f'Unable to find registry with the registry name of {registry_name}')
