"""Module for what studies are configured to work with the DS SDK."""

from typing import Optional

from verily.ds_sdk.core.grpc.registry_service import \
    find_study_info_for_registry
from verily.ds_sdk.core.studies import gen_studies
from verily.ds_sdk.core.studies.study_info import StudyInfo

_ENV_TO_MAP = {
    'prod': gen_studies.REGISTRY_TO_STUDY_INFO,
    'preprod': gen_studies.REGISTRY_TO_STUDY_INFO_PREPROD,
    'autopush': gen_studies.REGISTRY_TO_STUDY_INFO_AUTOPUSH,
    'qa': gen_studies.REGISTRY_TO_STUDY_INFO_QA,
    'prod-batch': gen_studies.REGISTRY_TO_STUDY_INFO,
}

def get_study_info(registry: Optional[str], env: str = 'prod') -> StudyInfo:
    """Returns the StudyInfo associated with the registry.

    Args:
        registry: The name of the registry
        env: The sensor store environment to use.
    """

    if (registry is not None) and (registry in _ENV_TO_MAP[env]):
        return _ENV_TO_MAP[env][registry]
    else:
        return StudyInfo(
            gcp_project=None,
            internal_echo_dataset=None,
            external_echo_dataset=None,
            cloud_service_account=None,
            registry_id=None)


def get_participant_table_for_study(study_info: StudyInfo) -> str:
    return f'{study_info.gcp_project}.{study_info.external_echo_dataset}.participant_associations'  # pylint: disable=line-too-long

def get_participant_table(registry: str, env: str = 'prod') -> str:
    return get_participant_table_for_study(get_study_info(registry, env))
