"""Container class for holding study information."""

from typing import Optional


class StudyInfo:
    """Dataclass representing information about a study."""

    def __init__(
        self,
        gcp_project: Optional[str] = None,
        internal_echo_dataset: Optional[str] = None,
        external_echo_dataset: Optional[str] = None,
        cloud_service_account: Optional[str] = None,
        registry_id: Optional[str] = None,
    ):
        self.gcp_project = gcp_project
        self.dataset = external_echo_dataset
        self.internal_echo_dataset = internal_echo_dataset
        self.external_echo_dataset = external_echo_dataset
        self.cloud_service_account = cloud_service_account
        self.registry_id = registry_id

    def __repr__(self):
        gcp_project = (f"'{self.gcp_project}'"
                       if self.gcp_project is not None else None)
        internal_echo_dataset = (f"'{self.internal_echo_dataset}'"
                                 if self.internal_echo_dataset is not None else
                                 None)
        external_echo_dataset = (f"'{self.external_echo_dataset}'"
                                 if self.external_echo_dataset is not None else
                                 None)
        cloud_service_account = (f"'{self.cloud_service_account}'"
                                 if self.cloud_service_account is not None else
                                 None)
        registry_id = (f"'{self.registry_id}'"
                       if self.registry_id is not None else None)

        return (f'StudyInfo(gcp_project={gcp_project}, '
                f'internal_echo_dataset={internal_echo_dataset}, '
                f'external_echo_dataset={external_echo_dataset}, '
                f'cloud_service_account={cloud_service_account}, '
                f'registry_id={registry_id})')
