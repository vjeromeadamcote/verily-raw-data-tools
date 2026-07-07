"""Wrapper for credentials used by raw_data_tools.

Vendored from the DS SDK credentials module with internal-only scopes
(SensorStore) removed. Uses only standard Google Cloud auth APIs
(google.auth.default, impersonated_credentials) — no internal endpoints.
"""

import logging
from typing import Any, Optional, Tuple, Union

import apache_beam as beam
from apache_beam.utils import interactive_utils
import google.auth
from google.auth import impersonated_credentials

from verily.raw_data_tools.pipeline import runner_utils

RAW_DATA_TOOLS_SCOPES = [
    'https://www.googleapis.com/auth/bigquery',
    'https://www.googleapis.com/auth/cloud-platform',
]


class RawDataToolsCredentials():
    """Wrapper class for credentials used by raw_data_tools."""

    def __init__(self, runner: Union[str, beam.runners.PipelineRunner],
                 service_account: str, billing_project: str):
        self._creds = None
        self._runner = runner
        self._service_account = service_account
        del billing_project

    def get_credentials(self) -> Tuple[Any, Optional[str]]:
        if self._creds is not None:
            return self._creds, None
        if interactive_utils.is_in_notebook():
            logging.warning('auth is only supported in managed notebooks. See:'
                            ' https://cloud.google.com/vertex-ai/docs/workbench/managed/create-instance')
        if runner_utils.is_dataflow_runner(self._runner):
            logging.info('fetching user credentials using application '
                         'default creds for dataflow.')
            credentials, _ = google.auth.default()
            return credentials, None
        else:
            logging.info('fetching user credentials using application '
                         'default creds for direct runner.')
            self._creds, _ = google.auth.default()
            return self._creds, None

    def get_impersonated_credentials(self) -> Any:
        creds, _ = self.get_credentials()
        if interactive_utils.is_in_notebook():
            return creds
        impersonated_creds = impersonated_credentials.Credentials(
            source_credentials=creds,
            target_principal=self._service_account,
            target_scopes=RAW_DATA_TOOLS_SCOPES)
        return impersonated_creds
