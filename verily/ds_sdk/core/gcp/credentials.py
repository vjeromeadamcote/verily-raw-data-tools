"""Wrapper for credentials used by the sandwich DS SDK."""

import logging
from typing import Any, Optional, Tuple, Union

import apache_beam as beam
from apache_beam.utils import interactive_utils
import google.auth
from google.auth import impersonated_credentials

from verily.ds_sdk.core.utils import runner_utils

DS_SDK_SCOPES = [
    'https://www.googleapis.com/auth/bigquery',
    'https://www.googleapis.com/auth/cloud-platform',
]
SENSOR_STORE_SCOPES = 'https://www.googleapis.com/auth/lifescience.sensorstore'


class DsSdkCredentials():
    """Wrapper class for credentials used by the DS SDK."""

    def __init__(self, runner: Union[str, beam.runners.PipelineRunner],
                 service_account: str, billing_project: str):
        self._creds = None
        self._runner = runner
        self._service_account = service_account
        # Billing project isn't used on GCP, but we pass them in so the
        # interface is consistent between google3 and GCP.
        del billing_project

    def get_credentials(self) -> Tuple[Any, Optional[str]]:
        if self._creds is not None:
            return self._creds, None
        if interactive_utils.is_in_notebook():
            logging.warning('auth is only supported in managed notebooks. See:'
                            ' https://cloud.google.com/vertex-ai/docs/workbench/managed/create-instance')  # pylint: disable=line-too-long
        if runner_utils.is_dataflow_runner(self._runner):
            logging.info('fetching user credentials using application '
                         'default creds for dataflow.')
            # Don't cache credentials when running on Dataflow.
            # This forces each worker to fetch new credentials.
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
            # Don't impersonate creds if we're in a notebook. This is done
            # because end-user-creds are used for reading when in a notebook
            # and we want all auth to be consistent.
            return creds
        impersonated_creds = impersonated_credentials.Credentials(
            source_credentials=creds,
            target_principal=self._service_account,
            target_scopes=[SENSOR_STORE_SCOPES] + DS_SDK_SCOPES)
        return impersonated_creds
