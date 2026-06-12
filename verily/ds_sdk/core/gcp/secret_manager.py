"""Client for retrieving secrets from secret manager."""

from google.cloud import secretmanager


class SecretManagerClient():
    """Client for fetching secrets from SecretManager."""

    def __init__(self):
        self._secret_client = secretmanager.SecretManagerServiceClient()

    def _get_secret(self, name: str, project: str = 'datascience-sdk'):
        secret_version_path = (
            f'projects/{project}/secrets/{name}/versions/latest')

        response = self._secret_client.access_secret_version(
            name=secret_version_path)

        return response.payload.data.decode('UTF-8')

    def sensor_store_api_key(self, project: str) -> str:
        if not project:
            return self._get_secret('ds-sdk-api-key')
        return self._get_secret(name='ds-sdk-api-key', project=project)

    def oauth_client_id(self) -> str:
        return self._get_secret('ds-sdk-client-id')

    def oauth_client_secret(self) -> str:
        return self._get_secret('ds-sdk-client-secret')
