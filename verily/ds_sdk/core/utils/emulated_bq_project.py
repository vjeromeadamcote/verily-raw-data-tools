'''Class for creating an emuated BQ project based on a given schema.'''
import os
from typing import Dict, List, Optional, Tuple
import warnings

from google.api_core.client_options import ClientOptions
import google.auth
from google.auth import impersonated_credentials
from google.auth.credentials import AnonymousCredentials
from google.cloud import bigquery
from google.cloud.bigquery import Client
import google.cloud.bigquery_storage_v1 as bq_storage


class EmulatedBQProject():
    '''An emulated BigQuery project based on inputted JSON schema. Interacts
    directly with the BigQuery Emulator. Thus, the BigQuery Emulator must
    already be spun up in order create an emulated project.

    JSON table schema is generated using the `generate_table_schemas_as_json.py`
    script.

    Args:
        project_id: Project to be emulated, i.e. `sensors-studies-devteam`.
        service_account: Defaults to application default credentials;
                         is optional.
        emulator_port: Port of where the BQ Emulator is to be spun up.
                       Tells emulated clients where to "look". Defaults to 9050.
                       Should be the same port that the emulator manager uses.
        input_json_schema_directory: Path to where JSON table schemas reside.
                                     Defaults to 'table_schemas_json'.

    Attributes:
        project_id, service_account, emulator_endpoint,
        input_json_schema_directory, bq_client, emulated_client,
        emulated_read_client, datasets_and_tables.
    '''

    def __init__(
            self,
            project_id: str,
            service_account: Optional[str] = None,
            emulator_port: int = 9050,
            input_json_schema_directory: str = 'table_schemas_json') -> None:
        self.project_id = project_id
        self.service_account = service_account
        self.emulator_endpoint = f'http://0.0.0.0:{emulator_port}'
        self.input_json_schema_directory = input_json_schema_directory
        warnings.warn(
            'The EmulatedBQProject class is not protected by testing due to '
            'dependency conflicts. Do not use this class for any critical '
            'testing or production applications.')

    def initialize_project(self) -> None:
        '''Initializes the project by creating a real and emulated clients,
        datasets, and tables.'''

        self.bq_client = self._get_bigquery_client_for_project()
        self.emulated_client, self.emulated_read_client = (
            self._create_emulated_clients())
        self.datasets_and_tables = self._create_all_datasets_and_tables(
            self.input_json_schema_directory)

    def _create_emulated_clients(
            self) -> Tuple[Client, bq_storage.BigQueryReadClient]:
        '''Creates emulated clients that point to where the BigQuery Emulator
        lives. Returns the clients as a Tuple.

        Returns:
            emulated_client, emulated_read_client: Emulated Bigquery client,
            emulated BigQueryReadClient.
        '''

        client_options = ClientOptions(api_endpoint=self.emulator_endpoint)
        emulated_client = Client(project=self.project_id,
                                 client_options=client_options,
                                 credentials=AnonymousCredentials())
        emulated_read_client = bq_storage.BigQueryReadClient(
            client_options=client_options, credentials=AnonymousCredentials())

        return emulated_client, emulated_read_client

    def _get_bigquery_client_for_project(self) -> bigquery.Client:
        '''Creates a BQ Client for the project.
        If no service account is provided, uses application default credentials.

        Returns:
            client: An authenticated BigQuery client.
        '''

        bigquery_creds, _ = google.auth.default()
        if self.service_account:
            bigquery_creds = impersonated_credentials.Credentials(
                source_credentials=bigquery_creds,
                target_principal=self.service_account,
                target_scopes=[
                    'https://www.googleapis.com/auth/cloud-platform'
                ])

        client = bigquery.Client(project=self.project_id,
                                 credentials=bigquery_creds)

        return client

    def _get_table_schema_files_paths(self, directory: str) -> List[str]:
        '''Gets the file path of each table schema JSON file that is
        outputted by the `generate_table_schemas_as_json.py` script.

        Args:
            directory: Path to the directory where JSON schema files live.

        Returns:
            file_paths: List of string file names in the format:
                `verily/ds_sdk/core/utils/table_schemas_json_directory/
                project_id:dataset_id.table_id.json`
        '''

        file_paths = []
        for file_name in os.listdir(directory):
            file_path = os.path.join(directory, file_name)
            if os.path.isfile(file_path):
                file_paths.append(file_path)

        return file_paths

    def _create_all_datasets_and_tables(
            self, input_schema_directory: str) -> Dict[str, List[str]]:
        '''Initializes the emulated project by creating all datasets and tables
        using the JSON schemas.

        Args:
            input_schema_directory: Path to the directory where JSON schema
            files live.

        Returns:
            created_datasets_and_tables: A dictionary keyed by dataset id
                                         with a list of table ids as values.
        '''

        schema_file_paths = self._get_table_schema_files_paths(
            input_schema_directory)

        created_datasets_and_tables = {}
        for file_path in schema_file_paths:
            temp = file_path.split('/')[-1].split(':')[1].split('.')
            dataset_id = temp[0]
            table_id = temp[1]
            full_table_id = self.project_id + '.' + dataset_id + '.' + table_id

            # don't create dataset if already created
            if dataset_id not in created_datasets_and_tables:
                self.emulated_client.create_dataset(dataset_id)
                created_datasets_and_tables[dataset_id] = [table_id]
            else:
                created_datasets_and_tables[dataset_id].append(table_id)

            # convert json schema to schema fields to create table
            # don't need to check if already created because each file is a
            # unique table
            schema_fields = self.emulated_client.schema_from_json(file_path)

            table = bigquery.Table(full_table_id, schema=schema_fields)
            self.emulated_client.create_table(table)

        return created_datasets_and_tables
