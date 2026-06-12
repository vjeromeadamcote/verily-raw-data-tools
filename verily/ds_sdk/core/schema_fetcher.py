# Lint as: python3
"""Retrieve schemas from BigQuery and convert to an Ibis schema."""

from typing import Text

from google.auth.exceptions import RefreshError
from google.cloud import bigquery  # type: ignore
import ibis
import ibis_bigquery


class SchemaFetcher(object):
    """Fetches a schema from BigQuery and converts it to an ibis schema."""

    def __init__(self, billing_project, creds, bigquery_location):
        self._bq_client = bigquery.Client(project=billing_project,
                                          credentials=creds,
                                          location=bigquery_location)
        self._schema_cache = {}

    def fetch_schema(self, table_id: Text):
        """Fetches the schema for the provided table."""

        if table_id in self._schema_cache:
            return ibis.table(self._schema_cache[table_id],
                              table_id)  # type: ignore
        try:
            bq_table = self._bq_client.get_table(table_id)
            bq_schema = ibis_bigquery.client.bigquery_schema(
                bq_table)  # type: ignore
        except RefreshError as error:
            raise ValueError(
                ('Failed to fetch credentials.  Do you have access to the '
                 'registry you are trying to query, or the service account you '
                 f'are using? table: {table_id} error details: '
                 f'{error}')) from error
        self._schema_cache[table_id] = bq_schema
        return ibis.table(bq_schema, table_id)  # type: ignore
