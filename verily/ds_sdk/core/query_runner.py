# Lint as: python3
"""Executes a provided query on BigQuery."""

import datetime
import time
from typing import Optional

import google.auth
from google.cloud import bigquery  # type: ignore


class QueryRunner(object):
    """QueryRunner executes queries on BigQuery."""

    def __init__(self, billing_project: str,
                 creds: google.auth.credentials.Credentials,
                 bigquery_location: str):
        self._client = bigquery.Client(project=billing_project,
                                       credentials=creds,
                                       location=bigquery_location)

    def launch_query_job(self,
                         query: str,
                         output_table: Optional[str] = None,
                         create_disposition: str = 'CREATE_IF_NEEDED',
                         write_disposition: str = 'WRITE_EMPTY'):
        """Launches a query job with the specified parameters."""

        if output_table is not None:
            bq_table = bigquery.Table(output_table)
            bq_table.expires = datetime.datetime.now() + datetime.timedelta(
                days=3)
            job_config = bigquery.QueryJobConfig(
                destination=output_table,
                create_disposition=create_disposition,
                write_disposition=write_disposition,
                priority=bigquery.QueryPriority.BATCH)
        else:
            job_config = bigquery.QueryJobConfig(
                priority=bigquery.QueryPriority.BATCH)

        if len(query) > 25:
            print_query = query[:25] + '...'
        else:
            print_query = query
        print(f'Executing query: {print_query}')
        return self._client.query(query, job_config=job_config)

    def execute_query(self,
                      query: str,
                      output_table: Optional[str] = None,
                      create_disposition: str = 'CREATE_IF_NEEDED',
                      write_disposition: str = 'WRITE_EMPTY'):
        """Executes the query on BigQuery and returns the row iterator."""

        query_job = self.launch_query_job(query, output_table,
                                          create_disposition, write_disposition)
        start_time = time.time()
        while not query_job.done():
            print(
                f'Query still running after: {(time.time() - start_time):.2f} seconds.'  # pylint: disable=line-too-long
            )
            time.sleep(10)
        print(
            f'Query complete after: {(time.time() - start_time):.2f} seconds.')
        return query_job.result()
