"""Simplified I/O interface for reading sensor data from BigQuery.

This module provides a streamlined interface for Workbench users to read
sensor data from BigQuery without requiring internal Verily infrastructure.
"""

import logging
from typing import List, Optional, Union
import uuid

import apache_beam as beam
from google.cloud import bigquery
import pandas as pd

from verily.raw_data_tools import conditions
from verily.raw_data_tools.io import data_filters
from verily.raw_data_tools.pipeline import options as pipeline_options
from verily.raw_data_tools.pipeline import runner_utils

logging.getLogger().setLevel(logging.INFO)


class RawDataIO:
    """Main I/O interface for reading and processing sensor data from BigQuery.

    This class provides a simplified interface for Workbench users to:
    - Read DataPoints from BigQuery tables
    - Filter and deduplicate data
    - Build Apache Beam pipelines for data processing

    Example:
        >>> io = RawDataIO(
        ...     project='my-gcp-project',
        ...     dataset='my_sensor_dataset',
        ...     runner='DataflowRunner'
        ... )
        >>>
        >>> pipeline = io.create_pipeline()
        >>> data = pipeline | io.read_datapoints(
        ...     table='datapoint',
        ...     device_ids=['device1', 'device2'],
        ...     start_time='2024-01-01',
        ...     end_time='2024-01-31'
        ... )
    """

    def __init__(
        self,
        *,
        project: str,
        dataset: str,
        runner: Union[str, beam.runners.PipelineRunner] = 'DirectRunner',
        dataflow_options: Optional[pipeline_options.DataflowOptions] = None,
        bigquery_location: str = 'US',
    ):
        """Initialize RawDataIO.

        Args:
            project: GCP project ID containing the BigQuery dataset
            dataset: BigQuery dataset name containing sensor data tables
            runner: Beam runner to use ('DirectRunner', 'DataflowRunner', etc.)
            dataflow_options: Configuration for Dataflow runner (required if runner='DataflowRunner')
            bigquery_location: BigQuery dataset location (default: 'US')
        """
        self.project = project
        self.dataset = dataset
        self.bigquery_location = bigquery_location
        self._runner = runner
        self._pipeline_options = {}

        # Configure Dataflow if needed
        self.is_dataflow_job = False
        if runner_utils.is_dataflow_runner(runner):
            self.is_dataflow_job = True
            if dataflow_options is None:
                raise ValueError(
                    'dataflow_options is required when runner="DataflowRunner"'
                )

            if not dataflow_options.job_name:
                dataflow_options.job_name = f'raw_data_tools_{uuid.uuid4().hex[:8]}'

            self._pipeline_options = dataflow_options.to_pipeline_options()

            # Set project in pipeline options
            if 'project' not in self._pipeline_options:
                self._pipeline_options['project'] = project

        self._bq_client = bigquery.Client(project=project, location=bigquery_location)

    def create_pipeline(self, name: Optional[str] = None) -> beam.Pipeline:
        """Create an Apache Beam pipeline.

        Args:
            name: Optional pipeline name

        Returns:
            Apache Beam Pipeline object
        """
        pipeline_name = name or f'raw_data_tools_pipeline_{uuid.uuid4().hex[:8]}'
        return beam.Pipeline(
            runner=self._runner,
            options=beam.pipeline.PipelineOptions(**self._pipeline_options),
        )

    def read_datapoints(
        self,
        table: str = 'datapoint',
        device_ids: Optional[List[str]] = None,
        start_time: Optional[Union[str, pd.Timestamp]] = None,
        end_time: Optional[Union[str, pd.Timestamp]] = None,
        data_types: Optional[List[str]] = None,
        limit: Optional[int] = None,
    ) -> beam.PTransform:
        """Create a Beam transform to read DataPoints from BigQuery.

        Args:
            table: BigQuery table name (default: 'datapoint')
            device_ids: List of device IDs to filter on
            start_time: Start time for data query (ISO format or pandas Timestamp)
            end_time: End time for data query (ISO format or pandas Timestamp)
            data_types: List of data types to filter on (e.g., ['IMU', 'PPG'])
            limit: Maximum number of rows to return

        Returns:
            Apache Beam PTransform that outputs DataPoint records

        Example:
            >>> pipeline = io.create_pipeline()
            >>> data = pipeline | io.read_datapoints(
            ...     device_ids=['device1'],
            ...     start_time='2024-01-01',
            ...     end_time='2024-01-31',
            ...     data_types=['IMU']
            ... )
        """
        # Build query conditions
        query_conditions = []

        if device_ids:
            device_list = "', '".join(device_ids)
            query_conditions.append(f"DeviceID IN ('{device_list}')")

        if start_time:
            if isinstance(start_time, str):
                start_time = pd.Timestamp(start_time)
            start_millis = int(start_time.timestamp() * 1000)
            query_conditions.append(f"DataPointTime >= {start_millis}")

        if end_time:
            if isinstance(end_time, str):
                end_time = pd.Timestamp(end_time)
            end_millis = int(end_time.timestamp() * 1000)
            query_conditions.append(f"DataPointTime <= {end_millis}")

        if data_types:
            type_list = "', '".join(data_types)
            query_conditions.append(f"DataPoint.data_type IN ('{type_list}')")

        # Build full query
        where_clause = " AND ".join(query_conditions) if query_conditions else "TRUE"
        limit_clause = f"LIMIT {limit}" if limit else ""

        query = f"""
            SELECT *
            FROM `{self.project}.{self.dataset}.{table}`
            WHERE {where_clause}
            {limit_clause}
        """

        logging.info(f"Reading from BigQuery with query:\n{query}")

        return beam.io.ReadFromBigQuery(
            query=query,
            use_standard_sql=True,
            project=self.project,
        )

    def get_table_schema(self, table: str) -> bigquery.Schema:
        """Get the schema for a BigQuery table.

        Args:
            table: Table name

        Returns:
            BigQuery table schema
        """
        table_ref = f"{self.project}.{self.dataset}.{table}"
        table_obj = self._bq_client.get_table(table_ref)
        return table_obj.schema

    def list_tables(self) -> List[str]:
        """List all tables in the dataset.

        Returns:
            List of table names
        """
        dataset_ref = f"{self.project}.{self.dataset}"
        tables = self._bq_client.list_tables(dataset_ref)
        return [table.table_id for table in tables]
