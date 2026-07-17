# -*- coding: utf-8 -*-
"""Reusable BigQuery operational utilities."""

import logging
from typing import List, Dict, Any
from google.cloud import bigquery

logger = logging.getLogger(__name__)

class BigQueryHandler:
    """Helper class to abstract BigQuery interactions."""
    
    def __init__(self, project_id: str):
        self.project_id = project_id
        self._client = None

    @property
    def client(self) -> bigquery.Client:
        """Lazy-loaded BigQuery client."""
        if self._client is None:
            self._client = bigquery.Client(project=self.project_id)
        return self._client

    def execute_query(self, query_text: str, parameters: List[bigquery.ScalarQueryParameter]) -> List[Dict[str, Any]]:
        """Executes a parameterized BigQuery query and returns results as a list of dicts."""
        job_config = bigquery.QueryJobConfig(query_parameters=parameters)
        try:
            query_job = self.client.query(query_text, job_config=job_config)
            results = query_job.result()
            # Convert Row iterator into serializable dictionaries preserving insertion order
            return [dict(row.items()) for row in results]
        except Exception as e:
            logger.error(f"Failed to execute BigQuery query: {str(e)}")
            raise e