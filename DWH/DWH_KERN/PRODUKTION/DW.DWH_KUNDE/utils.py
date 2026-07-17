#!/usr/bin/env python3
"""
Common utilities for DWH Python migration processes.
Provides reusable logging wrappers and database client adapters.
"""

import sys
from datetime import datetime
from google.cloud import bigquery


def f_alis_msgerr(level: str, text: str) -> None:
    """
    Prints an error/warning message matching the legacy format.
    
    Args:
        level: Severity level (e.g., 'W' for Warning, 'E' for Error)
        text: The message body
    """
    now_str = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    sys.stderr.write(f"[{level}] {now_str} {text}\n")


def execute_bigquery_query(
    sql_text: str, 
    parameters: dict,
    project: str = None
) -> bigquery.table.RowIterator:
    """
    Executes a BigQuery parameterized query and returns the results.
    
    Args:
        sql_text: Standard SQL dialect query string
        parameters: Dictionary containing parameter names, types, and values
        project: Target GCP project ID (Optional)
    """
    client = bigquery.Client(project=project)
    
    query_parameters = []
    for name, param_info in parameters.items():
        query_parameters.append(
            bigquery.ScalarQueryParameter(
                name, 
                param_info.get("type", "STRING"), 
                param_info.get("value")
            )
        )
        
    job_config = bigquery.QueryJobConfig(query_parameters=query_parameters)
    query_job = client.query(sql_text, job_config=job_config)
    return query_job.result()