# DW.BERT_AUSD_V_TA_CNTRCT_TEMPL - Utility Functions
# This file will contain Python utility functions replacing KornShell script functionality.

import logging
from datetime import datetime, timedelta
import yaml
from google.cloud import bigquery
import os

logger = logging.getLogger(__name__)

def load_yaml_config(config_file: str) -> dict:
    """
    Loads configuration from a YAML file.
    """
    logger.info(f"Loading configuration from {config_file}")
    with open(config_file, 'r') as f:
        config = yaml.safe_load(f)
    return config

def log_error_message(job_id: str, message: str, error_details: dict = None):
    """
    Replaces f_alis_msgerr.ksh and DWMSG_MeldeFehler for logging errors.
    Logs an error message, potentially sending it to Cloud Logging and monitoring systems.
    """
    full_message = f"JOB_ID: {job_id} - ERROR: {message}"
    if error_details:
        full_message += f" Details: {error_details}"
    logger.error(full_message)
    # In a real Airflow environment, this would integrate with Cloud Logging
    # and potentially trigger alerts via Cloud Monitoring.

def get_processing_date_from_bq(gcp_project_id: str, job_kennung: str = 'BERT_DROP_TEMP_TABLE') -> str:
    """
    Replaces the SQL logic for determining v_datum from isbert_schema.dwtk_meldungen.
    Queries BigQuery to get the maximum timecreated for a given job_kennung.
    Returns the date as 'YYYYMMDD' string.
    """
    logger.info(f"Fetching processing date for job_kennung: {job_kennung}")
    bq_client = bigquery.Client(project=gcp_project_id)
    query = f"""
        SELECT
            FORMAT_DATE('%Y%m%d', MAX(timecreated))
        FROM
            `{gcp_project_id}.metadata.dwtk_meldungen`
        WHERE
            job_kennung = @job_kennung
    """
    job_config = bigquery.QueryJobConfig(
        query_parameters=[
            bigquery.ScalarQueryParameter("job_kennung", "STRING", job_kennung)
        ]
    )
    query_job = bq_client.query(query, job_config=job_config)
    results = query_job.result()
    for row in results:
        processing_date = row[0]
        if processing_date:
            logger.info(f"Determined processing date: {processing_date}")
            return processing_date
    
    logger.warning(f"No processing date found for job_kennung: {job_kennung}. Returning default '19000101'.")
    return '19000101' # Default as per original SQL NVL

def parse_parameters(args: list) -> dict:
    """
    Replaces h_alis_parameter.ksh for parsing command-line arguments.
    This is simplified for Airflow context where parameters are often passed directly.
    """
    logger.info(f"Parsing parameters: {args}")
    # In an Airflow context, parameters are usually passed via DAG params or XComs.
    # This function might be adapted to process Airflow task instance parameters.
    # For now, it's a simple placeholder.
    params = {}
    for arg in args:
        if '=' in arg:
            key, value = arg.split('=', 1)
            params[key.strip()] = value.strip()
    return params

def execute_bq_sql_script(gcp_project_id: str, sql_file_path: str, params: dict = None):
    """
    Replaces h_alis_sqlplus.ksh for executing SQL.
    Executes a BigQuery SQL script, optionally with parameters.
    """
    logger.info(f"Executing BigQuery SQL script: {sql_file_path}")
    try:
        # Construct the absolute path to the SQL file relative to the DAGs folder
        # Assuming the structure is dags/dag_file.py and sql/sql_file.sql
        current_dir = os.path.dirname(os.path.abspath(__file__))
        sql_absolute_path = os.path.join(current_dir, "..", sql_file_path)

        with open(sql_absolute_path, 'r') as f:
            sql_content = f.read()

        # Simple templating for now, Airflow's BigQueryOperator can do more
        # For this context, BigQueryInsertJobOperator will handle templating
        # via Jinja2 if the query is passed directly.
        # This function is more for general-purpose direct execution if needed.
        if params:
            for key, value in params.items():
                sql_content = sql_content.replace(f"{{{{ {key} }}}}", str(value))
        
        bq_client = bigquery.Client(project=gcp_project_id)
        query_job = bq_client.query(sql_content)
        query_job.result() # Wait for the job to complete
        logger.info(f"Successfully executed BigQuery SQL script: {sql_file_path}")
    except Exception as e:
        log_error_message("DW.BERT_AUSD_V_TA_CNTRCT_TEMPL", f"Failed to execute SQL script {sql_file_path}", {"error": str(e)})
        raise