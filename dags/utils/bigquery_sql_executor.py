"""
This module provides a Python function to execute BigQuery SQL scripts,
migrated from the legacy KornShell utility h_alis_sqlplus.ksh.
"""
# Replaces legacy source vobs/dw_source/isrpt/isbert/SQL/aktuell/allgemein/is/util/bin/h_alis_sqlplus.ksh
import logging
from airflow.exceptions import AirflowException
from airflow.providers.google.cloud.operators.bigquery import BigQueryExecuteQueryOperator
from google.cloud import storage

logger = logging.getLogger(__name__)

def execute_bigquery_script(entry_number: str, script_ref: str, sql_parameters: dict = None, **kwargs):
    """
    Executes a BigQuery SQL script or query, analogous to the legacy starteSQLSkript.
    
    This function replaces the functionality of the h_alis_sqlplus.ksh script's
    `starteSQLSkript` function, adapting it for Google Cloud Composer (Airflow)
    and BigQuery. It handles parameter validation, optional fetching of SQL
    content from Google Cloud Storage, and execution using Airflow's
    BigQueryExecuteQueryOperator.

    :param entry_number: An identifier for logging/error reporting, corresponding
                         to p_Eintragsnr in the legacy script.
    :param script_ref: Reference to the SQL script. This can be:
                       1. A direct SQL string.
                       2. A GCS path (e.g., 'gs://my-bucket/sql/my_script.sql')
                          from which the SQL content will be fetched.
                       This corresponds to p_Skript in the legacy script.
    :param sql_parameters: An optional dictionary of parameters to pass to the
                           BigQuery query. These are used for templating in BigQuery
                           SQL (e.g., #declare, @param).
    :param kwargs: Arbitrary keyword arguments, typically containing Airflow's
                   context variables (e.g., 'task_instance').
    :raises AirflowException: If required parameters are missing, GCS script is
                              not found/inaccessible, or BigQuery execution fails.
    """
    sql_parameters = sql_parameters or {}

    # 1. Parameter Validation (equivalent to `if [ -z ... ]` in KSH)
    if not entry_number:
        # Using a default error code if entry_number is missing for the error message itself
        error_msg = "Error N/A E 196: Missing 'entry_number' for BigQuery execution."
        logger.error(error_msg)
        raise AirflowException(error_msg)
    
    if not script_ref:
        error_msg = f"Error {entry_number} E 196: Missing 'script_ref' for BigQuery execution."
        logger.error(error_msg)
        raise AirflowException(error_msg)

    sql_content = script_ref
    
    # 2. Handle GCS script reference (equivalent to `if [ ! -r ... ]` for file existence)
    if script_ref.startswith("gs://"):
        try:
            # The client is initialized here to ensure it's created within the task's context
            # and to allow mocking in unit tests more easily.
            gcs_client = storage.Client()
            # Split GCS path: "gs://bucket_name/path/to/blob"
            path_parts = script_ref[5:].split('/', 1) # [5:] to remove "gs://"
            bucket_name = path_parts[0]
            blob_name = path_parts[1] if len(path_parts) > 1 else ''

            bucket = gcs_client.bucket(bucket_name)
            blob = bucket.blob(blob_name)
            
            if not blob.exists():
                error_msg = f"Error {entry_number} E 201: GCS SQL script '{script_ref}' not found or inaccessible."
                logger.error(error_msg)
                raise AirflowException(error_msg)
            
            sql_content = blob.download_as_text()
            logger.info(f"Loaded SQL content from GCS: {script_ref}")
        except Exception as e:
            error_msg = f"Error {entry_number} E 201: Failed to read GCS SQL script '{script_ref}': {e}"
            logger.error(error_msg)
            raise AirflowException(error_msg)
    
    logger.info(f"Initiating BigQuery SQL execution for script reference: {script_ref}")
    if sql_parameters:
        logger.info(f"SQL parameters: {sql_parameters}")

    # 3. SQL Execution (equivalent to `sqlplus ...`)
    try:
        # It's important to provide a unique task_id for the BigQueryExecuteQueryOperator
        # if this function were to be called multiple times within a single PythonOperator,
        # or if it's meant to represent a distinct step in Airflow's UI/metadata.
        # For a PythonOperator, the BigQuery task is ephemeral; the task_id here
        # is mainly for internal operator logging.
        # We can derive a task_id from the PythonOperator's task_id and entry_number
        calling_task_id = kwargs.get('task_instance', {}).get('task_id', 'unknown_python_task')
        bq_operator_task_id = f"execute_bq_script_{calling_task_id}_{entry_number}"

        bq_operator = BigQueryExecuteQueryOperator(
            task_id=bq_operator_task_id,
            sql=sql_content,
            use_legacy_sql=False, # Always use standard SQL for BigQuery
            params=sql_parameters,
            gcp_conn_id='google_cloud_default', # Use the default GCP connection
            # Other BigQuery specific parameters (e.g., destination_dataset_table, write_disposition)
            # could be passed via kwargs to this function if needed,
            # and then dynamically added to the BigQueryExecuteQueryOperator.
        )
        # The .execute() method requires the Airflow context
        bq_operator.execute(context=kwargs)
        logger.info(f"Successfully executed BigQuery SQL script via reference: {script_ref}")
    except Exception as e:
        error_msg = f"Error executing BigQuery SQL for script reference '{script_ref}': {e}"
        logger.error(error_msg)
        # Re-raise as AirflowException to ensure task failure and proper Airflow handling
        raise AirflowException(error_msg)