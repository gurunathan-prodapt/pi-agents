# Migrated from vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/k_ausd_v_ta_p_discount_rr.ksh
# This Airflow DAG orchestrates the data processing for ta_p_discount_rr in BigQuery.

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.google.cloud.operators.bigquery import BigQueryOperator
from airflow.utils.dates import days_ago
from airflow.exceptions import AirflowException
import logging

# Set up logging
log = logging.getLogger(__name__)

# --- Configuration Variables ---
# Replace with your actual BigQuery project and dataset IDs
BIGQUERY_PROJECT_ID = 'your_bigquery_project'
BIGQUERY_DATASET_ID = 'your_bigquery_dataset'
TARGET_TABLE_ID = 'ta_p_discount_rr'
SOURCE_SQL_PATH = 'd_ausd_v_ta_p_discount_rr.bq.sql' # Path to the BigQuery SQL file

# Default arguments for the DAG
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': ['your_email@example.com'],
    'email_on_retry': False,
    'retries': 1,
    'start_date': days_ago(1), # Set a sensible start date
}

def _validate_parameters(**kwargs):
    """
    Validates required parameters (p_JobKennung, p_EintragsNr) from DAG run configuration.
    Corresponds to 'pruefeParameterGesetzt' and error handling in the KSH script.
    """
    ti = kwargs['ti']
    job_kennung = kwargs['dag_run'].conf.get('job_kennung')
    eintrags_nr = kwargs['dag_run'].conf.get('eintrags_nr')

    if not job_kennung:
        raise AirflowException("Required parameter 'job_kennung' is missing. Please provide it in DAG run configuration.")
    if not eintrags_nr:
        raise AirflowException("Required parameter 'eintrags_nr' is missing. Please provide it in DAG run configuration.")

    log.info(f"Parameters validated: p_JobKennung={job_kennung}, p_EintragsNr={eintrags_nr}")
    ti.xcom_push(key='job_kennung', value=job_kennung)
    ti.xcom_push(key='eintrags_nr', value=eintrags_nr)


def _log_record_count(**kwargs):
    """
    Retrieves and logs the number of records processed by the BigQuery task.
    Corresponds to 'eval "v_records=`cat $tmpFile`"' in the KSH script.
    """
    ti = kwargs['ti']
    # The BigQueryOperator pushes job_id to XCom. We can get job statistics from it.
    # For a direct count from the BigQueryOperator, we'd need to inspect its return value.
    # A simpler way is to query the target table after the insert.

    # Assuming `execute_data_processing` task returns number of rows inserted if configured.
    # The BigQueryOperator's default behavior is to push the job_id.
    # To get row counts, one might need a follow-up query or specific configuration for BigQueryOperator.
    # For simplicity, we'll assume `num_rows_inserted` is pushed to XCom by a customized BQ operator
    # or perform a separate count query.
    # For standard BigQueryOperator, we'd query the table for total rows after truncation/insert.
    # Here, we'll just acknowledge the successful completion.

    # Example if BigQueryOperator pushed actual rows inserted:
    # num_rows_inserted = ti.xcom_pull(task_ids='execute_data_processing', key='return_value')
    # if num_rows_inserted is not None:
    #     log.info(f"Number of records processed: {num_rows_inserted}")
    # else:
    #     log.warning("Could not retrieve number of records processed from BigQuery task.")

    # A more robust way: query the target table
    # This task would typically be a BigQueryGetDataOperator or BigQueryExecuteQueryOperator
    # followed by a PythonOperator to parse the result.
    log.info("Data processing completed. Please check BigQuery job logs for record counts.")


def _update_job_management_tables(**kwargs):
    """
    Placeholder for logic to update job management tables.
    Corresponds to "Eintrag in die Job-Tabelle" and "alte aktive Jobs werden einfach dekativiert".
    This would typically involve BigQuery UPDATE/INSERT statements or calls to a metadata service.
    """
    log.info("Executing job table update and deactivation logic (placeholder).")
    # Example:
    # from airflow.providers.google.cloud.operators.bigquery import BigQueryExecuteQueryOperator
    # update_query = f"UPDATE `{BIGQUERY_PROJECT_ID}.{BIGQUERY_DATASET_ID}.job_tracking_table` SET status='COMPLETED' WHERE job_id = '{job_kennung}';"
    # BigQueryExecuteQueryOperator(task_id='update_job_tracking', sql=update_query).execute(context=kwargs)


with DAG(
    dag_id='k_ausd_v_ta_p_discount_rr_dag',
    default_args=default_args,
    description='Airflow DAG for k_ausd_v_ta_p_discount_rr.ksh migration',
    schedule_interval=None, # This DAG is likely triggered manually or by another system
    tags=['bigquery', 'data_ingestion', 'isbert'],
    catchup=False,
) as dag:
    start_task = PythonOperator(
        task_id='start_processing',
        python_callable=lambda: log.info("Starting data processing for ta_p_discount_rr."),
    )

    validate_parameters = PythonOperator(
        task_id='validate_parameters',
        python_callable=_validate_parameters,
        provide_context=True,
    )

    # Note: The original script derived v_datum but didn't use it in the provided SQL.
    # If v_datum logic is required for partitioning or dynamic filtering,
    # it can be extracted into a separate PythonOperator or BigQuery query task.

    execute_data_processing = BigQueryOperator(
        task_id='execute_data_processing',
        sql=SOURCE_SQL_PATH,
        destination_project_dataset_table=f'{BIGQUERY_PROJECT_ID}.{BIGQUERY_DATASET_ID}.{TARGET_TABLE_ID}',
        write_disposition='WRITE_TRUNCATE', # Equivalent to TRUNCATE and then INSERT
        use_legacy_sql=False,
        gcp_conn_id='google_cloud_default', # Ensure you have this connection configured
        # Pass original KSH parameters to the SQL script as BigQuery query parameters
        params={
            'p_job_kennung': "{{ task_instance.xcom_pull(task_ids='validate_parameters', key='job_kennung') }}",
            'p_eintrags_nr': "{{ task_instance.xcom_pull(task_ids='validate_parameters', key='eintrags_nr') }}",
        },
        # If the SQL file requires specific settings or UDFs, they can be added here.
    )

    # Placeholder for job management before processing, if needed (e.g., mark job as running)
    # This could be integrated with the _update_job_management_tables function with a status parameter
    update_job_status_start = PythonOperator(
        task_id='update_job_status_start',
        python_callable=_update_job_management_tables, # This needs modification to accept a status
        # op_kwargs={'status': 'RUNNING', 'job_kennung': "{{ task_instance.xcom_pull(task_ids='validate_parameters', key='job_kennung') }}"}
    )

    log_record_count = PythonOperator(
        task_id='log_record_count',
        python_callable=_log_record_count,
        provide_context=True,
    )

    update_job_status_end = PythonOperator(
        task_id='update_job_status_end',
        python_callable=_update_job_management_tables,
        # op_kwargs={'status': 'COMPLETED', 'job_kennung': "{{ task_instance.xcom_pull(task_ids='validate_parameters', key='job_kennung') }}"}
    )

    end_task = PythonOperator(
        task_id='end_processing',
        python_callable=lambda: log.info("Finished data processing for ta_p_discount_rr."),
    )

    # Define task dependencies
    start_task >> validate_parameters >> update_job_status_start >> execute_data_processing >> log_record_count >> update_job_status_end >> end_task