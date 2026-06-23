#
# Airflow DAG for contract data reconciliation of ta_discount_rr.
# This DAG replaces the legacy KornShell script:
# vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/r_ausd_v_ta_discount_rr.ksh
#
# The core SQL logic is migrated from:
# vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/sql/d_ausd_v_ta_discount_rr.sql
#

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.google.cloud.operators.bigquery import BigQueryExecuteQueryOperator
from airflow.utils.dates import days_ago
from datetime import datetime
import logging

# Import the utility functions. Assuming 'utils' directory is in PYTHONPATH or next to DAGs.
try:
    from utils.dw_utils import dwmsg_meldefehler, pruefe_parameter_gesetzt, get_current_dw_date_str, DWError
except ImportError:
    # Fallback for environments where dw_utils might not be directly available yet
    logging.warning("Could not import dw_utils. Falling back to dummy functions.")
    class DWError(Exception): pass
    def dwmsg_meldefehler(*args, **kwargs):
        logging.error(f"DUMMY DWMSG_MeldeFehler: {args}, {kwargs}")
        raise DWError("DUMMY DWError triggered")
    def pruefe_parameter_gesetzt(param_name: str, param_value: any):
        if param_value is None or (isinstance(param_value, str) and not param_value.strip()):
            raise DWError(f"DUMMY Param missing or empty: {param_name}")
        logging.info(f"DUMMY Param set: {param_name}={param_value}")
    def get_current_dw_date_str() -> str:
        return datetime.now().strftime('%Y%m%d')


# Configure logging for the DAG
log = logging.getLogger(__name__)

# Default arguments for the DAG
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': None, # Using default retry logic often handled by Airflow configs
    # on_failure_callback can be used here for custom error alerting
    # 'on_failure_callback': some_failure_alert_function,
}

# Define the Airflow DAG
with DAG(
    dag_id='r_ausd_v_ta_discount_rr_dag',
    default_args=default_args,
    description='Orchestrates contract data reconciliation for ta_discount_rr. Replaces ksh wrapper.',
    schedule_interval=None, # The original ksh script was likely triggered by an external scheduler.
                            # Set to a cron expression (e.g., '0 0 * * *') for daily runs, or None for manual/external triggers.
    start_date=days_ago(1),
    tags=['isbert', 'ta_discount_rr', 'reconciliation', 'bigquery'],
    catchup=False, # Do not run for past missed schedules
) as dag:

    # Task to determine the processing date (v_datum_str)
    # This replicates the logic from d_ausd_v_ta_discount_rr.sql to get the max timecreated
    # for 'BERT_DROP_TEMP_TABLE' from the dwtk_meldungen table.
    extract_v_datum_task = BigQueryExecuteQueryOperator(
        task_id='extract_v_datum_from_bigquery',
        sql='''
            SELECT COALESCE(FORMAT_DATE('%Y%m%d', MAX(m.timecreated)), '19000101')
            FROM `source_project.source_dataset.dwtk_meldungen` AS m
            WHERE m.job_kennung = 'BERT_DROP_TEMP_TABLE'
        ''',
        use_legacy_sql=False, # Use standard SQL
        gcp_conn_id='google_cloud_default', # Assumes a configured GCP connection
        # result_as_miq=True pushes the result (a list of lists) to XCom
        # We'll retrieve it in the next PythonOperator.
        do_xcom_push=True,
    )

    # Python function to process the extracted v_datum and push it to XCom
    def _set_v_datum_parameter(**context):
        """
        Retrieves v_datum from the BigQuery task's XCom output.
        If not found or empty, it defaults to '19000101' as per original script's NVL logic.
        """
        ti = context['ti']
        query_results = ti.xcom_pull(task_ids='extract_v_datum_from_bigquery')

        v_datum_str = '19000101' # Default value as per original SQL's NVL
        if query_results and query_results[0] and query_results[0][0]:
            v_datum_str = query_results[0][0]
        else:
            log.warning(f"Could not retrieve v_datum from BigQuery. Using default: {v_datum_str}")

        log.info(f"Final processing date (v_datum_str): {v_datum_str}")
        ti.xcom_push(key='v_datum_str', value=v_datum_str)

    set_v_datum_parameter_task = PythonOperator(
        task_id='set_v_datum_parameter',
        python_callable=_set_v_datum_parameter,
        provide_context=True,
    )

    # Python function for parameter validation and job identification
    # This replaces parts of r_ausd_v_ta_discount_rr.ksh for parameter handling
    def _initialize_job_parameters(**context):
        """
        Initializes job-specific parameters like job_kennung and eintrags_nr.
        Performs validation using the re-implemented utility functions.
        """
        ti = context['ti']
        # The original ksh script takes -j (JobKennung) and -f (EintragsNr).
        # In Airflow, JobKennung can be a fixed string or configurable via DAG params/Airflow Variables.
        # EintragsNr can be based on the Airflow run_id for uniqueness.
        job_kennung = dag.dag_id # Using the DAG ID as the job identifier
        eintrags_nr = ti.run_id # Airflow's unique run ID for this execution

        # Validate parameters using the utility function
        try:
            pruefe_parameter_gesetzt('JobKennung', job_kennung)
            pruefe_parameter_gesetzt('EintragsNr', eintrags_nr)
        except DWError as e:
            log.error(f"Job parameter validation failed: {e}")
            raise # Re-raise to fail the Airflow task

        log.info(f"Initialized JobKennung: {job_kennung}, EintragsNr: {eintrags_nr}")
        ti.xcom_push(key='job_kennung', value=job_kennung)
        ti.xcom_push(key='eintrags_nr', value=eintrags_nr)

    initialize_job_parameters_task = PythonOperator(
        task_id='initialize_job_parameters',
        python_callable=_initialize_job_parameters,
        provide_context=True,
    )

    # Task to execute the core BigQuery SQL logic
    # This uses the migrated d_ausd_v_ta_discount_rr.sql script.
    execute_core_sql_task = BigQueryExecuteQueryOperator(
        task_id='execute_core_reconciliation_sql',
        # The SQL file needs to be accessible by the Airflow worker.
        # This typically means it's deployed alongside the DAG or in GCS.
        sql='sql/d_ausd_v_ta_discount_rr.sql',
        use_legacy_sql=False, # Use standard SQL
        gcp_conn_id='google_cloud_default',
        # Pass the extracted v_datum_str as a parameter to the SQL script
        params={
            'v_datum_str': '{{ ti.xcom_pull(task_ids="set_v_datum_parameter", key="v_datum_str") }}'
        },
        # If this task were to create a new table or overwrite one entirely
        # you might specify destination_dataset_table and write_disposition.
        # However, since the SQL itself performs TRUNCATE and INSERT, this is not strictly necessary.
    )

    # Define task dependencies
    # Extract the date and initialize job parameters concurrently
    # Then set the date parameter (which depends on the BigQuery extract)
    # Finally, execute the SQL which depends on both initialization and date parameter.
    extract_v_datum_task >> set_v_datum_parameter_task
    initialize_job_parameters_task >> execute_core_sql_task # Parameters are used within the DAG itself for logging, but not directly for BigQuery task
    set_v_datum_parameter_task >> execute_core_sql_task