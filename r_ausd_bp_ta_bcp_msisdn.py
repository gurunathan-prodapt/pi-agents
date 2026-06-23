# Legacy Source: vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/r_ausd_bp_ta_bcp_msisdn.ksh
# Job: vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/r_ausd_bp_ta_bcp_msisdn.ksh

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago
from airflow.exceptions import AirflowException
import datetime
import logging

# Assuming bert_utils.py is available in the Python path or bundled with the DAG
# For deployment, ensure `bert_utils.py` is accessible by Airflow workers
import bert_utils as bu

# Set up logging for the DAG file
logger = logging.getLogger(__name__)

def _parse_and_validate_parameters_task(**context):
    """
    Parses and validates the Stichtag and Wiederanlaufwert parameters
    from Airflow DAG run configuration or default parameters.
    Pushes validated parameters to XComs.
    """
    # Attempt to retrieve stichtag from DAG run config, then from default params
    stichtag_param = None
    if context.get('dag_run') and context['dag_run'].conf:
        stichtag_param = context['dag_run'].conf.get('stichtag')
    if stichtag_param is None:
        stichtag_param = context['params'].get('stichtag')

    # Attempt to retrieve wiederanlaufwert from DAG run config, then from default params
    wiederanlaufwert_param = None
    if context.get('dag_run') and context['dag_run'].conf:
        wiederanlaufwert_param = context['dag_run'].conf.get('wiederanlaufwert')
    if wiederanlaufwert_param is None:
        wiederanlaufwert_param = context['params'].get('wiederanlaufwert')

    try:
        stichtag_yyyymmdd, wiederanlaufwert_int = bu.parse_and_validate_parameters(
            stichtag_raw=stichtag_param,
            wiederanlaufwert_raw=wiederanlaufwert_param
        )
        # Push validated parameters to XComs for downstream tasks
        context['ti'].xcom_push(key='stichtag_yyyymmdd', value=stichtag_yyyymmdd)
        context['ti'].xcom_push(key='wiederanlaufwert', value=wiederanlaufwert_int)
        context['ti'].xcom_push(key='sysdate_yyyymmdd', value=bu.get_current_date_yyyymmdd())
        bu.log_message(f"Parameters successfully parsed: Stichtag={stichtag_yyyymmdd}, Wiederanlaufwert={wiederanlaufwert_int}")
    except Exception as e:
        # bu.log_error would have already logged the specific issue
        raise AirflowException(f"Parameter parsing and validation failed: {e}")

def _init_logging_task(**context):
    """
    Initializes job logging details, including job identifier and a unique entry number.
    Pushes job details to XComs.
    """
    job_kennung = "ausd_bp_ta_bcp_msisdnT"
    job_nr = bu.generate_job_entry_number()

    context['ti'].xcom_push(key='job_kennung', value=job_kennung)
    context['ti'].xcom_push(key='job_nr', value=job_nr)

    stichtag_for_log = context['ti'].xcom_pull(key='stichtag_yyyymmdd')
    bu.log_message(f"Job Initialized: JobKennung='{job_kennung}', JobEntryNumber='{job_nr}'")
    bu.log_message(f"Stichtag for run: {stichtag_for_log}")

def _execute_kernel_script_logic_task(**context):
    """
    Placeholder for executing the migrated k_ausd_bp_ta_bcp_msisdn.ksh logic.
    This task would typically invoke a BigQuery Stored Procedure or a PySpark job.
    """
    stichtag_yyyymmdd = context['ti'].xcom_pull(key='stichtag_yyyymmdd')
    wiederanlaufwert = context['ti'].xcom_pull(key='wiederanlaufwert')
    job_kennung = context['ti'].xcom_pull(key='job_kennung')
    job_nr = context['ti'].xcom_pull(key='job_nr')

    bu.log_message(f"Commencing core transformation logic with parameters:")
    bu.log_message(f"  - JobKennung: {job_kennung}")
    bu.log_message(f"  - Stichtag: {stichtag_yyyymmdd}")
    bu.log_message(f"  - JobEntryNumber: {job_nr}")
    bu.log_message(f"  - Wiederanlaufwert: {wiederanlaufwert}")

    # --- Core Logic of k_ausd_bp_ta_bcp_msisdn.ksh goes here ---
    # As per the design document, this will be migrated to BigQuery SQL/Stored Procedures
    # or PySpark. The following are placeholders for how they would be invoked.

    # Example for BigQuery SQL (e.g., calling a stored procedure):
    # from airflow.providers.google.cloud.operators.bigquery import BigQueryExecuteQueryOperator
    # bq_operator = BigQueryExecuteQueryOperator(
    #     task_id='run_bq_transformation',
    #     sql=f"CALL `your_gcp_project.your_bq_dataset.k_ausd_bp_ta_bcp_msisdn_sp`("
    #         f"p_job_kennung=>'{job_kennung}', "
    #         f"p_stichtag=>'{stichtag_yyyymmdd}', "
    #         f"p_job_nr=>'{job_nr}', "
    #         f"p_wiederanlaufwert=>{wiederanlaufwert}"
    #     f")",
    #     use_legacy_sql=False,
    #     gcp_conn_id='google_cloud_default' # Ensure this connection is configured in Airflow
    # )
    # bq_operator.execute(context=context)

    # Example for PySpark on Dataproc:
    # from airflow.providers.google.cloud.operators.dataproc import DataprocSparkOperator
    # dataproc_operator = DataprocSparkOperator(
    #     task_id='run_pyspark_transformation',
    #     main_python_file=f'gs://your-gcs-bucket/k_ausd_bp_ta_bcp_msisdn.py', # Path to PySpark script in GCS
    #     arguments=[
    #         f'--job_kennung={job_kennung}',
    #         f'--stichtag={stichtag_yyyymmdd}',
    #         f'--job_nr={job_nr}',
    #         f'--wiederanlaufwert={wiederanlaufwert}'
    #     ],
    #     cluster_name='your-dataproc-cluster-name', # Replace with your Dataproc cluster name
    #     region='your-gcp-region', # Replace with your GCP region
    #     project_id='your-gcp-project-id' # Replace with your GCP project ID
    # )
    # dataproc_operator.execute(context=context)

    # For now, we just log that the core logic would be executed.
    bu.log_message("Placeholder: Core transformation logic (from k_ausd_bp_ta_bcp_msisdn.ksh) would be executed here.")
    bu.log_message("Core transformation logic completed (placeholder).")


with DAG(
    dag_id='r_ausd_bp_ta_bcp_msisdn_migration',
    start_date=days_ago(1), # Set a past date for the DAG to be ready
    schedule_interval=None, # This DAG is likely triggered manually or by upstream events
    catchup=False,
    tags=['bert', 'etl', 'migration', 'msisdn'],
    params={
        "stichtag": {
            "type": ["string", "null"],
            "description": "Cutoff date for data extraction (DDMMYYYY format). Defaults to system date if not provided.",
            "pattern": r"^(0[1-9]|[12][0-9]|3[01])(0[1-9]|1[0-2])\d{4}$|^$", # Allows DDMMYYYY or empty string
            "default": None,
        },
        "wiederanlaufwert": {
            "type": ["integer", "string", "null"], # Allow string for Airflow UI input, convert to int in code
            "description": "Restart value (DWH_VERTRAG_ID). Only process contracts with ID > this value. Defaults to 0.",
            "default": 0,
        },
    },
) as dag:
    parse_validate_task = PythonOperator(
        task_id='parse_and_validate_parameters',
        python_callable=_parse_and_validate_parameters_task,
        do_xcom_push=True,
    )

    init_logging_task = PythonOperator(
        task_id='initialize_job_logging',
        python_callable=_init_logging_task,
        do_xcom_push=True,
    )

    execute_core_task = PythonOperator(
        task_id='execute_core_transformation_logic',
        python_callable=_execute_kernel_script_logic_task,
        do_xcom_push=False, # This task doesn't explicitly need to push XComs for this example
    )

    # Define task dependencies
    parse_validate_task >> init_logging_task >> execute_core_task