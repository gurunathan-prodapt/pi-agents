# Legacy Source: vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/r_ausd_adressen.ksh
# Job: vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/r_ausd_adressen.ksh

"""
Airflow DAG for migrating the 'r_ausd_adressen.ksh' KornShell orchestration script.
This DAG handles parameter parsing, validation, logging, and orchestrates
the core address data processing, which is assumed to be migrated to BigQuery SQL.
"""

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.google.cloud.operators.bigquery import BigQueryOperator
from airflow.utils.dates import days_ago
from datetime import datetime, timedelta

# Import custom utilities. In a deployed Airflow environment, 'dwh_util'
# should be available on the Python path (e.g., in a plugins folder or as a custom Python package).
try:
    from dwh_util import utils
except ImportError:
    # Fallback for local development/testing where dwh_util might not be packaged
    import sys
    import os
    _current_dir = os.path.dirname(os.path.abspath(__file__))
    _parent_dir = os.path.abspath(os.path.join(_current_dir, '..'))
    if _parent_dir not in sys.path:
        sys.path.append(_parent_dir)
    from dwh_util import utils

# Default arguments for the DAG
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    dag_id='r_ausd_adressen_ksh_migration',
    default_args=default_args,
    description='Migrated Airflow DAG for r_ausd_adressen.ksh - Orchestrates address data extraction.',
    start_date=days_ago(1),
    schedule_interval=None,  # Set to a cron expression (e.g., '0 0 * * *' for daily) or None for manual/external trigger
    catchup=False,
    tags=['isrpt', 'bert', 'adressen', 'migration'],
    params={
        "stichtag": {
            "type": "string",
            "title": "Reference Date (DDMMYYYY)",
            "description": "Date for data extraction in DDMMYYYY format. Defaults to system date if empty.",
            "pattern": "^\\d{8}$|^$", # Allows empty string for defaulting
            "default": ""
        },
        "wiederanlaufwert": {
            "type": "integer",
            "title": "Restart Value",
            "description": "If specified, influences which contracts are processed. Defaults to 0.",
            "default": 0
        }
    }
) as dag:

    def prepare_params_callable(**context):
        """
        Prepares and defaults parameters from DAG run configuration.
        Replaces part of the shell script's argument parsing and defaulting logic.
        """
        stichtag = context['params'].get('stichtag')
        wiederanlaufwert = context['params'].get('wiederanlaufwert')

        # Defaulting logic from original script
        if wiederanlaufwert is None: # Use 'is None' to distinguish from 0
            wiederanlaufwert = 0
        if not stichtag:
            stichtag = datetime.now().strftime('%d%m%Y')
        
        context['ti'].xcom_push(key='processed_stichtag', value=stichtag)
        context['ti'].xcom_push(key='processed_wiederanlaufwert', value=wiederanlaufwert)
        context['ti'].xcom_push(key='job_kennung', value="BERT_P_ADRESSEN") # Define JobKennung as per design

        utils.logger.info(f"Parameters prepared: Stichtag={stichtag}, Wiederanlaufwert={wiederanlaufwert}")


    prepare_parameters_task = PythonOperator(
        task_id='prepare_parameters_and_env',
        python_callable=prepare_params_callable,
        provide_context=True,
    )

    def validate_params_callable(**context):
        """
        Validates the prepared parameters.
        Equivalent to calling pruefeParameterGesetzt.
        """
        stichtag = context['ti'].xcom_pull(task_ids='prepare_parameters_and_env', key='processed_stichtag')
        job_kennung = context['ti'].xcom_pull(task_ids='prepare_parameters_and_env', key='job_kennung')

        # Generate a temporary entry_nr for validation logging, as the main one is generated later.
        # This prevents breaking the flow if validation fails before init_job_logging.
        temp_entry_nr = utils.generate_new_entry_number()
        utils.pruefe_parameter_gesetzt("Stichtag", stichtag, temp_entry_nr)
        utils.logger.info("Parameters validated successfully.")


    validate_parameters_task = PythonOperator(
        task_id='validate_parameters',
        python_callable=validate_params_callable,
        provide_context=True,
    )

    def init_job_logging_callable(**context):
        """
        Initializes job tracking and logging.
        Replaces DWMSG_ calls for setting up job entry, log file, and initial status.
        """
        job_kennung = context['ti'].xcom_pull(task_ids='prepare_parameters_and_env', key='job_kennung')
        stichtag = context['ti'].xcom_pull(task_ids='prepare_parameters_and_env', key='processed_stichtag')

        entry_nr = utils.generate_new_entry_number()
        log_file_name = utils.generate_log_filename(job_kennung, entry_nr)

        context['ti'].xcom_push(key='entry_nr', value=entry_nr)
        context['ti'].xcom_push(key='log_file_name', value=log_file_name)

        utils.log_job_entry(entry_nr, job_kennung, log_file_name)
        utils.log_stichtag_info(entry_nr, stichtag)
        
        # Determine start_date and end_date using DWDate_Gib_Zeitraum equivalent
        # Assuming %d%m%Y format for stichtag as per design
        start_date, end_date = utils.get_zeitraum_dates(stichtag, '%d%m%Y')
        context['ti'].xcom_push(key='zeitraum_start_date', value=start_date)
        context['ti'].xcom_push(key='zeitraum_end_date', value=end_date)
        utils.logger.info(f"Zeitraum dates derived: Start={start_date}, End={end_date}")


    initialize_job_logging_task = PythonOperator(
        task_id='initialize_job_logging',
        python_callable=init_job_logging_callable,
        provide_context=True,
    )

    # Task to execute core processing logic (k_ausd_adressen.ksh equivalent) in BigQuery.
    # This assumes k_ausd_adressen.ksh has been fully migrated to BigQuery SQL,
    # as per the recommended approach in the design document.
    # The SQL file 'sql/k_ausd_adressen_logic.sql' is executed with templated parameters.
    execute_core_processing_task = BigQueryOperator(
        task_id='execute_k_ausd_adressen_bq',
        sql='sql/k_ausd_adressen_logic.sql', # Path to the BigQuery SQL file within the DAGs folder or plugins
        params={
            'job_kennung': "{{ ti.xcom_pull(task_ids='initialize_job_logging', key='job_kennung') }}",
            'stichtag': "{{ ti.xcom_pull(task_ids='prepare_parameters_and_env', key='processed_stichtag') }}",
            'entry_nr': "{{ ti.xcom_pull(task_ids='initialize_job_logging', key='entry_nr') }}",
            'wiederanlaufwert': "{{ ti.xcom_pull(task_ids='prepare_parameters_and_env', key='processed_wiederanlaufwert') }}",
            'start_date': "{{ ti.xcom_pull(task_ids='initialize_job_logging', key='zeitraum_start_date') }}",
            'end_date': "{{ ti.xcom_pull(task_ids='initialize_job_logging', key='zeitraum_end_date') }}"
        },
        use_legacy_sql=False, # Use Standard SQL
        # Replace 'your-gcp-project-id' and 'your_target_dataset' with actual BigQuery project/dataset
        # If the SQL inserts into a specific table, you might define it here or within the SQL.
        # E.g., if the SQL overwrites a table:
        # destination_dataset_table='your-gcp-project-id.your_target_dataset.dwh_target_addresses',
        # write_disposition='WRITE_TRUNCATE',
    )

    def update_final_status_callable(**context):
        """
        Updates the final status of the job.
        Replaces DWMSG_SetzeStatusOK. This task will only run if preceding tasks succeed.
        """
        entry_nr = context['ti'].xcom_pull(task_ids='initialize_job_logging', key='entry_nr')
        utils.log_job_status(entry_nr, "OK")


    update_final_status_task = PythonOperator(
        task_id='update_final_status',
        python_callable=update_final_status_callable,
        provide_context=True,
    )

    # Define Task Dependencies
    prepare_parameters_task >> validate_parameters_task >> initialize_job_logging_task >> execute_core_processing_task >> update_final_status_task