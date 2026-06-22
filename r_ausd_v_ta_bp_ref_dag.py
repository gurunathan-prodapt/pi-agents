# Airflow DAG for vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/r_ausd_v_ta_bp_ref.ksh
# Replaces legacy KornShell script with an Apache Airflow workflow.

import logging
import os
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.models import Variable

# Initialize logging for this DAG.
# Airflow automatically integrates this with Cloud Logging.
log = logging.getLogger(__name__)

# Default arguments for the DAG.
# These mirror some common practices in legacy ETL and map to Airflow features.
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,  # Configure email alerts via Airflow callbacks/integrations
    'email_on_retry': False,
    'retries': 1,               # Number of retries for failed tasks
    'retry_delay': timedelta(minutes=5), # Delay between retries
}

def log_job_start(**kwargs):
    """
    Logs the start of the job, replicating the functionality of DWMSG_ErzeugeEintrag
    and DWMSG_SetzeStichtagInfo from the legacy KornShell script.
    It also generates a unique entry number (DW_EintragsNr) for the run.
    """
    ti = kwargs['ti']
    dag_run = kwargs['dag_run']
    job_kennung = kwargs.get('job_kennung', 'UNKNOWN_JOB')

    # Simulate DW_EintragsNr. In Airflow, the run_id can serve this purpose
    # or a specific ID generated for lineage. Here, we use a cleaned run_id.
    dw_eintrags_nr = f"{dag_run.run_id.replace('__', '_').replace('-', '_')}"

    log.info(f"Job '{job_kennung}' started.")
    log.info(f"DW_EintragsNr for this run: {dw_eintrags_nr}")
    log.info(f"Execution date: {kwargs['ds']}")

    # Push DW_EintragsNr to XCom for downstream tasks to consume.
    ti.xcom_push(key='dw_eintrags_nr', value=dw_eintrags_nr)

def log_job_success(**kwargs):
    """
    Logs the successful completion of the job, replicating DWMSG_SetzeStatusOK.
    """
    job_kennung = kwargs.get('job_kennung', 'UNKNOWN_JOB')
    log.info(f"Job '{job_kennung}' completed successfully.")

# Define the Airflow DAG.
with DAG(
    dag_id='r_ausd_v_ta_bp_ref_dag',
    start_date=datetime(2023, 1, 1), # Set an appropriate start date
    schedule_interval=None,         # Set a cron schedule (e.g., '0 0 * * *' for daily) or None for manual/external trigger
    catchup=False,                  # Do not run for past missed schedules
    default_args=default_args,
    tags=['isbert', 'auftrag', 'bert'],
    params={                        # Define DAG parameters, mirroring ksh command-line args -s and -l
        'job_kennung': 'BERT_V_TA_BP_REF',
        'param_s': None,            # Optional parameter '-s'
        'param_l': None,            # Optional parameter '-l'
    },
    doc_md="""
    ### Airflow DAG for r_ausd_v_ta_bp_ref.ksh
    This Airflow DAG replaces the legacy KornShell script `vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/r_ausd_v_ta_bp_ref.ksh`.
    It orchestrates the execution of the core data reconciliation logic for the `ta_bp_ref` table.
    The core logic (`k_ausd_v_ta_bp_ref.ksh` or its migrated equivalent) is invoked as a separate task.
    
    **Legacy Source:** `vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/r_ausd_v_ta_bp_ref.ksh`
    **Job ID:** `vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/r_ausd_v_ta_bp_ref.ksh`
    """
) as dag:
    # Retrieve JobKennung, prioritizing runtime config over DAG default params.
    job_kennung = "{{ dag_run.conf.get('job_kennung', params.job_kennung) }}"

    # Task 1: Log the start of the job and generate DW_EintragsNr.
    start_job = PythonOperator(
        task_id='start_job_logging',
        python_callable=log_job_start,
        op_kwargs={'job_kennung': job_kennung},
        provide_context=True, # Required to access Airflow context variables like ti, dag_run
    )

    # Retrieve DW_EintragsNr from XCom, which was pushed by the 'start_job_logging' task.
    dw_eintrags_nr = "{{ task_instance.xcom_pull(task_ids='start_job_logging', key='dw_eintrags_nr') }}"
    
    # Construct optional parameters for the core script invocation,
    # prioritizing runtime config over DAG default params for -s and -l.
    param_s_arg = "{% if dag_run.conf.get('param_s') is not none %} -s {{ dag_run.conf.param_s }}{% elif params.param_s is not none %} -s {{ params.param_s }}{% else %}{% endif %}"
    param_l_arg = "{% if dag_run.conf.get('param_l') is not none %} -l {{ dag_run.conf.param_l }}{% elif params.param_l is not none %} -l {{ params.param_l }}{% else %}{% endif %}"

    # The BERT_DIR_ROOT environment variable (from legacy .dw_init)
    # is assumed to be available as an Airflow Variable.
    # Provide a default value for local testing or if the variable is not set.
    bert_dir_root = Variable.get("BERT_DIR_ROOT", default="/usr/local/bert")
    
    # Placeholder for the path to the migrated k_ausd_v_ta_bp_ref.ksh script.
    # The actual extension (.sh, .py, etc.) and command will depend on its migration strategy.
    k_ausd_script_path = f"{bert_dir_root}/aufbereitung/bin/k_ausd_v_ta_bp_ref.sh" # Assuming a shell script for now

    # Task 2: Execute the core data reconciliation logic (migrated k_ausd_v_ta_bp_ref.ksh).
    # This is a placeholder task. The actual operator (e.g., BigQueryOperator, DataflowTemplateOperator,
    # PythonOperator) and its command/parameters will depend on the detailed migration design
    # for 'k_ausd_v_ta_bp_ref.ksh'. For now, we use a BashOperator assuming it's runnable.
    execute_core_logic = BashOperator(
        task_id='execute_k_ausd_v_ta_bp_ref_logic',
        bash_command=f'''
            set -euo pipefail # Exit immediately if a command exits with a non-zero status.
            echo "Invoking core logic script: {k_ausd_script_path}"
            
            # This command needs to be replaced with the actual execution of the migrated core logic.
            # Examples:
            #   - For BigQuery SQL: 'bq query --use_legacy_sql=false --dataset_id="mydataset" "CALL my_stored_proc(\'{job_kennung}\', \'{dw_eintrags_nr}\');"'
            #   - For Python script: 'python /path/to/migrated_k_ausd.py --job_id "{job_kennung}" --entry_nr "{dw_eintrags_nr}" {param_s_arg} {param_l_arg}'
            #   - For Dataflow: 'gcloud dataflow jobs run my_dataflow_job ...'
            
            # Current placeholder: Execute a shell script
            if [ -f "{k_ausd_script_path}" ]; then
                {k_ausd_script_path} -j "{job_kennung}" -f "{dw_eintrags_nr}" {param_s_arg} {param_l_arg}
            else
                echo "ERROR: Core logic script '{k_ausd_script_path}' not found or not executable. This task is a placeholder."
                exit 1
            fi
            echo "Core logic execution completed (placeholder)."
        ''',
    )

    # Task 3: Log the successful completion of the job.
    end_job = PythonOperator(
        task_id='end_job_logging',
        python_callable=log_job_success,
        op_kwargs={'job_kennung': job_kennung},
    )

    # Define task dependencies to ensure correct execution order.
    start_job >> execute_core_logic >> end_job