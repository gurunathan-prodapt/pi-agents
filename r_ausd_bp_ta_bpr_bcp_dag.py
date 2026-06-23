# Legacy Source: vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/r_ausd_bp_ta_bpr_bcp.ksh
# Job: vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/r_ausd_bp_ta_bpr_bcp.ksh
"""
Airflow DAG for orchestrating the initial provision of selected "Basisprodukte" (base products)
for the BERT system, replacing the legacy KornShell script r_ausd_bp_ta_bpr_bcp.ksh.

This DAG handles parameter parsing, environment setup, logging, and invokes
the core processing logic, which is expected to be migrated separately.
"""

from __future__ import annotations

import pendulum
from datetime import datetime

from airflow.models.dag import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from airflow.models.variable import Variable

# Import custom utility functions
from airflow_utils import (
    DWDate_Gib_Zeitraum,
    pruefeParameterGesetzt,
    DWMSG_init_job,
    DWMSG_get_eintrags_nr,
    DWMSG_log_message,
    DWMSG_Fehlerbehandlung,
    DWMSG_SetzeStatusOK,
)

# --- Configuration Variables (replace with Airflow Variables or Connections as needed) ---
# BERT_DIR_ROOT environment variable equivalent from the legacy script.
# This should ideally be stored in Airflow Variables.
BERT_DIR_ROOT = Variable.get("BERT_DIR_ROOT", "/app/bert")
LOG_BASE_DIR = Variable.get("LOG_BASE_DIR", "/var/log/airflow/bert")
JOB_KENNUNG_PREFIX = "R_AUSD_BP_TA_BPR_BCP" # Base job identifier

# --- Callbacks for error handling ---
def dag_failure_callback(context):
    """
    Callback function to handle DAG failures.
    """
    task_instance = context.get("task_instance")
    exception = context.get("exception")
    dag_run = context.get("dag_run")

    job_kennung = f"{JOB_KENNUNG_PREFIX}_{dag_run.run_id}"
    # In a real scenario, the eintrags_nr might be passed down or retrieved.
    # For now, generate a placeholder.
    eintrags_nr = datetime.now().strftime('%Y%m%d%H%M%S%f') # Placeholder if not available
    error_message = f"Task '{task_instance.task_id}' failed in DAG '{dag_run.dag_id}': {exception}"
    DWMSG_Fehlerbehandlung(job_kennung, eintrags_nr, error_message)
    DWMSG_log_message(job_kennung, eintrags_nr, f"DAG run failed. Check logs for details. Exception: {exception}", level='ERROR')


with DAG(
    dag_id="r_ausd_bp_ta_bpr_bcp_dag",
    start_date=pendulum.datetime(2023, 1, 1, tz="UTC"),
    catchup=False,
    schedule=None, # This is an on-demand orchestrator, schedule can be set if needed
    tags=["bert", "orchestration", "basisprodukte"],
    params={
        "stichtag": None, # Key date in DDMMYYYY format, e.g., "01012023"
        "wiederanlaufwert": 0, # Restart value, defaults to 0
    },
    on_failure_callback=dag_failure_callback,
    doc_md="""
    ### Airflow DAG for r_ausd_bp_ta_bpr_bcp.ksh Migration

    This DAG replaces the legacy KornShell script `r_ausd_bp_ta_bpr_bcp.ksh`.
    It orchestrates the initial provision of selected "Basisprodukte" (base products)
    for the BERT system.

    **Parameters:**
    - `stichtag` (str, optional): Key date in DDMMYYYY format. Defaults to current system date.
    - `wiederanlaufwert` (int, optional): Restart value. Defaults to 0.

    The DAG performs:
    1. Environment setup and logging initialization.
    2. Parameter validation.
    3. Invocation of the core processing logic (`k_ausd_bp_ta_bpr_bcp.ksh` equivalent,
       which is expected to be migrated separately).
    4. Success/failure logging.
    """,
) as dag:
    
    def _initialize_job_context(**kwargs):
        """Initializes job-specific context and parameters."""
        ds = kwargs["ds"] # execution_date in YYYY-MM-DD format
        params = kwargs["params"]
        
        # Determine stichtag (key date)
        stichtag = params.get("stichtag")
        if not stichtag:
            stichtag = DWDate_Gib_Zeitraum() # Get current date in DDMMYYYY
            DWMSG_log_message(job_kennung=kwargs["ti"].xcom_pull(task_ids='initialize_job_context', key='job_kennung'),
                              eintrags_nr=kwargs["ti"].xcom_pull(task_ids='initialize_job_context', key='eintrags_nr'),
                              message=f"Stichtag not provided, defaulting to current system date: {stichtag}")
        
        # Determine wiederanlaufwert
        wiederanlaufwert = params.get("wiederanlaufwert", 0)

        # Generate a unique job identifier for this run
        job_kennung_run_id = f"{JOB_KENNUNG_PREFIX}_{kwargs['run_id']}"
        eintrags_nr = DWMSG_get_eintrags_nr(job_kennung_run_id)
        
        # Construct log file name
        log_date = datetime.strptime(stichtag, '%d%m%Y').strftime('%Y%m%d')
        log_file = f"{LOG_BASE_DIR}/{JOB_KENNUNG_PREFIX}_{log_date}_{eintrags_nr}.log"

        DWMSG_init_job(job_kennung_run_id, log_file)
        DWMSG_log_message(job_kennung_run_id, eintrags_nr, f"Job Parameters: Stichtag={stichtag}, Wiederanlaufwert={wiederanlaufwert}")

        # Push context variables to XCom for subsequent tasks
        kwargs["ti"].xcom_push(key="stichtag", value=stichtag)
        kwargs["ti"].xcom_push(key="wiederanlaufwert", value=wiederanlaufwert)
        kwargs["ti"].xcom_push(key="job_kennung", value=job_kennung_run_id)
        kwargs["ti"].xcom_push(key="eintrags_nr", value=eintrags_nr)
        kwargs["ti"].xcom_push(key="log_file", value=log_file)


    initialize_job_context = PythonOperator(
        task_id="initialize_job_context",
        python_callable=_initialize_job_context,
    )

    def _validate_parameters(**kwargs):
        """Validates the extracted parameters."""
        stichtag = kwargs["ti"].xcom_pull(task_ids="initialize_job_context", key="stichtag")
        wiederanlaufwert = kwargs["ti"].xcom_pull(task_ids="initialize_job_context", key="wiederanlaufwert")
        job_kennung = kwargs["ti"].xcom_pull(task_ids='initialize_job_context', key='job_kennung')
        eintrags_nr = kwargs["ti"].xcom_pull(task_ids='initialize_job_context', key='eintrags_nr')

        # Parameters to validate. In legacy script, only stichtag might be explicitly checked
        # if other parameters are always set by getopts or have defaults.
        # Here, we assume stichtag is critical.
        params_to_check = {
            "stichtag": stichtag,
        }
        
        try:
            pruefeParameterGesetzt(params_to_check, ["stichtag"])
            DWMSG_log_message(job_kennung, eintrags_nr, "Parameter validation successful.")
        except ValueError as e:
            DWMSG_log_message(job_kennung, eintrags_nr, f"Parameter validation failed: {e}", level='ERROR')
            raise # Re-raise to fail the task


    validate_parameters_task = PythonOperator(
        task_id="validate_parameters",
        python_callable=_validate_parameters,
    )

    # The core logic script k_ausd_bp_ta_bpr_bcp.ksh is a critical dependency
    # that requires its own migration. This task acts as a placeholder
    # for its future migrated form (e.g., BigQueryOperator, DataprocSubmitJobOperator).
    # For now, it's a BashOperator simulating its invocation.
    invoke_core_logic_task = BashOperator(
        task_id="invoke_core_logic",
        bash_command=f"""
            JOB_KENNUNG="{{{{ ti.xcom_pull(task_ids='initialize_job_context', key='job_kennung') }}}}}"
            STICHTAG="{{{{ ti.xcom_pull(task_ids='initialize_job_context', key='stichtag') }}}}}"
            EINTRAGS_NR="{{{{ ti.xcom_pull(task_ids='initialize_job_context', key='eintrags_nr') }}}}}"
            WIEDERANLAUFWERT="{{{{ ti.xcom_pull(task_ids='initialize_job_context', key='wiederanlaufwert') }}}}}"
            LOG_FILE="{{{{ ti.xcom_pull(task_ids='initialize_job_context', key='log_file') }}}}}"

            echo "--- Simulating invocation of k_ausd_bp_ta_bpr_bcp.ksh equivalent ---"
            echo "This task will eventually run the migrated core logic (e.g., BigQuery SQL or PySpark job)."
            echo "Legacy command parameters:"
            echo "-j $JOB_KENNUNG"
            echo "-s $STICHTAG"
            echo "-f $EINTRAGS_NR"
            echo "-l $WIEDERANLAUFWERT"
            echo "Output redirected to: $LOG_FILE"
            
            # Placeholder for actual command execution
            # Example: gcloud dataproc jobs submit pyspark --cluster=my-cluster ...
            # Example: bq query --use_legacy_sql=false 'SELECT 1;'
            
            # Simulate some work
            sleep 5 
            echo "Core logic simulation complete."

            # Exit with 0 for success, non-zero for failure (handled by Airflow's retry mechanism)
            exit 0
        """,
    )

    def _log_success(**kwargs):
        """Logs job success."""
        job_kennung = kwargs["ti"].xcom_pull(task_ids='initialize_job_context', key='job_kennung')
        eintrags_nr = kwargs["ti"].xcom_pull(task_ids='initialize_job_context', key='eintrags_nr')
        DWMSG_SetzeStatusOK(job_kennung, eintrags_nr)

    log_success_task = PythonOperator(
        task_id="log_success",
        python_callable=_log_success,
    )

    # Define task dependencies
    initialize_job_context >> validate_parameters_task >> invoke_core_logic_task >> log_success_task