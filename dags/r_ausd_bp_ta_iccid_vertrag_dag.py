# Airflow DAG for orchestrating the initial provisioning of base products for BERT.
# Replaces legacy source: vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/r_ausd_bp_ta_iccid_vertrag.ksh

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago
from airflow.models.param import Param
from datetime import datetime, timedelta
import logging

# Initialize logging
log = logging.getLogger(__name__)

# Default arguments for the DAG
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

def _process_parameters(**context):
    """
    Processes the Stichtag (key date) and Wiederanlaufwert (restart value) parameters.
    Defaults them if not provided and stores them in XComs.
    """
    dag_run_conf = context['dag_run'].conf
    params = context['params']

    # Get stichtag from dag_run_conf (manual trigger) or DAG params (default value in UI),
    # default to today's system date if nothing is provided.
    stichtag_str = dag_run_conf.get('stichtag', params.get('stichtag'))
    if not stichtag_str:
        stichtag_dt = datetime.today()
        stichtag_str = stichtag_dt.strftime('%d%m%Y')
        log.info(f"Stichtag not provided, defaulting to system date: {stichtag_str}")
    else:
        try:
            # Validate format DDMMYYYY
            datetime.strptime(stichtag_str, '%d%m%Y')
            log.info(f"Stichtag provided: {stichtag_str}")
        except ValueError:
            raise ValueError(f"Invalid Stichtag format: '{stichtag_str}'. Expected DDMMYYYY.")

    # Get wiederanlaufwert from dag_run_conf or DAG params, default to 0.
    wiederanlaufwert = dag_run_conf.get('wiederanlaufwert', params.get('wiederanlaufwert'))
    if wiederanlaufwert is None or str(wiederanlaufwert) == '': # Also handle empty string from UI for integer param
        wiederanlaufwert = 0
        log.info(f"Wiederanlaufwert not provided, defaulting to: {wiederanlaufwert}")
    else:
        try:
            wiederanlaufwert = int(wiederanlaufwert)
            log.info(f"Wiederanlaufwert provided: {wiederanlaufwert}")
        except ValueError:
            raise ValueError(f"Invalid Wiederanlaufwert: '{wiederanlaufwert}'. Expected an integer.")

    # Push processed parameters to XComs for downstream tasks
    context['ti'].xcom_push(key='processed_stichtag', value=stichtag_str)
    context['ti'].xcom_push(key='processed_wiederanlaufwert', value=str(wiederanlaufwert))


def _invoke_core_processing(**context):
    """
    Placeholder task to invoke the migrated k_ausd_bp_ta_iccid_vertrag.ksh component.
    The actual operator will depend on its migration strategy (e.g., BigQueryOperator, DataflowOperator).
    For now, it logs the command that would be executed.
    """
    stichtag = context['ti'].xcom_pull(key='processed_stichtag', task_ids='process_parameters')
    wiederanlaufwert = context['ti'].xcom_pull(key='processed_wiederanlaufwert', task_ids='process_parameters')

    # Placeholder for JobKennung and DW_EintragsNr. In a real scenario, these would
    # likely come from Airflow Variables, a configuration file, or be fixed constants
    # specific to this job's context.
    job_kennung = "ISBERT_ICCID_VERTRAG"
    dw_eintrags_nr = "DW00123" # Example entry number

    # Construct the command/message that represents the invocation of the
    # migrated k_ausd_bp_ta_iccid_vertrag.ksh equivalent.
    # The original script invoked it as:
    # "${BERT_DIR_ROOT}/aufbereitung/bin/k_ausd_bp_ta_iccid_vertrag.ksh -j $JobKennung -s $p_stichtag -f ${DW_EintragsNr} -l ${p_wiederanlaufWert}"
    # This command string will be replaced by an appropriate Airflow operator (e.g., BigQueryOperator, DataflowPythonOperator)
    # once the k_ausd_bp_ta_iccid_vertrag.ksh script itself is migrated to a GCP service.
    command_to_execute = (
        f"MIGRATED_K_SCRIPT_INVOCATION "
        f"--job_kennung {job_kennung} "
        f"--stichtag {stichtag} "
        f"--entry_nr {dw_eintrags_nr} "
        f"--restart_value {wiederanlaufwert}"
    )

    log.info(f"Simulating core processing invocation with command: {command_to_execute}")
    log.info("This task will be replaced by a BigQueryOperator, DataflowOperator, or similar, "
             "once 'k_ausd_bp_ta_iccid_vertrag.ksh' is migrated to GCP.")


with DAG(
    dag_id='r_ausd_bp_ta_iccid_vertrag_dag',
    start_date=days_ago(1),
    schedule_interval=None,  # This DAG is manually triggered, or via external scheduling
    default_args=default_args,
    catchup=False,
    tags=['isbert', 'orchestration', 'etl'],
    params={
        "stichtag": Param(
            type="string",
            title="Stichtag (Key Date)",
            description="The key date for data extraction in DDMMYYYY format. Defaults to system date if not provided. (e.g., 01012023)",
            pattern="^(0[1-9]|[12][0-9]|3[01])(0[1-9]|1[0-2])(19|20)\d{2}$|^$", # Regex for DDMMYYYY or empty string
            default="",
            max_length=8
        ),
        "wiederanlaufwert": Param(
            type="integer",
            title="Wiederanlaufwert (Restart Value)",
            description="An integer restart value. Defaults to 0 if not provided.",
            minimum=0,
            default=0,
        ),
    }
) as dag:
    process_parameters_task = PythonOperator(
        task_id='process_parameters',
        python_callable=_process_parameters,
        provide_context=True,
    )

    invoke_core_processing_task = PythonOperator(
        task_id='invoke_core_processing',
        python_callable=_invoke_core_processing,
        provide_context=True,
    )

    # Define the task flow
    process_parameters_task >> invoke_core_processing_task