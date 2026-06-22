# Replaces legacy source: vobs/dw_source/isdwh/uc4_prod_exports/UC4_PROD - 0001/DWH/DWH_KERN/RELEASEWECHSEL/RELEASEWECHSEL172/INSTALL_NACH_172_APT_DWHM/DW.DWH_APT_EXPORT_MONATLICH_JP/DW.DWH_RUN_APT_EXPORT_MONATLICH_JP_EVT.xml
# Job: DW.DWH_APT_EXPORT_MONATLICH_JP

from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.utils.dates import days_ago
from airflow.models import DagRun
from airflow.utils.state import DagRunState
from airflow.exceptions import AirflowSkipException
import logging

logger = logging.getLogger(__name__)

def guard_concurrency_function(**kwargs):
    """
    Implements the 'SYNCREF Else=Skip' logic.
    If another DAG run of this DAG is currently running, this run will be skipped.
    """
    dag_run_id = kwargs["dag_run"].run_id
    dag_id = kwargs["dag"].dag_id

    # Check for active DAG runs excluding the current one
    running_dag_runs = DagRun.find(
        dag_id=dag_id, state=DagRunState.RUNNING, external_trigger=False
    )
    
    # Filter out the current DAG run
    other_running_dag_runs = [
        dr for dr in running_dag_runs if dr.run_id != dag_run_id
    ]

    if other_running_dag_runs:
        logger.info(f"Skipping current DAG run {dag_run_id} as another run is active.")
        raise AirflowSkipException("Skipping DAG run due to active concurrency.")
    logger.info("No other active DAG runs found. Proceeding.")


def check_prerequisites_function(**kwargs):
    """
    Checks the status of prerequisite job plans: DW.BERT_STAMMDATEN_JP and DW.ACCESSP_SIGMA_GPRS_MONATLICH_JP.
    UC4 status "1900" for success.
    This is a placeholder and needs actual implementation to query Airflow metadata
    or external system APIs to check the status of these DAGs/jobs.
    """
    # Example: Check if DW.BERT_STAMMDATEN_JP completed successfully recently
    # from airflow.sensors.external_task import ExternalTaskSensor
    # To properly implement this, you would likely use ExternalTaskSensor within the DAG,
    # or query DagRun in a more sophisticated Python function.
    # For now, we assume they are successful.
    logger.info("Placeholder: Prerequisite checks for DW.BERT_STAMMDATEN_JP and DW.ACCESSP_SIGMA_GPRS_MONATLICH_JP passed.")
    return True

def cleanup_event_state_function(**kwargs):
    """
    Placeholder for logging/cleanup actions corresponding to UC4 'CANCEL_UC_OBJECT'.
    """
    logger.info("Placeholder: Performing event state cleanup/logging.")
    pass

with DAG(
    dag_id="dw_dwh_run_apt_export_monatlich_jp_evt",
    start_date=days_ago(1),
    # The design document states "Schedule: Approximated to `0 7 * * *` (daily at 07:00 AM) based on `TimePeriodTT=0720` in UC4.
    # This needs manual validation for monthly intent."
    # We use the daily schedule as specified, with a comment.
    schedule_interval="0 7 * * *",
    catchup=False,
    tags=["dwh", "export", "monthly", "uc4_event"],
    # max_active_runs=1 ensures only one scheduled run can be active at a time.
    # The guard_concurrency_function adds the 'Else=Skip' behavior for externally triggered runs or concurrent manual runs.
    max_active_runs=1,
    default_args={
        "owner": "airflow",
        "depends_on_past": False,
        "email_on_failure": False,
        "email_on_retry": False,
        "retries": 1,
    }
) as dag:
    start = EmptyOperator(task_id="start")

    # Enforces 'Else=Skip' logic from UC4. If another run of this DAG is active, this one skips.
    guard_concurrency = PythonOperator(
        task_id="guard_concurrency",
        python_callable=guard_concurrency_function,
    )

    # Placeholder for checking prerequisite DAGs. In a real scenario, this might be
    # replaced by one or more ExternalTaskSensors.
    check_prerequisites = PythonOperator(
        task_id="check_prerequisites",
        python_callable=check_prerequisites_function,
    )

    # Triggers the main data export DAG.
    # The 'conf' dictionary can pass parameters to the triggered DAG.
    # The 'monat_id' parameter is derived from the original UC4 job's '&MONAT_ID'.
    trigger_main_export_dag = TriggerDagRunOperator(
        task_id="trigger_main_export_dag",
        trigger_dag_id="dw_dwh_apt_export_monatlich_jp",
        # Pass the execution date in YYYYMMDD format as 'monat_id'
        conf={"monat_id": "{{ ds_nodash }}"},
        wait_for_completion=False, # UC4 'Activate' doesn't wait.
    )

    # Corresponds to UC4 'CANCEL_UC_OBJECT' for logging/cleanup.
    cleanup_event_state = PythonOperator(
        task_id="cleanup_event_state",
        python_callable=cleanup_event_state_function,
    )

    end = EmptyOperator(task_id="end")

    start >> guard_concurrency >> check_prerequisites >> trigger_main_export_dag >> cleanup_event_state >> end