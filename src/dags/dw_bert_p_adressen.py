"""
Legacy Job: BERT_P_ADRESSEN (Aufbereitung der Adressdaten)
Legacy Source: DW.BERT_P_ADRESSEN.xml (UC4 Job Orchestration)
Target Platform: Google Cloud Composer (Apache Airflow 2.x)
Description: Orchestrates the delta preparation pipeline of master address data inside BigQuery.
"""

from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.sensors.external_task import ExternalTaskSensor
from airflow.providers.google.cloud.operators.bigquery import BigQueryInsertJobOperator
from airflow.exceptions import AirflowSkipException
from airflow.models import Variable

# Initialize Structured Cloud Logging
from utils.logging_helper import get_cloud_logger, log_job_event

logger = get_cloud_logger("BERT_P_ADRESSEN")

def guard_active_run(dag_id: str, **context) -> None:
    """
    Simulates UC4 lock semantics: DW.BERT_ADRESS_SYNC (Else=Skip).
    If another execution of this DAG is currently active, skip current execution path.
    """
    from airflow.models import DagRun
    from airflow.utils.state import State

    current_run_id = context.get("run_id")
    active_runs = DagRun.find(dag_id=dag_id, state=State.RUNNING)
    other_active_runs = [r for r in active_runs if r.run_id != current_run_id]

    if other_active_runs:
        log_job_event(
            logger, 
            job_name="BERT_P_ADRESSEN", 
            event_type="GUARD_ACTIVE_RUN", 
            status="SKIPPED", 
            extra_info={"message": f"Another run of {dag_id} is already in progress. Skipping execution to avoid overlaps."}
        )
        raise AirflowSkipException(
            f"Sync Object constraint failed: {dag_id} already running. Skipping execution."
        )
    logger.info("No competing active runs detected. Safe to proceed.")

# Configuration & Variables
PROJECT_ID = Variable.get("gcp_project_id", default_var="gcp-dwh-prod")
GCP_CONN_ID = Variable.get("gcp_conn_id", default_var="google_cloud_default")

default_args = {
    "owner": "DW.UNIX.ISBERT",
    "depends_on_past": False,
    "email_on_failure": True,
    "email": ["sa-bert-dwh-prod@gcp-project.iam.gserviceaccount.com"],
    "retries": 1,
    "retry_delay": timedelta(minutes=15),
}

with DAG(
    dag_id="dw_bert_p_adressen",
    default_args=default_args,
    description="Orchestrator for BERT master address data processing",
    schedule_interval=None,  # Typically triggered on upstream file arrivals or custom plans
    start_date=datetime(2026, 4, 21),
    catchup=False,
    max_active_runs=1,
    tags=["bert", "dw", "address"],
) as dag:

    # Task 1: Check for running DAG instances and skip if active (replaces DW.BERT_ADRESS_SYNC lock)
    task_guard_active_run = PythonOperator(
        task_id="guard_active_run",
        python_callable=guard_active_run,
        op_kwargs={"dag_id": "dw_bert_p_adressen"},
        provide_context=True,
    )

    # Task 2: Wait for Business Partner DAG success (replaces DW.BERT_GP_SYNC lock)
    sensor_gp = ExternalTaskSensor(
        task_id="sensor_gp",
        external_dag_id="dw_bert_p_geschaeftsp",
        external_task_id=None,  # Wait for the completion of the target DAG
        allowed_states=["success"],
        failed_states=["failed", "upstream_failed"],
        mode="reschedule",
        poke_interval=120,
        timeout=14400,  # 4 hours SLA match
    )

    # Task 3: Wait for Invoice Recipient DAG success (replaces DW.BERT_RECH_SYNC lock)
    sensor_rech = ExternalTaskSensor(
        task_id="sensor_rech",
        external_dag_id="dw_bert_p_rechempf",
        external_task_id=None,  # Wait for the completion of the target DAG
        allowed_states=["success"],
        failed_states=["failed", "upstream_failed"],
        mode="reschedule",
        poke_interval=120,
        timeout=14400,  # 4 hours SLA match
    )

    # Task 4: Execute BigQuery Stored Procedure (replaces r_ausd_adressen.ksh)
    task_execute_sp = BigQueryInsertJobOperator(
        task_id="dw_bert_p_adressen_run",
        configuration={
            "query": {
                "query": f"CALL `{PROJECT_ID}.dw_bert.sp_prep_adressen`();",
                "useLegacySql": False,
            }
        },
        gcp_conn_id=GCP_CONN_ID,
    )

    # Define orchestration workflow and strict synchronization rules
    task_guard_active_run >> [sensor_gp, sensor_rech] >> task_execute_sp