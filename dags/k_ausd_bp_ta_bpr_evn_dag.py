"""
Airflow DAG for orchestrating k_ausd_bp_ta_bpr_evn migration.

This DAG handles environment parameter loading, validation, logging, 
and execution of the BigQuery stored procedure r_ausd_bp_ta_bpr_evn.
"""

from datetime import datetime, timedelta
import re
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.google.cloud.operators.bigquery import BigQueryInsertJobOperator
from airflow.operators.empty import EmptyOperator

# --- ENVIRONMENT-SPECIFIC CONFIGURATION ---
# Adjust these values to match your target environment
GCP_PROJECT_ID = "${GCP_PROJECT_ID}"  # e.g., "my-gcp-project-id"
BQ_DATASET = "${BQ_DATASET}"          # e.g., "my_bq_dataset"

DEFAULT_ARGS = {
    "owner": "data-platform",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

def validate_inputs(**kwargs):
    """
    Validates that necessary command-line/runtime parameters are specified
    and verifies the date format of the Stichtag parameter (DDMMYYYY).
    """
    dag_run = kwargs.get("dag_run")
    if not dag_run or not dag_run.conf:
        raise ValueError(
            "No execution configuration found. Please trigger this DAG with config. "
            "Example: {\"p_JobKennung\": \"job1\", \"p_EintragsNr\": \"123\", \"p_Stichtag\": \"31122023\"}"
        )
    
    conf = dag_run.conf
    p_JobKennung = conf.get("p_JobKennung")
    p_EintragsNr = conf.get("p_EintragsNr")
    p_Stichtag = conf.get("p_Stichtag")
    
    # Validate mandatory parameters (analogous to legacy pruefeParameterGesetzt)
    missing_params = []
    if not p_JobKennung:
        missing_params.append("p_JobKennung (Jobkennung)")
    if not p_EintragsNr:
        missing_params.append("p_EintragsNr (EintragsNr)")
    if not p_Stichtag:
        missing_params.append("p_Stichtag (Stichtag)")
        
    if missing_params:
        raise ValueError(f"Mandatory parameter(s) missing: {', '.join(missing_params)}")

    # Validate date format (analogous to legacy DWDate_Datum_Check ... 'DDMMYYYY')
    if not re.match(r"^\d{8}$", str(p_Stichtag)):
        raise ValueError(f"Stichtag '{p_Stichtag}' does not match format DDMMYYYY (8 digits required)")
    try:
        datetime.strptime(str(p_Stichtag), "%d%m%Y")
    except ValueError:
        raise ValueError(f"Stichtag '{p_Stichtag}' is not a valid calendar date")

with DAG(
    dag_id="k_ausd_bp_ta_bpr_evn_dag",
    default_args=DEFAULT_ARGS,
    description="Orchestration DAG replacing k_ausd_bp_ta_bpr_evn.ksh shell script",
    schedule_interval=None,
    start_date=datetime(2023, 1, 1),
    catchup=False,
    tags=["isbert", "migration", "bigquery"],
) as dag:

    start = EmptyOperator(task_id="start")

    validate_parameters = PythonOperator(
        task_id="validate_parameters",
        python_callable=validate_inputs,
        provide_context=True,
    )

    # Task to call the migrated BigQuery stored procedure
    execute_stored_procedure = BigQueryInsertJobOperator(
        task_id="execute_stored_procedure",
        configuration={
            "query": {
                "query": """
                    CALL `{gcp_project_id}.{bq_dataset}.r_ausd_bp_ta_bpr_evn`(
                      '{{ dag_run.conf.get("p_JobKennung", "") }}',
                      '{{ dag_run.conf.get("p_EintragsNr", "") }}',
                      '{{ dag_run.conf.get("p_Stichtag", "") }}',
                      '{{ dag_run.conf.get("p_wiederanlaufWert", "0") }}'
                    )
                """.format(gcp_project_id=GCP_PROJECT_ID, bq_dataset=BQ_DATASET),
                "useLegacySql": False,
            }
        },
    )

    end = EmptyOperator(task_id="end")

    start >> validate_parameters >> execute_stored_procedure >> end