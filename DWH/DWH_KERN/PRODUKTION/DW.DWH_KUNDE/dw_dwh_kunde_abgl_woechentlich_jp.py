"""
DAG: dw_dwh_kunde_abgl_woechentlich_jp
Target Platform: Cloud Composer (Airflow) + BigQuery
Description: Weekly customer master data reconciliation pipeline.
             Orchestrates logging steps and dynamic BigQuery executions.
"""

from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.google.cloud.operators.bigquery import BigQueryInsertJobOperator

# Import modular execution and validation functions
from dw_dwh_kunde.bin.r_abgl_kunde_woech import (
    get_gcp_variable,
    pre_execution_logging,
    post_execution_logging,
)

# ── Dynamic Environment Configurations ───────────────────
# Variables fall back to defaults or fail-fast with runtime errors
GCP_PROJECT = get_gcp_variable("GCP_PROJECT")
BQ_LOCATION = get_gcp_variable("BQ_LOCATION", default="EU")
BQ_DATASET = get_gcp_variable("BQ_DATASET", default="dwh_kunde")

# ── Orchestration Properties (UC4 to DAG) ────────────────
DEFAULT_ARGS = {
    "owner": "airflow",
    "depends_on_past": False,
    "start_date": datetime(2024, 10, 7),
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    dag_id="dw_dwh_kunde_abgl_woechentlich_jp",
    default_args=DEFAULT_ARGS,
    description="Woechentlicher Adressabgleich der Kundenstammdaten (KUNDE) gegen das Referenzsystem",
    schedule_interval="0 3 * * 0",  # Runs weekly on Sundays at 03:00 AM
    catchup=False,
    max_active_runs=1,
    is_paused_upon_creation=True,
    tags=["dwh", "kunde", "weekly", "reconciliation"],
) as dag:

    # ── Task: Pre-Execution Log ───────────────────────────
    # Emulates start sequence parameters of legacy shell scripts
    task_pre_log = PythonOperator(
        task_id="pre_log",
        python_callable=pre_execution_logging,
        templates_dict={"lauf_woche": "{{ ds_nodash }}"},
        provide_context=True,
    )

    # ── Task: Execution (Reconciliation query) ─────────────
    # Executes converted BigQuery SQL and sets template parameters
    task_run_reconciliation = BigQueryInsertJobOperator(
        task_id="dw_dwh_kunde_abgl_woechentlich_js",
        configuration={
            "query": {
                "query": f"""
                    -- Converted Oracle SQL wrapper logic targeting Google BigQuery syntax
                    CREATE OR REPLACE TABLE `{GCP_PROJECT}.{BQ_DATASET}.d_abgl_kunde_woech_results` AS
                    SELECT 
                      PARSE_DATE('%Y%m%d', @lauf_woche) AS execution_date,
                      'KUNDE_ABGL_WOECHENTLICH' AS job_kennung,
                      COUNT(*) AS dummy_diff_count
                    FROM `{GCP_PROJECT}.{BQ_DATASET}.kunde_master` m
                    LEFT JOIN `{GCP_PROJECT}.{BQ_DATASET}.kunde_reference` r
                      ON m.kunde_id = r.kunde_id
                    WHERE m.adresse != r.adresse;
                """,
                "useLegacySql": False,
                "parameterMode": "NAMED",
                "queryParameters": [
                    {
                        "name": "lauf_woche",
                        "parameterType": {"type": "STRING"},
                        "parameterValue": {"value": "{{ ds_nodash }}"}
                    }
                ]
            }
        },
        location=BQ_LOCATION,
    )

    # ── Task: Post-Execution Log ──────────────────────────
    # Reads validation metrics from BigQuery and logs out output
    task_post_log = PythonOperator(
        task_id="post_log",
        python_callable=post_execution_logging,
        templates_dict={"lauf_woche": "{{ ds_nodash }}"},
        provide_context=True,
    )

    # ── Dependency Graph Definition ───────────────────────
    task_pre_log >> task_run_reconciliation >> task_post_log