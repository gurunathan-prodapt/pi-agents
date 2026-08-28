"""
Airflow DAG to execute DW.DWH_PFIS_MPS_VBA_KORR.

Performs data corrections on unidentifiable VBA IDs (Korrektur nicht ermittelbarer VBA-IDs).
Runs a backend script/binary located at $HOME/aktuell/pruef/is/bin/r_pfis_mps_vba_korrektur.
This workflow is fully idempotent and safe to restart or rerun directly without manual cleanup.
"""

from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.operators.bash import BashOperator
from airflow.providers.google.cloud.operators.bigquery import BigQueryInsertJobOperator
from airflow.models import Variable

# ── GLOBAL CONFIGURATION ───────────────────────────────────────────────────────
# Sourced at runtime from Airflow Variables to avoid hardcoded environment values.
GCP_PROJECT = Variable.get("GCP_PROJECT")

# ── DEFAULT ARGS ───────────────────────────────────────────────────────────────
DEFAULT_ARGS = {
    'owner': 'airflow',
    'depends_on_past': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# ── DAG DEFINITION ─────────────────────────────────────────────────────────────
with DAG(
    dag_id='dw_dwh_pfis_mps_vba_korr',
    default_args=DEFAULT_ARGS,
    description='Korrektur nicht ermittelbarer VBA-IDs',
    schedule=None,
    start_date=datetime(2023, 1, 1),
    catchup=False,
    max_active_runs=1,
    is_paused_upon_creation=False,
    tags=['migrated_uc4', 'dwh', 'pfis'],
) as dag:

    # Orchestrator Initiation Task
    dw_dwh_pfis_mps_vba_korr_task = EmptyOperator(
        task_id='dw_dwh_pfis_mps_vba_korr_task',
    )

    # Executes the migrated Python script r_pfis_mps_vba_korrektur.py
    # Passes the job-specific variable DWH_JOB_KENNUNG via environment variables.
    run_r_pfis_mps_vba_korrektur = BashOperator(
        task_id='run_r_pfis_mps_vba_korrektur',
        bash_command='python -m r_pfis_mps_vba_korrektur',
        env={
            'DWH_JOB_KENNUNG': 'PFIS_MPS_VBA_KORR',
            'GCP_PROJECT': GCP_PROJECT,
        },
    )

    # Executes the migrated SQL query d_pfis_mps_vba_korrektur.sql in BigQuery
    run_d_pfis_mps_vba_korrektur_sql = BigQueryInsertJobOperator(
        task_id='run_d_pfis_mps_vba_korrektur_sql',
        configuration={
            "query": {
                "query": "{% include 'd_pfis_mps_vba_korrektur.sql' %}",
                "useLegacySql": False,
                "queryParameters": [
                    {
                        "name": "P1",
                        "parameterType": {"type": "INT64"},
                        "parameterValue": {"value": "{{ task_instance.try_number }}"}
                    }
                ]
            }
        },
    )

    # ── DEPENDENCY GRAPH ───────────────────────────────────────────────────────────
    dw_dwh_pfis_mps_vba_korr_task >> run_r_pfis_mps_vba_korrektur >> run_d_pfis_mps_vba_korrektur_sql