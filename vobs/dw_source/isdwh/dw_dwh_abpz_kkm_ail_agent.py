"""
DAG: dw_dwh_abpz_kkm_ail_agent
Migrated from: DW.DWH_ABPZ_KKM_AIL_AGENT (UC4 + KSH + Ab Initio)
Target Engine: Dataproc Serverless + BigQuery
"""

import os
from datetime import datetime, timedelta
from airflow import DAG
from airflow.models import Variable
from airflow.operators.python import PythonOperator
from airflow.providers.google.cloud.operators.bigquery import BigQueryValueCheckOperator
from airflow.providers.google.cloud.operators.dataproc import DataprocCreateBatchOperator

# -----------------------------------------------------------------------------
# GLOBAL VARIABLES (ENVIRONMENT POLICY MAPPING)
# -----------------------------------------------------------------------------
GCP_PROJECT = Variable.get("GCP_PROJECT")
DATAPROC_REGION = Variable.get("DATAPROC_REGION")
GCS_BUCKET = Variable.get("GCS_BUCKET")
BQ_DATASET = Variable.get("BQ_DATASET")

# -----------------------------------------------------------------------------
# JOB-SPECIFIC PARAMETERS
# -----------------------------------------------------------------------------
JOB_KENNUNG = "ABPZ_KKM_AIL_AGENT"
LOOKUP_FILE_NAME = "AgentADSLookup.txt"
RUECKBLICK_DAYS = 84

default_args = {
    "owner": "istns",
    "depends_on_past": False,
    "start_date": datetime(2023, 6, 1),
    "email_on_failure": True,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

def register_start(**context):
    """Start hook representing DW.DWH_ADM_JOB_MONITOR_START."""
    print(f"Added {JOB_KENNUNG} with run_id {context['run_id']}")

def register_end(task_instance, **context):
    """End hook representing LESE_LOG and DW.DWH_ADM_JOB_MONITOR_END."""
    batch_state = task_instance.xcom_pull(
        task_ids="execute_pyspark_write_agent_ads_lookup"
    )
    print(f"Dataproc Batch complete execution state: {batch_state}")
    print(f"Jobkennung {JOB_KENNUNG} eingetragen")
    print("Die Abarbeitung des Rahmenskriptes wurde ohne erkennbare Fehler beendet.")

with DAG(
    dag_id="dw_dwh_abpz_kkm_ail_agent",
    default_args=default_args,
    schedule_interval="@daily",
    catchup=False,
    max_active_runs=1,
) as dag:

    start_monitor = PythonOperator(
        task_id="dw_dwh_adm_job_monitor_start",
        python_callable=register_start,
    )

    poll_ab_initio_status = BigQueryValueCheckOperator(
        task_id="poll_ab_initio_status_barrier",
        sql=f"SELECT status_val FROM `{GCP_PROJECT}.{BQ_DATASET}.DW_ADM_AB_INITIO_VAR` WHERE app_name = 'STATUS_DWH'",
        pass_value="go",
        use_legacy_sql=False,
    )

    batch_execution_id = (
        f"batch-{JOB_KENNUNG.lower().replace('_', '-')}-{{{{ ts_nocase_str | lower }}}}"
    )

    execute_pyspark_lookup = DataprocCreateBatchOperator(
        task_id="execute_pyspark_write_agent_ads_lookup",
        project_id=GCP_PROJECT,
        region=DATAPROC_REGION,
        batch_id=batch_execution_id,
        batch={
            "pyspark_batch": {
                "main_python_file_uri": f"gs://{GCS_BUCKET}/code/tmp5bupf309_write_agent_lookup.py",
                "args": [
                    "--lookback-days",
                    str(RUECKBLICK_DAYS),
                    "--output-path",
                    f"gs://{GCS_BUCKET}/lookups/{LOOKUP_FILE_NAME}",
                    "--job-kennung",
                    JOB_KENNUNG,
                ],
            },
            "environment_config": {
                "execution_config": {
                    "subnetwork_uri": "default"
                }
            },
        },
    )

    end_monitor = PythonOperator(
        task_id="dw_dwh_adm_job_monitor_end",
        python_callable=register_end,
    )

    start_monitor >> poll_ab_initio_status >> execute_pyspark_lookup >> end_monitor