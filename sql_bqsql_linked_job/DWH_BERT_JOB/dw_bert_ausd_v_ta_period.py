"""
DAG to mirror Carmen period definitions.
Migrated from UC4 object DW.BERT_AUSD_V_TA_PERIOD.
"""

import os
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.models import Variable

# === GLOBAL (Environment-Wide) Variables ===
GCP_PROJECT = os.environ.get("GCP_PROJECT") or Variable.get("GCP_PROJECT")
GCS_BUCKET = os.environ.get("GCS_BUCKET") or Variable.get("GCS_BUCKET")

# === JOB-SPECIFIC Variables ===
DWH_JOB_KENNUNG = Variable.get("DWH_JOB_KENNUNG", default_var="AUSD_V_TA_PERIOD")

# === Default Args ===
DEFAULT_ARGS = {
    'owner': 'airflow',
    'depends_on_past': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# === DAG Definition ===
with DAG( 
    dag_id='dw_bert_ausd_v_ta_period',
    default_args=DEFAULT_ARGS,
    description='Mirror Carmen period definitions - Migrated from DW.BERT_AUSD_V_TA_PERIOD',
    start_date=datetime(2023, 1, 1),
    schedule=None,
    catchup=False,
    max_active_runs=1,
    is_paused_upon_creation=False,
) as dag:

    # ── Task: dw_bert_ausd_v_ta_period_task ──────────────
    # The legacy launcher command `&HOME/SQL/aktuell/aufbereitung/bin/r_ausd_v_ta_period.ksh` 
    # has been converted into a Python wrapper script `r_ausd_v_ta_period.py` per the target execution plan.
    # It executes the SQL synchronously via the Google Cloud BigQuery client library to ensure 
    # correct execution tracking and logging.
    dw_bert_ausd_v_ta_period_task = BashOperator(
        task_id="dw_bert_ausd_v_ta_period_task",
        bash_command="python3 {{ params.script_path }}",
        params={
            "script_path": "/opt/airflow/scripts/sql_bqsql_linked_job/isbert/aufbereitung/bin/r_ausd_v_ta_period.py"
        },
        env={
            "DWH_JOB_KENNUNG": DWH_JOB_KENNUNG,
            "GCP_PROJECT": GCP_PROJECT,
            "GCS_BUCKET": GCS_BUCKET,
        },
    )

    # Single-task execution flow
    dw_bert_ausd_v_ta_period_task