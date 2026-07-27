"""
DAG to mirror Carmen period definitions.

This workflow migrates the UC4 Unix job DW.BERT_AUSD_V_TA_PERIOD.
The primary function of this object is to mirror Carmen period definitions
by executing a shell script (r_ausd_v_ta_period.ksh) after loading target
environment variables. This job is treated as an externally triggered,
standalone workflow.
"""

from datetime import datetime, timedelta
from airflow import DAG
from airflow.models import Variable
from airflow.operators.empty import EmptyOperator

# ── GCP CONFIGURATION ────────────────────────────────────────────────────────
GCP_PROJECT = Variable.get("GCP_PROJECT")
GCP_REGION = Variable.get("GCP_REGION")
DATAPROC_REGION = Variable.get("DATAPROC_REGION")
DATAPROC_CLUSTER = Variable.get("DATAPROC_CLUSTER")
GCS_BUCKET = Variable.get("GCS_BUCKET")

# ── JOB-SPECIFIC CONFIGURATION ──────────────────────────────────────────────
JOB_CONFIG = {
    "DWH_JOB_KENNUNG": "AUSD_V_TA_PERIOD",
}

# ── DEFAULT ARGUMENTS ────────────────────────────────────────────────────────
DEFAULT_ARGS = {
    'owner': 'DW',
    'depends_on_past': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# # REVIEW: The source object is a standalone JOBS_UNIX object with no parent JOBP.
# A wrapper DAG (dw_bert_ausd_v_ta_period) has been generated to hold this task.
# Verify if this task should instead be incorporated into a larger parent DAG.

dag = DAG(
    dag_id='dw_bert_ausd_v_ta_period',
    default_args=DEFAULT_ARGS,
    description='Mirror Carmen period definitions',
    schedule=None,
    start_date=datetime(2023, 1, 1),
    catchup=False,
    max_active_runs=1,
    is_paused_upon_creation=False,
    tags=['uc4_migration', 'standalone_job'],
)

with dag:
    # Task to mirror Carmen period definitions.
    # REVIEW-STRUCT: launcher command not recognised during conversion — implement the correct operator once its behaviour is confirmed
    # Original raw command: &HOME/SQL/aktuell/aufbereitung/bin/r_ausd_v_ta_period.ksh
    bert_ausd_v_ta_period = EmptyOperator(
        task_id='bert_ausd_v_ta_period',
    )

    # Single-task workflow execution dependency mapping
    bert_ausd_v_ta_period