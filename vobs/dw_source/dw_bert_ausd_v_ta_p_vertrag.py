"""
DAG to update contract information regarding twin-bill.
Derived from UC4 Job: DW.BERT_AUSD_V_TA_P_VERTRAG
"""

from datetime import datetime, timedelta
from airflow import DAG
from airflow.models import Variable
from airflow.operators.bash import BashOperator
from airflow.operators.empty import EmptyOperator

# ── ENVIRONMENT CONFIGURATIONS ────────────────────────────────────────────────
# All environment configurations are retrieved dynamically at runtime per policy.
GCP_PROJECT = Variable.get("GCP_PROJECT")
GCP_REGION = Variable.get("GCP_REGION")
GCS_BUCKET = Variable.get("GCS_BUCKET")
WORKSPACE_ROOT = Variable.get("WORKSPACE_ROOT")

# ── JOB-SPECIFIC VARIABLES ────────────────────────────────────────────────────
# Scheduler-set variables and job identification.
DWH_JOB_KENNUNG = Variable.get("DWH_JOB_KENNUNG", default_var="AUSD_V_TA_P_VERTRAG")

# ── DEFAULT ARGS ─────────────────────────────────────────────────────────────
DEFAULT_ARGS = { 
    'owner': 'airflow',
    'depends_on_past': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# ── DAG DEFINITION ───────────────────────────────────────────────────────────
with DAG( 
    dag_id='dw_bert_ausd_v_ta_p_vertrag', 
    default_args=DEFAULT_ARGS, 
    description='Update contract information regarding twin-bill', 
    schedule=None, 
    start_date=datetime(2023, 1, 1), 
    catchup=False, 
    max_active_runs=1, 
    is_paused_upon_creation=False, 
) as dag:

    # Step 2: KSH Control Script
    # Runs the migrated r_ausd_v_ta_p_vertrag.py
    r_ausd_v_ta_p_vertrag_task = BashOperator( 
        task_id='r_ausd_v_ta_p_vertrag',
        bash_command='python3 {{ var.value.WORKSPACE_ROOT }}/SQL/aktuell/aufbereitung/bin/r_ausd_v_ta_p_vertrag.py',
        env={
            'DWH_JOB_KENNUNG': DWH_JOB_KENNUNG,
            'WORKSPACE_ROOT': WORKSPACE_ROOT,
            'GCP_PROJECT': GCP_PROJECT,
            'GCS_BUCKET': GCS_BUCKET,
        },
        do_xcom_push=True,
    )

    # Step 3: KSH Core Alignment Script
    # Runs the migrated k_ausd_v_ta_p_vertrag.py with parameters passed via XCom.
    k_ausd_v_ta_p_vertrag_task = BashOperator(
        task_id='k_ausd_v_ta_p_vertrag',
        bash_command='python3 {{ var.value.WORKSPACE_ROOT }}/SQL/aktuell/aufbereitung/bin/k_ausd_v_ta_p_vertrag.py -j AUSD_V_TA_P_VERTRAG -f "{{ task_instance.xcom_pull(task_ids=\'r_ausd_v_ta_p_vertrag\') }}"',
        env={
            'DWH_JOB_KENNUNG': DWH_JOB_KENNUNG,
            'WORKSPACE_ROOT': WORKSPACE_ROOT,
            'GCP_PROJECT': GCP_PROJECT,
            'GCS_BUCKET': GCS_BUCKET,
        },
    )

    # Step 4: SQL Transformation Script
    # This represents the Dataform compile run or BigQuery SQL execution for d_ausd_v_ta_p_vertrag.sql.
    # It is represented here as an EmptyOperator stub to be fully integrated with 
    # Dataform/BigQuery once the SQL pass is complete.
    d_ausd_v_ta_p_vertrag_task = EmptyOperator(
        task_id='d_ausd_v_ta_p_vertrag_dataform',
    )

    # Orchestration order based on legacy dependency graph:
    # 1. UC4 Job (this DAG) triggers Step 2 -> Step 3 -> Step 4
    r_ausd_v_ta_p_vertrag_task >> k_ausd_v_ta_p_vertrag_task >> d_ausd_v_ta_p_vertrag_task