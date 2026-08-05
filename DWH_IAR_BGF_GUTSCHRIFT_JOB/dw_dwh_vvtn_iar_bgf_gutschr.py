"""
DAG Name: dw_dwh_vvtn_iar_bgf_gutschr
Description: Migrated DAG for the UC4 job DW.DWH_VVTN_IAR_BGF_GUTSCHR.
This process transforms Gutschrift (credit note) files into a single unified CSV format.
"""

from datetime import datetime, timedelta
from airflow import DAG
from airflow.models import Variable
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator

# === GLOBAL ENVIRONMENT CONFIGURATION ===
GCP_PROJECT_ID = Variable.get("GCP_PROJECT")
DATAPROC_REGION = Variable.get("DATAPROC_REGION")
DATAPROC_CLUSTER_NAME = Variable.get("DATAPROC_CLUSTER")
GCS_BUCKET = Variable.get("GCS_BUCKET")

# === DEFAULT ARGS ===
DEFAULT_ARGS = {
    'owner': 'airflow',
    'depends_on_past': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

def print_lastmonth_func(month_id, **context):
    # Maintain strict character-for-character compliance with original-language wording
    # Source print literal: "Lastmonth is &Month_ID"
    print(f"Lastmonth is {month_id}")

with DAG( 
    dag_id='dw_dwh_vvtn_iar_bgf_gutschr',
    default_args=DEFAULT_ARGS,
    description='Transform Gutschrift files to one file CSV',
    schedule=None,
    start_date=datetime(2023, 1, 1),
    catchup=False,
    max_active_runs=1,
    is_paused_upon_creation=False,
    tags=['migrated_uc4', 'jobs_unix'],
    params={
        'DWH_JOB_KENNUNG': 'VVTN_IAR_BGF_GUTSCHR',
    }
) as dag:

    # ── Task: print_lastmonth_task ──
    # Ported from legacy print log statement: :print Lastmonth is &Month_ID
    # Dynamic parameter resolution utilizing native Airflow context macros
    print_lastmonth_task = PythonOperator(
        task_id='print_lastmonth_task',
        python_callable=print_lastmonth_func,
        op_kwargs={
            'month_id': "{{ (data_interval_start.add(months=-1)).strftime('%Y%m') }}"
        },
    )

    # ── Task: dw_dwh_vvtn_iar_bgf_gutschr_task ──
    # This task maps to the legacy Unix job DW.DWH_VVTN_IAR_BGF_GUTSCHR.
    # The original script executed: $HOME/aktuell/vorverarbeitung/tn/bin/r_vvtn_iar_bgf_gutschrift on host |DWHDWH1P|HOST.
    # REVIEW-STRUCT: launcher command not recognised during conversion — implement the correct operator once its behaviour is confirmed.
    # The workspace file reads/writes are redirected to the Google Cloud Storage bucket (GCS_BUCKET).
    dw_dwh_vvtn_iar_bgf_gutschr_task = EmptyOperator(
        task_id='dw_dwh_vvtn_iar_bgf_gutschr_task',
    )

    # ── Dependencies ──
    print_lastmonth_task >> dw_dwh_vvtn_iar_bgf_gutschr_task