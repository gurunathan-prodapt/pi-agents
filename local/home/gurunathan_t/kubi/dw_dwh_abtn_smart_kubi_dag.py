"""
### dw_dwh_abtn_smart_kubi
The `dw_dwh_abtn_smart_kubi` workflow is a standalone data preparation job designed 
to populate a temporary database table. It executes a specific SQL script (`d_abtn_x_smart_kubi.sql`) 
after dynamically calculating a reporting month ID (`MONATSID`) based on the execution date 
(if the execution day is before the 15th, it targets the previous month; otherwise, it targets 
the current month). Since no calendar schedule or parent job plan (JOBP) is provided in this 
extraction, this DAG is designed to be triggered externally.
"""

from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator
from airflow.models import Variable

# ─── GLOBAL CONFIGURATION ───────────────────────────────────────────────────
GCP_PROJECT = Variable.get("GCP_PROJECT", default_var=None)
GCP_REGION = Variable.get("GCP_REGION", default_var=None)
BQ_DATASET = Variable.get("BQ_DATASET", default_var=None)

# ─── JOB-SPECIFIC CONFIGURATION ─────────────────────────────────────────────
DWH_JOB_KENNUNG = "ABTN_SMART_KUBI"
SQL_FILE_PATH = "local/home/gurunathan_t/kubi/d_abtn_x_smart_kubi.sql"

# ─── DEFAULT ARGS ─────────────────────────────────────────────────────────────
default_args = {
    'owner': 'dw',
    'depends_on_past': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# ─── PYTHON LOGIC ─────────────────────────────────────────────────────────────
def calculate_and_log_monatsid(**context):
    """
    Calculates and logs MONATSID based on the DAG run's logical date.
    Replicates the original UC4 logic:
    If day < 15, use the previous month (YYYYMM), else use the current month (YYYYMM).
    """
    logical_date = context['logical_date']
    if logical_date.day < 15:
        # Go to first day of current month, then subtract 1 day to get previous month
        first_of_month = logical_date.replace(day=1)
        prev_month = first_of_month - timedelta(days=1)
        monats_id = prev_month.strftime('%Y%m')
    else:
        monats_id = logical_date.strftime('%Y%m')
    
    # Print statement must exactly match legacy log output, including the spacing
    print(f"Berichtsmonat:  {monats_id}")
    
    # Push to XCom so downstream operators can consume it
    context['ti'].xcom_push(key='MONATSID', value=monats_id)
    return monats_id

# ─── DAG DEFINITION ───────────────────────────────────────────────────────────
with DAG(
    dag_id='dw_dwh_abtn_smart_kubi',
    default_args=default_args,
    description='Populate temp table - migrated from DW.DWH_ABTN_SMART_KUBI',
    schedule=None,
    start_date=datetime(2023, 1, 1),
    catchup=False,
    max_active_runs=1,
    is_paused_upon_creation=False,
    tags=['dw', 'uc4_migration'],
) as dag:

    # Task to calculate and log the dynamic reporting month (MONATSID)
    log_monatsid = PythonOperator(
        task_id='log_monatsid',
        python_callable=calculate_and_log_monatsid,
    )

    # REVIEW-STRUCT: launcher wraps SQL script [d_abtn_x_smart_kubi.sql] converted by a separate pipeline --
    # confirm whether it produced a Python script or BigQuery SQL, then replace this stub with a
    # BashOperator/PythonOperator (Python) or BigQueryInsertJobOperator (BigQuery SQL) accordingly
    stub_dwh_abtn_smart_kubi = EmptyOperator(
        task_id="dwh_abtn_smart_kubi",
        doc_md=f"""
        ### Migration Note
        Runs raw SQL: `{SQL_FILE_PATH}`
        Parameters required: `MONATSID` (calculated via `calculate_and_log_monatsid()`)
        Job Identifier: `{DWH_JOB_KENNUNG}`
        """,
    )

    # ─── DEPENDENCIES ─────────────────────────────────────────────────────────
    log_monatsid >> stub_dwh_abtn_smart_kubi