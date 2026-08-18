"""
DAG: dw_dwh_abtn_smart_kubi

Overview:
This workflow encapsulates a single UC4 Unix job (DW.DWH_ABTN_SMART_KUBI) 
that executes a SQL script to populate a data warehouse temporary table. 
Prior to running the database query, the job executes logic to determine a 
reporting month parameter (&MONATSID). If the current day of the month is 
before the 15th, it sets the reporting month to the previous month; otherwise, 
it uses the current month. The execution uses a custom SQL wrapper utility 
(r_sqlscript). Since this extraction contains only the UNIX job without a 
surrounding workflow (JOBP) or schedule, it is classified as an externally 
triggered job.
"""

from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.models import Variable

# === GCP CONFIGURATION ===
GCP_PROJECT_ID = Variable.get("GCP_PROJECT")
DATAPROC_REGION = Variable.get("DATAPROC_REGION")
DATAPROC_CLUSTER_NAME = Variable.get("DATAPROC_CLUSTER")
GCS_BUCKET = Variable.get("GCS_BUCKET")

# === DEFAULT ARGS ===
DEFAULT_ARGS = {
    "owner": "airflow",
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

def run_abtn_smart_kubi(**context):
    logical_date = context["logical_date"]
    if logical_date.day < 15:
        first_of_this_month = logical_date.replace(day=1)
        last_day_of_prev_month = first_of_this_month - timedelta(days=1)
        target_date = last_day_of_prev_month
    else:
        target_date = logical_date
    monatsid = target_date.strftime("%Y%m")
    
    # Print statement matching the legacy German output exactly
    print(f"Berichtsmonat:  {monatsid}")
    
    return monatsid

with DAG(
    dag_id="dw_dwh_abtn_smart_kubi",
    default_args=DEFAULT_ARGS,
    description="Populate temp table - migrated from DW.DWH_ABTN_SMART_KUBI",
    schedule=None,
    start_date=datetime(2023, 1, 1),
    catchup=False,
    max_active_runs=1,
    is_paused_upon_creation=False,
    tags=["migrated_uc4", "dwh"],
) as dag:

    # REVIEW-STRUCT: launcher wraps SQL script [d_abtn_x_smart_kubi.sql] converted by a separate pipeline --
    # confirm whether it produced a Python script or BigQuery SQL, then replace this task with a
    # BashOperator/PythonOperator (Python) or BigQueryInsertJobOperator (BigQuery SQL) accordingly.
    # The calculated MONATSID is returned by this task and can be pulled via XCom in downstream tasks.
    dw_dwh_abtn_smart_kubi_task = PythonOperator(
        task_id="dw_dwh_abtn_smart_kubi_task",
        python_callable=run_abtn_smart_kubi,
    )

    dw_dwh_abtn_smart_kubi_task