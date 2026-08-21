"""
DAG: dw_dwh_abtn_smart_kubi
Description:
    Populates a temporary database table by executing a SQL script via a migrated
    Python wrapper. Before executing, it dynamically calculates a reporting month
    identifier (MONATSID) based on the current logical execution date (if before
    the 15th, targets the previous month; otherwise, the current month).
    Converted from UC4 job: DW.DWH_ABTN_SMART_KUBI
"""

import os
from datetime import datetime, timedelta
from airflow import DAG
from airflow.models import Variable
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator

# ─── GCP CONFIGURATION ────────────────────────────────────────────────────────
GCP_PROJECT = os.environ.get("GCP_PROJECT")
GCS_BUCKET = Variable.get("GCS_BUCKET")
GCP_CONN_ID = Variable.get("GCP_CONN_ID")
R_SQLSCRIPT_PATH = Variable.get("R_SQLSCRIPT_PATH")

# ─── DEFAULT ARGUMENTS ────────────────────────────────────────────────────────
default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

# ─── DATE CALCULATION HELPER FUNCTION ─────────────────────────────────────────
def calculate_monatsid_callable(**context):
    """
    Translates the original UC4 logic:
    :set &cdate = SYS_DATE("YYYYMMDD")
    :if &cday < '15'
      subtract 1 month
    :set &MONATSID = &cmonth
    
    This calculation uses the DAG's logical execution date (formerly execution_date)
    to maintain workflow idempotency.
    """
    logical_date = context["logical_date"]
    day = logical_date.day
    
    if day < 15:
        # Subtract one month by getting the 1st of current month and subtracting 1 day
        first_day_current_month = logical_date.replace(day=1)
        last_day_prev_month = first_day_current_month - timedelta(days=1)
        monatsid = last_day_prev_month.strftime("%Y%m")
    else:
        monatsid = logical_date.strftime("%Y%m")
        
    # Echo exact legacy German syntax output to execution logs
    print(f"Berichtsmonat:  {monatsid}")
    return monatsid


# ─── DAG DEFINITION ───────────────────────────────────────────────────────────
with DAG(
    dag_id="dw_dwh_abtn_smart_kubi",
    default_args=default_args,
    description="Populate temp table - Converted from UC4 job DW.DWH_ABTN_SMART_KUBI",
    schedule=None,  # No schedule defined in extraction, triggered externally
    start_date=datetime(2023, 1, 1),
    catchup=False,
    max_active_runs=1,  # Prevent concurrent execution on target temp tables
    is_paused_upon_creation=False,
) as dag:

    # Task 1: Dynamically calculate the reporting month ID (MONATSID)
    calculate_monatsid = PythonOperator(
        task_id="calculate_monatsid",
        python_callable=calculate_monatsid_callable,
    )

    # Task 2: Execute the wrapper script to run the converted SQL script with arguments
    dwh_abtn_smart_kubi = BashOperator(
        task_id="dwh_abtn_smart_kubi",
        bash_command=(
            "python3 {{ params.script_path }} "
            "-j ABTN_SMART_KUBI "
            "-f d_abtn_x_smart_kubi.sql "
            "-i {{ task_instance.xcom_pull(task_ids='calculate_monatsid') }}"
        ),
        params={
            "script_path": R_SQLSCRIPT_PATH,
        },
    )

    # ─── DEPENDENCIES ─────────────────────────────────────────────────────────
    calculate_monatsid >> dwh_abtn_smart_kubi