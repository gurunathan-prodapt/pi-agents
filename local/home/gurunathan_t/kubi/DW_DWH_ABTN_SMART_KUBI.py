"""
DAG: dw_dwh_abtn_smart_kubi
Description: Populate temporary table (ABTN_SMART_KUBI) by executing a SQL script.
             Migrated from UC4 standalone UNIX job DW.DWH_ABTN_SMART_KUBI.
             Calculates reporting month parameter (MONATSID) dynamically.
"""

from datetime import datetime, timedelta
from airflow import DAG
from airflow.models import Variable
from airflow.operators.empty import EmptyOperator
from airflow.decorators import task

# ─── GCP CONFIGURATION ────────────────────────────────────────────────────────
GCP_PROJECT = Variable.get("GCP_PROJECT")
GCP_REGION = Variable.get("GCP_REGION")
DATAPROC_REGION = Variable.get("DATAPROC_REGION")
DATAPROC_CLUSTER = Variable.get("DATAPROC_CLUSTER")
GCS_BUCKET = Variable.get("GCS_BUCKET")

# ─── HELPER FUNCTIONS ─────────────────────────────────────────────────────────
def calculate_monatsid(logical_date: datetime) -> str:
    """
    Replicates legacy UC4 date calculation logic:
    If execution day is < 15, use YYYYMM of the previous month.
    Else, use YYYYMM of the current month.
    """
    day = logical_date.day
    if day < 15:
        first_day_of_current = logical_date.replace(day=1)
        previous_month_date = first_day_of_current - timedelta(days=1)
        return previous_month_date.strftime("%Y%m")
    else:
        return logical_date.strftime("%Y%m")

# ─── DEFAULT ARGS ─────────────────────────────────────────────────────────────
default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "retries": 1,
    "retry_delay": timedelta(seconds=300),
}

# ─── DAG DEFINITION ───────────────────────────────────────────────────────────
with DAG(
    dag_id="dw_dwh_abtn_smart_kubi",
    default_args=default_args,
    description="Populate temp table - Migrated from UC4 DW.DWH_ABTN_SMART_KUBI",
    schedule=None,  # Externally triggered
    start_date=datetime(2023, 1, 1),
    catchup=False,
    max_active_runs=1,
    is_paused_upon_creation=False,
    tags=["migrated_uc4", "jobs_unix"],
) as dag:

    # ─── PARAMETER RESOLUTION TASK ────────────────────────────────────────────
    # Calculates dynamic variables and outputs the required print statement
    @task(task_id="calculate_parameters")
    def resolve_parameters(**context):
        logical_date = context["logical_date"]
        monatsid = calculate_monatsid(logical_date)
        
        # Output/Print Literal Rule: Must output exact German text
        print(f"Berichtsmonat:  {monatsid}")
        
        return {
            "monatsid": monatsid,
            "job_kennung": "ABTN_SMART_KUBI"
        }

    params = resolve_parameters()

    # ─── SQL SCRIPT LAUNCHER TASK ─────────────────────────────────────────────
    # Launcher wraps SQL script [d_abtn_x_smart_kubi.sql] converted by a separate pipeline --
    # confirm whether it produced a Python script or BigQuery SQL, then replace this stub with a
    # BashOperator/PythonOperator (Python) or BigQueryInsertJobOperator (BigQuery SQL) accordingly
    populate_temp_table = EmptyOperator(
        task_id="populate_temp_table",
    )

    # ─── DEPENDENCIES ─────────────────────────────────────────────────────────
    params >> populate_temp_table