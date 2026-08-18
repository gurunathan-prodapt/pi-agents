from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.models import Variable

# ── GLOBAL CONFIGURATION ─────────────────────────────────────────────────────
GCP_PROJECT = Variable.get("GCP_PROJECT")
GCP_REGION = Variable.get("GCP_REGION")

# ── DEFAULT ARGS ─────────────────────────────────────────────────────────────
default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

# ── PARAMETER LOGIC ──────────────────────────────────────────────────────────
def get_monatsid(execution_date_str):
    """
    Calculates the reporting month parameter (MONATSID) dynamically.
    If the day of execution is before the 15th, it targets the previous month;
    otherwise, it targets the current month.
    """
    dt = datetime.strptime(execution_date_str, "%Y%m%d")
    if dt.day < 15:
        first_day_current = dt.replace(day=1)
        prev_month = first_day_current - timedelta(days=1)
        return prev_month.strftime("%Y%m")
    else:
        return dt.strftime("%Y%m")

def run_sql_script(**context):
    # Get execution date in YYYYMMDD format from Airflow context
    execution_date_str = context['ds_nodash']
    
    # Calculate MONATSID
    monats_id = get_monatsid(execution_date_str)
    
    # Output/Print Logging: The calculated reporting month must be logged preserving the exact original German literal from the source:
    print(f"Berichtsmonat:  {monats_id}")
    
    # Import the migrated r_sqlscript module
    import r_sqlscript
    
    # Execute the SQL script
    r_sqlscript.execute_sql_script(
        job='ABTN_SMART_KUBI',
        sql_file='d_abtn_x_smart_kubi.sql',
        monats_id=monats_id
    )

# ── DAG DEFINITION ───────────────────────────────────────────────────────────
with DAG(
    dag_id="dw_dwh_abtn_smart_kubi",
    default_args=default_args,
    description="Populate temp table - Standalone migrated UC4 JOBS_UNIX",
    schedule=None,  # Externally triggered / on-demand
    start_date=datetime(2023, 1, 1),
    catchup=False,
    max_active_runs=1,
    tags=["migrated_uc4", "jobs_unix", "sql_script"],
) as dag:

    dw_dwh_abtn_smart_kubi_task = PythonOperator(
        task_id="dw_dwh_abtn_smart_kubi_task",
        python_callable=run_sql_script,
        provide_context=True,
    )

    dw_dwh_abtn_smart_kubi_task