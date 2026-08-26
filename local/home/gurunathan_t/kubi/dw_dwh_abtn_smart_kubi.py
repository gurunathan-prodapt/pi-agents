"""
DAG for DW.DWH_ABTN_SMART_KUBI
Populate temp table

This DAG orchestrates the execution of the SQL script 'd_abtn_x_smart_kubi.sql'
by dynamically calculating the reporting month (MONATSID) based on the execution date:
if the run occurs before the 15th of the month, it processes data for the previous month;
otherwise, it processes the current month.
"""

from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.models import Variable

# === GCP Configuration ===
GCS_BUCKET = Variable.get("GCS_BUCKET")
GCP_PROJECT = Variable.get("GCP_PROJECT")

# === Default Arguments ===
default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

# === DAG Definition ===
with DAG(
    dag_id="dw_dwh_abtn_smart_kubi",
    default_args=default_args,
    description="Populate temp table - Converted from UC4 DW.DWH_ABTN_SMART_KUBI",
    schedule=None,  # Externally triggered
    start_date=datetime(2023, 1, 1),
    catchup=False,
    max_active_runs=1,  # Safeguard to prevent parallel database writes
    is_paused_upon_creation=False,
) as dag:

    # Task: d_abtn_x_smart_kubi
    # Converted SQL-script task executing the BigQuery SQL script with dynamic MONATSID calculation.
    # Jinja calculation logic: If day of logical_date < 15, subtract 15 days to yield previous month (YYYYMM),
    # else subtract 15 days which keeps it in the current month (YYYYMM).
    monatsid_expression = "{{ (logical_date - macros.timedelta(days=15)).strftime('%Y%m') }}"

    # The bash command echoes the reporting month exactly as written in the source script:
    # :print Berichtsmonat:  &MONATSID
    # and then executes the BigQuery SQL script stored in GCS.
    bash_command = (
        f"echo \"Berichtsmonat:  {monatsid_expression}\" && "
        f"bq query --use_legacy_sql=false "
        f"--project_id={GCP_PROJECT} "
        f"--parameter=MONATSID:STRING:{monatsid_expression} "
        f"\"$(gsutil cat gs://{GCS_BUCKET}/sql/d_abtn_x_smart_kubi.sql)\""
    )

    d_abtn_x_smart_kubi = BashOperator(
        task_id="d_abtn_x_smart_kubi",
        bash_command=bash_command,
        env={
            "DWH_JOB_KENNUNG": "ABTN_SMART_KUBI",
            "LOGIN": "DW.UNIX.ISTNS",
            "HOST": "DWHDWH1P",
        },
    )

    # Dependency routing (Single-task execution flow)
    d_abtn_x_smart_kubi