"""Airflow DAG for the daily invoice/rechnung export workflow.

This DAG represents the UC4 job plan DW.DWH_RECHNUNG_EXPORT_TAEGLICH_JP.
It launches the daily export job task that prepares the business date and
invokes the migrated export script logic for the reporting directory flow.
"""

from datetime import datetime, timedelta
from airflow import DAG
from airflow.models import Variable
from airflow.operators.bash import BashOperator

# Environment-specific configuration (sourced at runtime)
# Sourced globally to identify target infrastructure and deployment details
GCP_PROJECT = Variable.get("GCP_PROJECT")
GCS_BUCKET = Variable.get("GCS_BUCKET")
AIRFLOW_CONN_DW_UNIX_ISTNS = Variable.get("AIRFLOW_CONN_DW_UNIX_ISTNS")

# Job-specific configurations
JOB_CONFIG = {
    "dwh_job_kennung": "RECHNUNG_EXPORT_TAEGLICH",
    "script_path": "/opt/airflow/dags/dw_source/isdwh/exporter/rechnung/bin/r_exp_rechnung_taeglich.py",
}

default_args = {
    "owner": "uc4_migration",
    "retries": 0,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    dag_id="dw_dwh_rechnung_export_taeglich_jp",
    schedule=None,
    start_date=datetime(2024, 1, 1),
    catchup=False,
    max_active_runs=1,
    is_paused_upon_creation=False,
    default_args=default_args,
    description="Taeglicher Export der Rechnungsdaten (RECHNUNG) aus der DWH-Kernschicht in das Reporting-Verzeichnis",
    tags=["uc4", "dwh", "rechnung", "export"],
) as dag:

    # Task representing the migrated UC4 JOBS_UNIX task DW.DWH_RECHNUNG_EXPORT_TAEGLICH_JS
    # It executes the python script replacing r_exp_rechnung_taeglich.ksh,
    # passes the stichtag parsed dynamically via the ds_nodash macro,
    # and logs the status using the exact German output from the original UC4 logic.
    dw_dwh_rechnung_export_taeglich_js = BashOperator(
        task_id="dw_dwh_rechnung_export_taeglich_js",
        env={
            "DWH_JOB_KENNUNG": JOB_CONFIG["dwh_job_kennung"],
            "GCP_PROJECT": GCP_PROJECT,
            "GCS_BUCKET": GCS_BUCKET,
            "AIRFLOW_CONN": AIRFLOW_CONN_DW_UNIX_ISTNS,
        },
        bash_command=(
            "python3 {{ params.script_path }} -s {{ ds_nodash }} && "
            "echo \"Rechnungsexport fuer Stichtag {{ ds_nodash }} angestossen\""
        ),
        params={
            "script_path": JOB_CONFIG["script_path"],
        },
    )

# Establish workflow execution flow
dw_dwh_rechnung_export_taeglich_js