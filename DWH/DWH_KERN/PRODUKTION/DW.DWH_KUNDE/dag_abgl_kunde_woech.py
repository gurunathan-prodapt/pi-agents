"""
Airflow DAG mapping the UC4 / Job stream scheduling parameters for:
DW.DWH_KUNDE_ABGL_WOECHENTLICH_JP
"""

from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.models import Variable

# Import decoupled task execution helper maintaining clean target structural separation
from dw_dwh_kunde.bin.r_abgl_kunde_woech_task import execute_and_log_reconciliation

# Environmental configurations sourced dynamically with production-safe fallbacks
GCP_PROJECT = Variable.get("GCP_PROJECT")
BQ_LOCATION = Variable.get("GCP_LOCATION", default_var="EU")

# Maintain production attributes
default_args = {
    "owner": "dw_produktion",
    "depends_on_past": False,
    "start_date": datetime(2020, 3, 9),
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    "dw_dwh_kunde_abgl_woechentlich",
    default_args=default_args,
    description="Weekly customer master address reconciliation (DWH_KUNDE)",
    schedule_interval="0 6 * * 1",  # Every Monday at 06:00 AM UTC
    catchup=False,
    max_active_runs=1,
) as dag:

    run_reconciliation = PythonOperator(
        task_id="run_reconciliation",
        python_callable=execute_and_log_reconciliation,
        op_kwargs={
            "gcp_project": GCP_PROJECT,
            "bq_location": BQ_LOCATION
        },
        provide_context=True,
    )

    run_reconciliation