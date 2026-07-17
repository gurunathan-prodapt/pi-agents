# -*- coding: utf-8 -*-
from datetime import datetime, timedelta
import os
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.models import Variable

# Define relative execution script paths based on structured folders
DAG_DIR = os.path.dirname(os.path.abspath(__file__))
SCRIPT_PATH = os.path.join(DAG_DIR, "bin", "r_exp_rechnung_taeglich.py")

# Default SLA attributes
default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "start_date": datetime(2024, 1, 1),
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

dag = DAG(
    "dw_dwh_rechnung_export_taeglich_js",
    default_args=default_args,
    description="Orchestrator for daily billing data exports",
    schedule_interval="0 3 * * *",  # Standard run time execution daily at 03:00 UTC
    catchup=False,
    max_active_runs=1,
)

def log_uc4_start(**context):
    """
    Preserves and prints the original verbatim UC4 start execution log.
    """
    export_stichtag = context["templates_dict"]["stichtag"]
    # Verbatim UC4 print output translation
    print(f"Rechnungsexport fuer Stichtag {export_stichtag} angestossen")

start_log = PythonOperator(
    task_id="log_uc4_start",
    python_callable=log_uc4_start,
    templates_dict={"stichtag": "{{ ds_nodash }}"},
    provide_context=True,
    dag=dag,
)

execute_export = BashOperator(
    task_id="execute_export_script",
    bash_command=f"python3 {SCRIPT_PATH} -s {{{{ ds_nodash }}}} -k 'RECHNUNG_EXPORT_TAEGLICH'",
    env={
        "GCP_PROJECT": Variable.get("GCP_PROJECT"),
        "GCS_EXPORT_BUCKET": Variable.get("GCS_EXPORT_BUCKET"),
    },
    dag=dag,
)

start_log >> execute_export