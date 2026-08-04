"""Airflow DAG for the weekly customer master-data address reconciliation job.

This DAG represents the UC4 Unix job DW.DWH_KUNDE_ABGL_WOECHENTLICH_JS.
It calculates the logical run date parameter, logs progress using verbatim legacy output,
and triggers the downstream customer address reconciliation task.
"""

import logging
from datetime import datetime, timedelta

from airflow import DAG
from airflow.models import Variable
from airflow.operators.bash import BashOperator
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator

# GLOBAL (environment-wide) - Sourced from Airflow Variables
GCP_PROJECT = Variable.get("GCP_PROJECT")
GCS_BUCKET = Variable.get("GCS_BUCKET")

# JOB-SPECIFIC - Sourced from Airflow Variables / local constants
JOB_CONFIG = {
    "DWH_JOB_KENNUNG": "KUNDE_ABGL_WOECHENTLICH",
}
SCRIPT_PATH = Variable.get("dw_dwh_kunde_abgl_woechentlich_js_script_path")

default_args = {
    "owner": "DW",
    "depends_on_past": False, 
    "retries": 0,
    "retry_delay": timedelta(minutes=5),
    "start_date": datetime(2024, 1, 1),
}

with DAG(
    dag_id="dw_dwh_kunde_abgl_woechentlich_js",
    default_args=default_args,
    schedule=None,
    catchup=False,
    max_active_runs=1,
    is_paused_upon_creation=False,
    tags=["DW"],
    doc_md=__doc__,
) as dag:

    # REVIEW-STRUCT: launcher command ":SET &DWH_JOB_KENNUNG = 'KUNDE_ABGL_WOECHENTLICH'" not recognised — confirm target operator/script manually
    dwh_kunde_abgl_woechentlich_js = EmptyOperator(
        task_id="dwh_kunde_abgl_woechentlich_js",
    )

    def log_start_message(**context):
        lauf_woche = context["ds_nodash"]
        # OUTPUT/PRINT LITERAL RULE: Verbally preserving the original German text from the source print statement
        logging.info(f"Kundenadressabgleich fuer Lauf {lauf_woche} angestossen")

    log_start = PythonOperator(
        task_id="log_start",
        python_callable=log_start_message,
    )

    execute_script = BashOperator(
        task_id="execute_r_abgl_kunde_woech",
        bash_command=f"python {SCRIPT_PATH} -s {{{{ ds_nodash }}}}",
        env={
            "DWH_JOB_KENNUNG": JOB_CONFIG["DWH_JOB_KENNUNG"],
            "GCP_PROJECT": GCP_PROJECT,
            "GCS_BUCKET": GCS_BUCKET,
        }
    )

    dwh_kunde_abgl_woechentlich_js >> log_start >> execute_script