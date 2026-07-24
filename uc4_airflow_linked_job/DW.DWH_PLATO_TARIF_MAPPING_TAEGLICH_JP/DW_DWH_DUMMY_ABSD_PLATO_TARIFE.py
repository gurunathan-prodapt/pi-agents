import os
from datetime import datetime, timedelta
from airflow import DAG
from airflow.models import Variable
from airflow.operators.empty import EmptyOperator
from airflow.operators.bash import BashOperator

# ----------------------------------------------------------------------
# Environment-Specific Values (Classified as GLOBAL)
# ----------------------------------------------------------------------
GCP_PROJECT_ID = Variable.get("GCP_PROJECT")
DATAPROC_REGION = Variable.get("GCP_REGION")
GCS_BUCKET_NAME = Variable.get("GCS_BUCKET")

# ----------------------------------------------------------------------
# Default DAG Arguments
# ----------------------------------------------------------------------
default_args = { 
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2026, 1, 1),
    'retries': 0,
    'retry_delay': timedelta(minutes=5),
}

# ----------------------------------------------------------------------
# DAG Definition
# ----------------------------------------------------------------------
dag = DAG(
    dag_id='dw_dwh_dummy_absd_plato_tarife',
    default_args=default_args,
    description='Converted from UC4 DW.DWH_DUMMY_ABSD_PLATO_TARIFE (Dummy Job)',
    schedule_interval=None,  # Triggered by parent pipeline scheduling
    catchup=False,
    max_active_runs=1,
    is_paused_upon_creation=False,
)

# ----------------------------------------------------------------------
# Execution Steps
# ----------------------------------------------------------------------
start_task = EmptyOperator(
    task_id='start',
    dag=dag,
)

# Converted from legacy script action: :print Doing nothinig
# The exact character literal typo 'Doing nothinig' must be retained verbatim.
dw_dwh_dummy_absd_plato_tarife_task = BashOperator(
    task_id='dw_dwh_dummy_absd_plato_tarife',
    bash_command="echo 'Doing nothinig'",
    dag=dag,
)

end_task = EmptyOperator(
    task_id='end',
    dag=dag,
)

# ----------------------------------------------------------------------
# Task Flow Map
# ----------------------------------------------------------------------
start_task >> dw_dwh_dummy_absd_plato_tarife_task >> end_task