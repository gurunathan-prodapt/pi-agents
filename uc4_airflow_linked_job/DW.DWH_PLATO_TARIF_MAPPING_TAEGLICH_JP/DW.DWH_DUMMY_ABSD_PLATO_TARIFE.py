from datetime import datetime, timedelta
import logging
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.models import Variable

# ─── GLOBAL CONFIGURATION (RUNTIME SOURCED) ──────────────────────────────────
# Sourced dynamically via Airflow Variables to prevent prose placeholders
GCP_PROJECT = Variable.get("GCP_PROJECT", default_var=None)
GCP_REGION = Variable.get("GCP_REGION", default_var=None)

# ─── DEFAULT ARGS ─────────────────────────────────────────────────────────────
DEFAULT_ARGS = {
    "owner": "airflow",
    "depends_on_past": False,
    "start_date": datetime(2026, 3, 30),
    "retries": 0,
    "retry_delay": timedelta(minutes=5),
}

# ─── PYTHON CALLABLE (LOGGING TASK) ──────────────────────────────────────────
def log_dummy_action():
    # OUTPUT/PRINT LITERAL RULE: Must match character-for-character, including legacy typo
    logging.info("Doing nothinig")

# ─── DAG DEFINITION ───────────────────────────────────────────────────────────
dag = DAG(
    dag_id="dw_dwh_dummy_absd_plato_tarife",
    default_args=DEFAULT_ARGS,
    schedule=None,  # Handled via parent workflow trigger (manual/external trigger placeholder)
    catchup=False,
    max_active_runs=1,
    is_paused_upon_creation=False,
    description="Transformed from UC4 JOBS_UNIX DW.DWH_DUMMY_ABSD_PLATO_TARIFE",
)

# ─── TASK ─────────────────────────────────────────────────────────────────────
execute_dummy = PythonOperator(
    task_id="dw_dwh_dummy_absd_plato_tarife",
    python_callable=log_dummy_action,
    dag=dag,
)

execute_dummy