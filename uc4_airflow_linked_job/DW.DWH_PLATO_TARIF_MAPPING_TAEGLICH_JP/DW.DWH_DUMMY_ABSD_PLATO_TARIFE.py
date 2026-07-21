# ── Imports ──────────────────────────────────────────────
import logging
from datetime import datetime, timedelta
from airflow import DAG
from airflow.models import Variable
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator

# ── GCP Configuration (GLOBAL Env Variables) ──────────────
# Sourced dynamically at runtime via Airflow Variables.
# No hardcoded placeholders or fake fallback values.
GCP_PROJECT_ID = Variable.get("GCP_PROJECT")
GCP_REGION = Variable.get("GCP_REGION")
GCS_BUCKET_NAME = Variable.get("GCS_BUCKET")

# ── Default Args ─────────────────────────────────────────
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2026, 3, 30),
    'retries': 0,
    'retry_delay': timedelta(minutes=5),
}

# ── DAG Definition ───────────────────────────────────────
dag = DAG(
    dag_id='dw_dwh_plato_tarif_mapping_taeglich_jp',
    default_args=default_args,
    description='Converted Plato Tarif Mapping Daily DAG from UC4',
    schedule=None,  # Dynamic scheduling managed by parent/external triggers
    catchup=False,
    max_active_runs=1,
    is_paused_upon_creation=False,  # Matches legacy active state
)

# ── Executable Logic (Print Literal Compliance) ──────────
def execute_dummy_job():
    # MUST print original-language and original-spelling exactly as-is
    print("Doing nothinig")
    logging.info("Doing nothinig")

# ── Tasks ───────────────────────────────────────────────

start = EmptyOperator(
    task_id='start',
    dag=dag,
)

# Optimized dummy job utilizing a PythonOperator (preserves execution logic efficiently)
dw_dwh_dummy_absd_plato_tarife = PythonOperator(
    task_id='dw_dwh_dummy_absd_plato_tarife',
    python_callable=execute_dummy_job,
    dag=dag,
)

end = EmptyOperator(
    task_id='end',
    dag=dag,
)

# ── Dependencies ─────────────────────────────────────────
start >> dw_dwh_dummy_absd_plato_tarife >> end