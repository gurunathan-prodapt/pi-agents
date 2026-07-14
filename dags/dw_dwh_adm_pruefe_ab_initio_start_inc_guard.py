"""
DAG: dw_dwh_adm_pruefe_ab_initio_start_inc_guard
Description: Polls the Ab Initio state variable and holds downstream execution 
             until application readiness is met.
"""

from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from utils.ab_initio_utils import poll_ab_initio_status_fn

# Default arguments across the environment
DEFAULT_ARGS = {
    'owner': 'dwh_admin',
    'start_date': datetime(2023, 1, 1),
    'retries': 0,
    'catchup': False
}

with DAG( 
    dag_id='dw_dwh_adm_pruefe_ab_initio_start_inc_guard',
    default_args=DEFAULT_ARGS,
    schedule_interval=None,
    max_active_runs=1,
    is_paused_upon_creation=False,
    catchup=False,
    tags=['dwh', 'admin', 'ab_initio', 'gatekeeper']
) as dag:

    ab_initio_gatekeeper = PythonOperator(
        task_id='ab_initio_gatekeeper',
        python_callable=poll_ab_initio_status_fn,
        provide_context=True,
        execution_timeout=timedelta(hours=1)  # Safeguard against endless loop costs
    )

    ab_initio_gatekeeper