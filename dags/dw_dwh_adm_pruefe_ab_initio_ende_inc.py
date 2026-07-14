"""
DAG: dw_dwh_adm_pruefe_ab_initio_ende_inc
Description: Audits and updates variables upon completion of the Ab Initio execution loop.
"""

from datetime import datetime
from airflow import DAG
from airflow.operators.python import PythonOperator
from utils.ab_initio_utils import update_ab_initio_status_fn

DEFAULT_ARGS = {
    'owner': 'dwh_admin',
    'start_date': datetime(2023, 6, 10),
    'retries': 0,
    'catchup': False
}

with DAG( 
    dag_id='dw_dwh_adm_pruefe_ab_initio_ende_inc',
    schedule_interval=None,
    max_active_runs=1,
    is_paused_upon_creation=False,
    default_args=DEFAULT_ARGS,
    catchup=False,
    description='Helper process to update Ab Initio execution state',
    tags=['dwh', 'admin', 'ab_initio', 'auditing']
) as dag:

    update_ab_initio_status = PythonOperator(
        task_id='update_ab_initio_status',
        python_callable=update_ab_initio_status_fn,
        provide_context=True
    )

    update_ab_initio_status