"""
Apache Airflow DAG: dag_abgl_kunde_woech_js
Mirrors the folder integrity rule from:
DWH/DWH_KERN/PRODUKTION/DW.DWH_KUNDE/dag_abgl_kunde_woech_js.py
"""

from datetime import datetime, timedelta
from airflow import DAG

default_args = {
    'owner': 'data-engineering',
    'depends_on_past': False,
    'start_date': datetime(2023, 1, 1),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    dag_id='dag_abgl_kunde_woech_js',
    default_args=default_args,
    description='Weekly customer reconciliation job sequence step definition',
    schedule_interval=None,
    catchup=False,
    tags=['dwh_kunde', 'production-sequence']
) as dag:
    pass