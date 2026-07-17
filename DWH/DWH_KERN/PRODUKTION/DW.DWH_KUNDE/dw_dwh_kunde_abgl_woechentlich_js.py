"""
DAG: dw_dwh_kunde_abgl_woechentlich_js.py
Description: Airflow DAG orchestrating weekly customer address reconciliation.
"""

import sys
import os
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator
from airflow.models import Variable

# Append dags/bin to path dynamically to resolve wrapper imports seamlessly
sys.path.append(os.path.join(os.path.dirname(__file__), 'bin'))
from r_abgl_kunde_woech import run_reconciliation_workflow

# ---------------------------------------------------------
# ENVIRONMENT CONFIGURATION (Classified per Policy)
# ---------------------------------------------------------
# Global/Infrastructure Variables
GCP_PROJECT = Variable.get("GCP_PROJECT")
BQ_DATASET_DWH = Variable.get("BQ_DATASET_DWH", default_var="DWH_KERN")
BQ_DATASET_STAMM = Variable.get("BQ_DATASET_STAMM", default_var="STAMMDATEN")

# DAG Setup
default_args = {
    'owner': 'dw_team',
    'depends_on_past': False,
    'start_date': datetime(2026, 1, 1),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

def reconciliation_task_callable(**context):
    """
    Adapts the context execution date parameter (ds_nodash) and executes wrapper flow.
    """
    # Extract runtime target execution date (YYYYMMDD)
    stichtag = context['templates_dict']['stichtag']
    
    # Run the imported wrapper workflow execution
    run_reconciliation_workflow(
        project_id=GCP_PROJECT,
        dataset_dwh=BQ_DATASET_DWH,
        dataset_stamm=BQ_DATASET_STAMM,
        stichtag=stichtag
    )

with DAG(
    dag_id='dw_dwh_kunde_abgl_woechentlich_js',
    default_args=default_args,
    description='Reconciles customer address details weekly, reproducing exact legacy logs.',
    schedule_interval='0 2 * * 0',  # Weekly on Sundays at 02:00 AM
    catchup=False,
    max_active_runs=1,
    tags=['dwh', 'kunde', 'weekly'],
) as dag:

    start = EmptyOperator(task_id='start')

    execute_reconciliation = PythonOperator(
        task_id='execute_reconciliation',
        python_callable=reconciliation_task_callable,
        templates_dict={'stichtag': '{{ ds_nodash }}'},
        provide_context=True,
    )

    end = EmptyOperator(task_id='end')

    start >> execute_reconciliation >> end