#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Module: dw_dwh_kunde_abgl_woechentlich.py
Path: dags/dw_dwh_kunde_abgl_woechentlich.py

Orchestration pipeline DAG matching the legacy Automic/UC4 Job Plan definitions
for the weekly customer address reconciliation process.
"""

import os
import sys
import datetime
from datetime import timedelta
from typing import Dict, Any

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.models import Variable

# Dynamically resolve root workspace path to import bin modules cleanly
ROOT_PATH = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
if ROOT_PATH not in sys.path:
    sys.path.append(ROOT_PATH)

# Import process-specific validation function from repository module
from bin.r_abgl_kunde_woech import run_reconciliation

# Default execution rules for Tasks
DEFAULT_ARGS = {
    'owner': 'dwh_kern',
    'depends_on_past': False,
    'start_date': datetime.datetime(2023, 1, 1),
    'email_on_failure': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}


def execute_reconciliation_wrapper(**context: Dict[str, Any]) -> None: 
    """
    Extracts runtime execution variables and executes the core business reconciliation function.
    """
    # 1. Environment-Specific Global Variable Resolution
    gcp_project = Variable.get("GCP_PROJECT")
    bq_dataset = Variable.get("BQ_DATASET_DWH_KERN", default_var="dwh_kern")
    
    # Try resolving custom path for the conversion SQL file
    sql_path = Variable.get("KUNDE_ABGL_SQL_PATH", default_var=None)
    if not sql_path:
        # Defaults to local structure if path variable is not registered
        sql_path = os.path.join(ROOT_PATH, "gcs", "sql", "d_abgl_kunde_woech.sql")

    # 2. Run Context & Dynamic Fallback logic
    dag_run = context.get('dag_run')
    dag_run_conf = dag_run.conf if dag_run else {}
    
    # Resolve 'stichtag' parameter
    l_stichtag = dag_run_conf.get('stichtag')
    if not l_stichtag:
        # Default target date calculation to exact legacy pattern: 7 days prior (YYYYMMDD)
        execution_date = context['execution_date']
        seven_days_ago = execution_date - timedelta(days=7)
        l_stichtag = seven_days_ago.strftime('%Y%m%d')
        
    run_id = context['run_id']
    lauf_woche = context['ds']
    
    # 3. Call Process Validation
    run_reconciliation(
        gcp_project=gcp_project,
        bq_dataset=bq_dataset,
        l_stichtag=l_stichtag,
        run_id=run_id,
        lauf_woche=lauf_woche,
        sql_path=sql_path
    )


# DAG definition
with DAG(
    dag_id='dw_dwh_kunde_abgl_woechentlich',
    default_args=DEFAULT_ARGS,
    description='Weekly customer address reconciliation migrated from Automic/UC4 JP.',
    schedule_interval='0 6 * * 1',  # Weekly on Monday mornings at 06:00
    catchup=False,
    max_active_runs=1,
    tags=['dwh_kern', 'migration', 'weekly']
) as dag:

    execute_abgleich = PythonOperator(
        task_id='execute_reconciliation',
        python_callable=execute_reconciliation_wrapper,
        provide_context=True,
    )

    execute_abgleich