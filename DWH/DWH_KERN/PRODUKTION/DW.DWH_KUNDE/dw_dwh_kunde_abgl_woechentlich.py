"""
Module: dw_dwh_kunde_abgl_woechentlich.py
Path: dags/dw_dwh_kunde/dw_dwh_kunde_abgl_woechentlich.py

Description:
    The central Airflow Orchestrator. Coordinates logging task pipelines,
    BigQuery insert actions, and operational compliance rules.
"""

from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.google.cloud.operators.bigquery import BigQueryInsertJobOperator
from airflow.utils.dates import days_ago

# Importing modules respecting the original folder integrity mapping
from dags.dw_dwh_kunde.bin.dw_dwh_kunde_abgl_woechentlich_bin import (
    log_start_message,
    log_end_message,
    log_discrepancy_count,
)
from dags.dw_dwh_kunde.sql.dw_dwh_kunde_abgl_woechentlich_sql import (
    get_insert_errors_query,
    get_count_query,
)

# Shared Default Task Parameters
DEFAULT_ARGS = {
    "owner": "data_analytics_team",
    "depends_on_past": False,
    "start_date": days_ago(7),
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
}

# Unified Dag Engine declaration
with DAG(
    "dw_kunde_abgleich_woechentlich",
    default_args=DEFAULT_ARGS,
    description="Weekly reconciliation of customer addresses between Core and Staging Layers",
    schedule_interval="0 6 * * 1",  # Every Monday at 06:00 UTC
    catchup=False,
    max_active_runs=1,
) as dag:

    # 1. Start execution logger task
    task_log_start = PythonOperator(
        task_id="log_start",
        python_callable=log_start_message,
    )

    # 2. Main data comparison execution job
    task_run_reconciliation = BigQueryInsertJobOperator(
        task_id="run_address_reconciliation_query",
        configuration={
            "query": {
                "query": get_insert_errors_query(logical_date_placeholder="{{ ds }}"),
                "useLegacySql": False,
            }
        },
    )

    # 3. Pull total discrepancy records metrics
    task_get_count = BigQueryInsertJobOperator(
        task_id="get_discrepancy_count_query",
        configuration={
            "query": {
                "query": get_count_query(logical_date_placeholder="{{ ds }}"),
                "useLegacySql": False,
            }
        },
    )

    # 4. Process and report discrepancy count
    task_log_count = PythonOperator(
        task_id="log_discrepancy_count",
        python_callable=log_discrepancy_count,
        op_kwargs={"task_id_for_count": "get_discrepancy_count_query"},
        provide_context=True,
    )

    # 5. Finalize processing logging message
    task_log_end = PythonOperator(
        task_id="log_end",
        python_callable=log_end_message,
    )

    # Execution workflow sequence
    task_log_start >> task_run_reconciliation >> task_get_count >> task_log_count >> task_log_end