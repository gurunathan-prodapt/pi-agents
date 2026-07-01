"""
Airflow DAG for DW.BERT_DROP_TEMP_TABLE.

This DAG is a direct migration of the UC4 Unix job DW.BERT_DROP_TEMP_TABLE.
In order to avoid the high start-up cost and resource overhead of launching a Dataproc cluster
solely to run drops, this DAG uses BigQuery DDL statements via BigQueryInsertJobOperator.

Sync/Lock mechanisms are resolved using Airflow Pools (bert_write_lock_pool).
"""

from datetime import datetime, timedelta
from airflow import DAG
from airflow.providers.google.cloud.operators.bigquery import BigQueryInsertJobOperator

default_args = {
    "owner": "uc4_migration",
    "depends_on_past": False,
    "retries": 0,
    "retry_delay": timedelta(seconds=0),
    "start_date": datetime(2026, 4, 21),
}

with DAG(
    dag_id="dw_bert_drop_temp_table",
    schedule_interval=None,
    catchup=False,
    max_active_runs=1,
    is_paused_upon_creation=False,
    default_args=default_args,
    template_searchpath="/home/airflow/gcs",
    tags=["uc4_migration", "bert", "maintenance"],
) as dag:

    # BigQuery task executing the SQL cleanup routine
    run_bert_drop_temp_table = BigQueryInsertJobOperator(
        task_id="run_bert_drop_temp_table",
        configuration={
            "query": {
                "query": "sql/bert/r_drop_temp_table.sql",
                "useLegacySql": False,
            }
        },
        gcp_conn_id="google_cloud_default",
        pool="bert_write_lock_pool",
    )