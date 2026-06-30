"""
Airflow DAG for ausd_bp_ta_bpr_instance.
Schedules and coordinates the migration of DW.BERT_AUSD_BP_TA_BPR_INSTANCE.
"""

from datetime import datetime, timedelta
from airflow import DAG
from airflow.providers.google.cloud.operators.bigquery import BigQueryExecuteQueryOperator

# Default arguments for Composer
default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "start_date": datetime(2026, 1, 1),
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

# Define DAG
with DAG(
    dag_id="ausd_bp_ta_bpr_instance",
    default_args=default_args,
    description="Orchestrates the truncation and reload of BPR instances from pds and cds source tables.",
    schedule_interval=None,  # Scheduled externally or triggered within parent UC4 chain
    catchup=False,
    max_active_runs=1,
    template_searchpath=["/home/airflow/gcs/sql", "/home/airflow/gcs/dags/sql"],
    tags=["bigquery", "bert", "basisprodukt"],
) as dag:

    # Task to execute the converted BigQuery script
    process_bpr_instance = BigQueryExecuteQueryOperator(
        task_id="process_bpr_instance",
        sql="d_ausd_bp_ta_bpr_instance.sql",
        use_legacy_sql=False,
        gcp_conn_id="google_cloud_default",
        location="EU",
    )

    process_bpr_instance