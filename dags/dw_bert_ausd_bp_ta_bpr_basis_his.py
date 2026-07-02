from datetime import datetime, timedelta
import os
from airflow import DAG
from airflow.providers.google.cloud.operators.bigquery import BigQueryExecuteQueryOperator

# Retrieve environment-specific variables
PROJECT_ID = os.getenv("GCP_PROJECT", "gcp-dwh-prod")
CONN_ID = "google_cloud_default"

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "start_date": datetime(2026, 4, 21),
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    dag_id="dw_bert_ausd_bp_ta_bpr_basis_his",
    default_args=default_args,
    description="Orchestrator for BERT Base Products Historical Processing",
    schedule_interval="0 4 * * *",  # Run daily at 04:00 AM
    catchup=False,
    max_active_runs=1,
    tags=["bigquery", "bert", "dwh"],
    template_searchpath=[os.environ.get("DAGS_FOLDER", "/home/airflow/gcs/dags")],
) as dag:

    execute_conversion_query = BigQueryExecuteQueryOperator(
        task_id="execute_ausd_bp_ta_bpr_basis_his",
        sql="sql/d_ausd_bp_ta_bpr_basis_his.sql",  # Points to SQL inside templated paths
        use_legacy_sql=False,
        gcp_conn_id=CONN_ID,
        write_disposition="WRITE_TRUNCATE",
        create_disposition="CREATE_IF_NEEDED",
    )

    execute_conversion_query