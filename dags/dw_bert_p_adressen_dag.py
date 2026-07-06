from __future__ import annotations

import os
from datetime import datetime
from airflow import DAG
from airflow.providers.google.cloud.operators.bigquery import BigQueryInsertJobOperator

# Configure Environment Variables
PROJECT_ID = os.getenv("GCP_PROJECT_ID", "gcp_project_id")
SQL_FILE_PATH = os.path.join(os.getenv("DAGS_FOLDER", "/home/airflow/gcs/dags"), "sql/d_ausd_adressen.sql")

default_args = {
    "owner": "data-engineering",
    "depends_on_past": False,
    "retries": 1,
}

with DAG(
    dag_id="dw_bert_p_adressen_dag",
    default_args=default_args,
    schedule_interval=None,
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=["migration", "bigquery", "dw", "bert"],
) as dag:

    run_dw_bert_p_adressen_sql = BigQueryInsertJobOperator(
        task_id="run_dw_bert_p_adressen_sql",
        configuration={
            "query": {
                "query": """SELECT 1""",  # Airflow will read and load the SQL file template dynamically
                "useLegacySql": False,
            }
        },
        gcp_conn_id="google_cloud_default",
    )

    # Resolve file reference dynamically at execution time
    with open(SQL_FILE_PATH, "r") as f:
        run_dw_bert_p_adressen_sql.configuration["query"]["query"] = f.read()