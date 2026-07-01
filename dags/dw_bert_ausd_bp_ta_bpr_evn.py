import os
from datetime import timedelta

from airflow import DAG
from airflow.models import Variable
from airflow.providers.google.cloud.operators.bigquery import BigQueryExecuteQueryOperator
from airflow.utils.dates import days_ago

# Dynamic retrieval of environment configuration values using Airflow variables with fallback values.
# Standard Airflow best practice uses Jinja template strings inside operators to avoid DB queries on DAG parsing,
# but we also define them here for programmatic configurations.
GCP_PROJECT = Variable.get("gcp_project", default_var="gcp-project-placeholder")
GCP_DATASET = Variable.get("gcp_dataset", default_var="isbert_schema")

# Configure DAG directory path to dynamically locate templates for relative paths.
DAG_FOLDER = os.path.dirname(os.path.realpath(__file__))

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "start_date": days_ago(1),
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    dag_id="dw_bert_ausd_bp_ta_bpr_evn",
    default_args=default_args,
    description="BERT_P_BASISPRODUKT: Aufbereitung der instantiierten Basisprodukte (EVN) in BigQuery",
    schedule_interval=None,
    catchup=False,
    max_active_runs=1,
    template_searchpath=[DAG_FOLDER],
    tags=["bigquery", "bert", "basisprodukt", "evn"],
) as dag:

    # Execute the BigQuery truncation and insertion logic utilizing the relative template path.
    # Dynamic parameters are safely evaluated during runtime via Jinja templates to prevent DB access overhead.
    # Uses var.json.get for safe variable lookups as recommended.
    process_evn_basis_products = BigQueryExecuteQueryOperator( 
        task_id="process_evn_basis_products",
        sql="sql/d_ausd_bp_ta_bpr_evn.sql",
        use_legacy_sql=False,
        gcp_conn_id="google_cloud_default",
        location="EU",
    )

    process_evn_basis_products