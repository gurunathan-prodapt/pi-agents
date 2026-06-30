"""
Airflow DAG for ausd_bp_ta_apn_vertrag.

This DAG replaces the legacy UC4/Automic batch job that aggregates active basic
products from sof_ta_bpr_apn into sof_ta_apn_vertrag using BigQuery SQL.
It runs on the schedule defined in the migration design document, with
catchup disabled and max_active_runs set to 1.
"""

from datetime import timedelta
import os

from airflow import DAG
from airflow.models import Variable
from airflow.utils.dates import days_ago
from airflow.providers.google.cloud.operators.bigquery import BigQueryExecuteQueryOperator

# Resolve environment configuration variables from Airflow with defaults from the design document.
GCP_PROJECT_ID = Variable.get("gcp_project_id", default_var="gcp-dwh-prod")
BQ_DATASET = Variable.get("bq_dataset", default_var="isbert_schema")
BQ_LOCATION = Variable.get("bq_location", default_var="EU")
GCP_CONN_ID = Variable.get("gcp_conn_id", default_var="google_cloud_default")

# Default DAG execution parameters
default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "start_date": days_ago(1),
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

# Define DAG path context to locate external SQL templates
DAGS_FOLDER = os.environ.get("AIRFLOW_HOME", "/usr/local/airflow")

with DAG(
    dag_id="dw_bert_ausd_bp_ta_apn_vertrag",
    default_args=default_args,
    description="BERT_P_BASISPRODUKT: Aufbereitung der instantiierten Basisprodukte",
    schedule_interval=None,  # Configured on-demand/ad-hoc as per UC4 definition
    catchup=False,
    max_active_runs=1,
    tags=["bigquery", "dw", "bert"],
) as dag:

    # Core SQL transformation operator to replace Oracle PL/SQL script
    process_apn_vertrag = BigQueryExecuteQueryOperator(
        task_id="process_apn_vertrag",
        sql="gcp/bigquery/sql/d_ausd_bp_ta_apn_vertrag.sql",
        use_legacy_sql=False,
        location=BQ_LOCATION,
        gcp_conn_id=GCP_CONN_ID,
        write_disposition="WRITE_TRUNCATE",
        create_disposition="CREATE_IF_NEEDED",
    )

    process_apn_vertrag