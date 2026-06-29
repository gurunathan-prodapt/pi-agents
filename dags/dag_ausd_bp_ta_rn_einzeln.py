from datetime import timedelta
from airflow import DAG
from airflow.providers.google.cloud.operators.bigquery import BigQueryExecuteQueryOperator
from airflow.utils.dates import days_ago

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "start_date": days_ago(1),
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    dag_id="dag_ausd_bp_ta_rn_einzeln",
    default_args=default_args,
    description="Orchestration DAG for BERT_P_BASISPRODUKT Aufbereitung der instantiierten Basisprodukte",
    schedule_interval=None,
    template_searchpath=["/home/airflow/gcs/dags/gcp_sql", "/opt/airflow/dags/gcp_sql"],
    catchup=False,
    max_active_runs=1,
    tags=["bigquery", "migration", "bert"],
) as dag:

    process_rn_einzeln = BigQueryExecuteQueryOperator(
        task_id="process_rn_einzeln",
        sql="d_ausd_bp_ta_rn_einzeln.sql",
        use_legacy_sql=False,
        location="{{ var.value.bq_location }}",
        gcp_conn_id="google_cloud_default",
    )

    process_rn_einzeln