# This DAG replaces the legacy KornShell script: vobs/dw_source/isrpt/isbert/install_save/k_ausd_bp_ta_tarifoption.ksh
# Legacy job: DW.BERT_AUSD_BP_TA_TARIFOPTION

from airflow import DAG
from airflow.providers.google.cloud.operators.bigquery import BigQueryInsertJobOperator
from airflow.utils.dates import days_ago
from datetime import datetime

with DAG(
    dag_id="dw_bert_ausd_bp_ta_tarifoption_dag",
    start_date=days_ago(1),
    schedule_interval="@daily",  # Assuming daily execution as per common ETL patterns and `p_Stichtag`
    catchup=False,
    tags=["bert", "bigquery", "etl"],
    default_args={
        "owner": "airflow",
        "depends_on_past": False,
        "email_on_failure": False,
        "email_on_retry": False,
        "retries": 1,
    }
) as dag:
    execute_tarifoption_sql = BigQueryInsertJobOperator(
        task_id="execute_tarifoption_sql",
        configuration={
            "query": {
                "query": "{% include 'sql/dw_bert_ausd_bp_ta_tarifoption.sql' %}",
                "useLegacySql": False,
                # The SQL script itself handles table creation and population,
                # so 'destinationTable', 'createDisposition', and 'writeDisposition'
                # are not specified at the operator level.
            }
        },
        gcp_conn_id="google_cloud_default",
        # Project ID can be implicitly resolved by BigQuery based on the connection,
        # or explicitly defined in the SQL table paths (e.g., `project_id.dataset_id.table_name`).
        # Here, `isbert_schema.table_name` implies the dataset `isbert_schema` within the
        # project associated with the service account of `google_cloud_default` connection.
    )