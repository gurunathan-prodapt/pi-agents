# This Airflow DAG replaces the legacy UC4 job DW.BERT_AUSD_V_TA_CNTRCT_TEMPL
# and orchestrates the BigQuery SQL transformation for mirroring Carmen contract templates.

from __future__ import annotations

import pendulum

from airflow.models.dag import DAG
from airflow.providers.google.cloud.operators.bigquery import BigQueryOperator

with DAG(
    dag_id="dw_bert_ausd_v_ta_cntrct_temmpl",
    start_date=pendulum.datetime(2023, 1, 1, tz="UTC"),
    schedule=None,  # Set your desired schedule, e.g., "@daily", "0 0 * * *"
    catchup=False,
    tags=["bert", "contract_template", "bigquery"],
    description="Mirrors Carmen contract templates from staging to target BigQuery table.",
) as dag:
    # Task to execute the core BigQuery SQL transformation
    execute_contract_template_mirroring = BigQueryOperator(
        task_id="mirror_contract_templates",
        sql="d_ausd_v_ta_cntrct_templ_bq.sql",
        use_legacy_sql=False,
        # Ensure that the connection ID to BigQuery is correctly configured
        # e.g., 'google_cloud_default' or your specific BigQuery connection
        bigquery_conn_id="google_cloud_default",
        # Optionally, specify the project_id and dataset_id if not using the default
        # project_id="your-gcp-project-id",
        # dataset_id="your_bigquery_dataset",
    )