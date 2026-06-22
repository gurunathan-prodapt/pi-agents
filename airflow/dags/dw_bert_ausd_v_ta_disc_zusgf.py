from airflow import DAG
from airflow.providers.google.cloud.operators.bigquery import BigQueryExecuteQueryOperator
from datetime import datetime

# Migrated from: vobs/dw_source/isdwh/uc4_prod_exports/UC4_PROD - 0001/DWH/BERT/PRODUKTION/DW.BERT_STAMMDATEN_JP/DW.BERT_P_VERTRAG_JP/DW.BERT_AUSD_V_TA_DISC_ZUSGF.xml
# Job: BERT_V_TA_DISC_ZUSGF
# Purpose: Orchestrates the BigQuery SQL transformation to concatenate discount descriptions.

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2023, 1, 1),
    'retries': 1,
    # Replace 'gcp_project_id' with your actual GCP Project ID.
    'project_id': 'gcp_project_id',
}

with DAG(
    dag_id='dw_bert_ausd_v_ta_disc_zusgf',
    default_args=default_args,
    description='Concatenates discount descriptions into sof_ta_disc_zusgf in BigQuery',
    # Adjust schedule_interval to match the original UC4 job frequency, e.g., '@daily', '0 5 * * *'
    schedule_interval='@daily',
    catchup=False,
    tags=['bigquery', 'bert', 'transformation'],
) as dag:
    # Task to execute the BigQuery SQL transformation.
    # The SQL script `d_ausd_v_ta_disc_zusgf_bq.sql` handles all the transformation logic
    # including date derivation (`v_datum`) and table truncation.
    # This task replaces the combined functionality of the original KornShell and PL/SQL scripts.
    execute_bq_transformation = BigQueryExecuteQueryOperator(
        task_id='execute_discount_concatenation',
        sql='bigquery/sql/d_ausd_v_ta_disc_zusgf_bq.sql',
        use_legacy_sql=False,
        # Ensure 'google_cloud_default' Airflow connection is configured for BigQuery access.
        gcp_conn_id='google_cloud_default',
    )

    # Further tasks (e.g., data quality checks, downstream dependencies) could be added here.
    # For this migration, the core logic is a single BigQuery SQL execution.