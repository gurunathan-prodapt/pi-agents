# Legacy Source: vobs/dw_source/isdwh/uc4_prod_exports/UC4_PROD - 0001/DWH/BERT/PRODUKTION/DW.BERT_AUSD_V_TA_VERTRAG_TMP.xml
# Job: DW.BERT_AUSD_V_TA_VERTRAG_TMP

from airflow import DAG
from airflow.providers.google.cloud.operators.bigquery import BigQueryOperator
from datetime import datetime, timedelta

# Define GCP project and dataset
GCP_PROJECT_ID = "your-gcp-project-id"  # Replace with your actual GCP Project ID
BIGQUERY_DATASET = "bert_dw_staging" # As specified in design document
BIGQUERY_TARGET_TABLE = f"{GCP_PROJECT_ID}.{BIGQUERY_DATASET}.bert_ausd_v_ta_vertrag_tmp" # As specified in design document
SQL_TRANSFORMATION_FILE = "dags/dw_bert_ausd_v_ta_vertrag_tmp_transform.sql" # Assuming SQL file is in the dags folder

default_args = {
    'owner': 'DW.UNIX.ISBERT',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 0, # As per UC4 design output
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    dag_id='dw_bert_ausd_v_ta_vertrag_tmp',
    default_args=default_args,
    description='Airflow DAG for DW.BERT_AUSD_V_TA_VERTRAG_TMP - Populates temporary contract table in BigQuery',
    schedule_interval=None, # As per design document, no schedule provided in UC4 XML
    start_date=datetime(2023, 1, 1), # Placeholder, update as needed
    catchup=False,
    tags=['bert', 'bigquery', 'etl'],
    max_active_runs=1,
    is_paused_upon_creation=False,
) as dag:
    
    # Task to execute the BigQuery SQL transformation
    execute_bigquery_transformation = BigQueryOperator(
        task_id='execute_bigquery_transformation',
        sql=f'{{% include "{SQL_TRANSFORMATION_FILE}" %}}',
        use_legacy_sql=False,
        params={
            'project_id': GCP_PROJECT_ID,
            'dataset': BIGQUERY_DATASET,
            'target_table': BIGQUERY_TARGET_TABLE,
        },
        # The SQL script already contains the TRUNCATE and INSERT INTO logic
        # For simplicity and direct translation, we are executing the entire script.
        # If the SQL were only a SELECT, an additional BigQueryCreateEmptyTableOperator
        # and BigQueryInsertJobOperator might be used.
    )

    # Define task dependencies
    # Only one task in this DAG
    # start_task >> execute_bigquery_transformation >> end_task (implicit)