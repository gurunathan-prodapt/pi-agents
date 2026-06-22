#
# Airflow DAG for job vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/r_ausd_v_ta_p_discount.ksh
# Orchestrates the BigQuery Stored Procedure r_ausd_v_ta_p_discount.
#
from __future__ import annotations

import pendulum

from airflow.models.dag import DAG
from airflow.providers.google.cloud.operators.bigquery import BigQueryExecuteStoredProcedureOperator

# Define BigQuery project and dataset
PROJECT_ID = 'your-gcp-project-id'  # Replace with your GCP project ID
DATASET_ID = 'your_bigquery_dataset'  # Replace with your BigQuery dataset ID

# Define default arguments for the DAG
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
}

with DAG(
    dag_id='r_ausd_v_ta_p_discount_dag',
    start_date=pendulum.datetime(2023, 1, 1, tz="UTC"),
    schedule=None,  # Set your desired schedule here, e.g., '@daily', '0 0 * * *'
    catchup=False,
    tags=['bigquery', 'etl'],
    default_args=default_args,
    description='Orchestrates the BigQuery Stored Procedure for r_ausd_v_ta_p_discount',
) as dag:
    # Task to execute the BigQuery Stored Procedure
    execute_bsp_r_ausd_v_ta_p_discount = BigQueryExecuteStoredProcedureOperator(
        task_id='call_r_ausd_v_ta_p_discount_sp',
        project_id=PROJECT_ID,
        dataset_id=DATASET_ID,
        procedure_id='r_ausd_v_ta_p_discount',
        gcp_conn_id='google_cloud_default',  # Ensure you have a BigQuery connection configured
        parameters={
            "p_job_kennung": "BERT_R_AUS_D_TA_P_DISCOUNT",  # Example value, adjust as needed
            "p_eintrags_nr": "1"  # Example value, adjust as needed
        }
    )