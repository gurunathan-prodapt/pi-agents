# Legacy Source: Upstream job invoking vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/k_ausd_v_ta_notice.ksh
# Description: Cloud Composer DAG to orchestrate the execution of the BigQuery Stored Procedure.
# This DAG replaces the upstream shell script (r_ausd_vertrag.ksh) that previously invoked k_ausd_v_ta_notice.ksh.

from __future__ import annotations

import pendulum

from airflow.models.dag import DAG
from airflow.operators.bash import BashOperator
from airflow.providers.google.cloud.operators.bigquery import BigQueryExecuteStoredProcedureOperator

# Define default arguments for the DAG
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': pendulum.duration(minutes=5),
}

with DAG(
    dag_id='r_ausd_vertrag_control_dag',
    start_date=pendulum.datetime(2023, 1, 1, tz="UTC"),
    catchup=False,
    schedule=None, # Define your schedule here, e.g., '0 0 * * *' for daily
    tags=['bigquery', 'elt', 'ta_notice'],
    default_args=default_args,
    description='DAG to execute the r_ausd_vertrag_control BigQuery Stored Procedure.'
) as dag:
    # Task to start the job - can be a simple BashOperator or a custom sensor
    start_job = BashOperator(
        task_id='start_job_process',
        bash_command='echo "Starting r_ausd_vertrag_control BigQuery job..."',
    )

    # Task to execute the BigQuery Stored Procedure
    # Replace 'your_gcp_project_id' with your actual GCP project ID
    # Replace 'mydataset' with your actual BigQuery dataset name
    execute_bq_sp = BigQueryExecuteStoredProcedureOperator(
        task_id='execute_bq_r_ausd_vertrag_control',
        project_id='your_gcp_project_id',
        dataset_id='mydataset',
        procedure_id='r_ausd_vertrag_control',
        gcp_conn_id='google_cloud_default', # Ensure this connection exists in Airflow
        parameters=[
            {'name': 'p_JobKennung', 'parameterType': {'type': 'STRING'}, 'value': 'TA_NOTICE_DAILY'},
            {'name': 'p_EintragsNr', 'parameterType': {'type': 'STRING'}, 'value': '{{ ds_nodash }}'}, # Example: Use execution date as EintragsNr
        ],
    )

    # Task for post-processing or success notification
    end_job = BashOperator(
        task_id='job_completed_successfully',
        bash_command='echo "r_ausd_vertrag_control BigQuery job finished."',
    )

    # Define task dependencies
    start_job >> execute_bq_sp >> end_job

# NOTE:
# 1. Replace 'your_gcp_project_id' with your actual Google Cloud Project ID.
# 2. Ensure 'google_cloud_default' Airflow connection is configured correctly with BigQuery access.
# 3. Adjust `schedule` parameter as per your requirements (e.g., daily, hourly, None for manual trigger).
# 4. The `p_JobKennung` and `p_EintragsNr` parameters passed to the BigQuery Stored Procedure
#    are examples. Adjust them based on your specific job configuration and dynamic needs.
#    `{{ ds_nodash }}` is an Airflow macro for the execution date as YYYYMMDD.