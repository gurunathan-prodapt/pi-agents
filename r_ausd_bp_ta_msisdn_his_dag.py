# Legacy Source: vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/r_ausd_bp_ta_msisdn_his.ksh
# This Airflow DAG orchestrates the execution of the BigQuery stored procedure
# `ausd_bp_ta_msisdn_his_wrapper_sp` which replaces the legacy KornShell script.

from airflow import DAG
from airflow.providers.google.cloud.operators.bigquery import BigQueryExecuteStoredProcedureOperator
from airflow.utils.dates import days_ago
import pendulum

# Define your GCP Project ID and BigQuery Dataset ID
# These values should be replaced with your actual GCP environment details.
GCP_PROJECT_ID = 'project'  # e.g., 'your-gcp-project-id'
BQ_DATASET_ID = 'dataset'   # e.g., 'your_bigquery_dataset'

with DAG(
    dag_id='r_ausd_bp_ta_msisdn_his_dag',
    start_date=pendulum.datetime(2023, 1, 1, tz="UTC"),
    schedule_interval='@daily', # Adjust as per actual scheduling requirements (e.g., '0 4 * * *' for daily at 4 AM)
    catchup=False,              # Set to True if historical runs are needed
    tags=['isrpt', 'isbert', 'bigquery', 'dwh'],
    params={
        'wiederanlaufwert': 0 # Default value for p_wiederanlaufWert, can be overridden during manual trigger
    }
) as dag:
    # The `p_stichtag_input` parameter for the BigQuery SP expects 'DDMMYYYY' format.
    # Airflow's `ds` variable provides the execution date in 'YYYY-MM-DD' format.
    # We transform `ds` into `DDMMYYYY` using JINJA templating.
    # Example: if ds = '2023-10-26', then '{{ ds[8:10] }}{{ ds[5:7] }}{{ ds[0:4] }}' results in '26102023'.
    call_wrapper_sp = BigQueryExecuteStoredProcedureOperator(
        task_id='call_ausd_bp_ta_msisdn_his_wrapper_sp',
        project_id=GCP_PROJECT_ID,
        dataset_id=BQ_DATASET_ID,
        procedure_id='ausd_bp_ta_msisdn_his_wrapper_sp',
        parameters=[
            {
                "name": "p_stichtag_input",
                "parameterType": {"type": "STRING"},
                "parameterValue": {"value": '{{ ds[8:10] }}{{ ds[5:7] }}{{ ds[0:4] }}'}
            },
            {
                "name": "p_wiederanlaufWert_input",
                "parameterType": {"type": "INT64"},
                "parameterValue": {"value": "{{ params.wiederanlaufwert }}"}
            }
        ]
    )