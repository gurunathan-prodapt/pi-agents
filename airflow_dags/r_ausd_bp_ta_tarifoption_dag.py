# Migrated from vobs/dw_source/isrpt/isbert/install_save/k_ausd_bp_ta_tarifoption.ksh
from airflow import DAG
from airflow.providers.google.cloud.operators.bigquery import BigQueryExecuteStoredProcedureOperator
from datetime import datetime

with DAG(
    dag_id='r_ausd_bp_ta_tarifoption_dag',
    start_date=datetime(2023, 1, 1),
    schedule_interval=None,
    catchup=False,
    tags=['bigquery', 'etl'],
) as dag:
    execute_bq_sp = BigQueryExecuteStoredProcedureOperator(
        task_id='execute_stored_procedure',
        project_id='your-gcp-project-id',
        dataset_id='your_bigquery_dataset',
        procedure_id='r_ausd_bp_ta_tarifoption',
        gcp_conn_id='google_cloud_default', # Ensure this connection is configured
        parameters=[
            {'name': 'p_JobKennung', 'parameterType': {'type': 'STRING'}, 'value': 'JOB_ABC'},
            {'name': 'p_EintragsNr', 'parameterType': {'type': 'STRING'}, 'value': 'ENTRY_123'},
            {'name': 'p_Stichtag', 'parameterType': {'type': 'STRING'}, 'value': '{{ ds_nodash }}'}, # Example: Use Airflow macro for date
            {'name': 'p_wiederanlaufWert', 'parameterType': {'type': 'STRING'}, 'value': '0'}
        ]
    )