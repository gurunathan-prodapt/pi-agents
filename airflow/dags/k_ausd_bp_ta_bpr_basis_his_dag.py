import pendulum
from airflow import DAG
from airflow.providers.google.cloud.operators.bigquery import BigQueryStoredProcedureOperator
from airflow.utils.trigger_rule import TriggerRule

# This DAG replaces: vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/k_ausd_bp_ta_bpr_basis_his.ksh

with DAG(
    dag_id='k_ausd_bp_ta_bpr_basis_his_orchestration',
    start_date=pendulum.datetime(2023, 1, 1, tz="UTC"),
    schedule=None,  # Define your schedule here, e.g., '0 0 * * *' for daily
    catchup=False,
    tags=['isbert', 'bigquery', 'etl'],
    params={
        'job_kennung': {'type': 'string', 'default': 'DEFAULT_JOB', 'title': 'Job Identifier'},
        'eintrags_nr': {'type': 'string', 'default': 'DEFAULT_ENTRY', 'title': 'Entry Number'},
        'stichtag': {'type': 'string', 'default': '{{ ds_nodash }}', 'title': 'Reference Date (DDMMYYYY)'}, # Default to current date for example
        'wiederanlauf_wert': {'type': 'string', 'default': '0', 'title': 'Restart Value'},
    },
) as dag:
    # Call the BigQuery Stored Procedure that orchestrates the data processing
    execute_orchestrator_sp = BigQueryStoredProcedureOperator(
        task_id='execute_bigquery_orchestrator_sp',
        project_id='project', # Replace with your GCP Project ID
        dataset_id='dataset', # Replace with your BigQuery Dataset ID
        procedure_id='r_ausd_bp_ta_bpr_basis_his',
        parameters=[
            {'name': 'p_JobKennung', 'parameterType': {'type': 'STRING'}, 'defaultValue': '{{ params.job_kennung }}'},
            {'name': 'p_EintragsNr', 'parameterType': {'type': 'STRING'}, 'defaultValue': '{{ params.eintrags_nr }}'},
            {'name': 'p_Stichtag', 'parameterType': {'type': 'STRING'}, 'defaultValue': '{{ params.stichtag }}'},
            {'name': 'p_wiederanlaufWert', 'parameterType': {'type': 'STRING'}, 'defaultValue': '{{ params.wiederanlauf_wert }}'},
        ],
        gcp_conn_id='google_cloud_default', # Ensure this connection is configured in Airflow
        # The procedure handles its own error logging, but Airflow will also log task failures.
        # on_failure_callback=lambda context: send_alert(context), # Example: custom failure callback
    )