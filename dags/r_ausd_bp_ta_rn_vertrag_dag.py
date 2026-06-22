# Apache Airflow DAG for invoking the BigQuery Stored Procedure
# Legacy Source: vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/k_ausd_bp_ta_rn_vertrag.ksh
# This DAG demonstrates how to orchestrate the BigQuery Stored Procedure.

from airflow import DAG
from airflow.providers.google.cloud.operators.bigquery import BigQueryExecuteStoredProcedureOperator
from airflow.utils.dates import days_ago

default_args = {
    'owner': 'airflow',
    'start_date': days_ago(1),
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
}

with DAG(
    dag_id='bq_r_ausd_bp_ta_rn_vertrag',
    default_args=default_args,
    description='Invokes BigQuery Stored Procedure for k_ausd_bp_ta_rn_vertrag migration',
    schedule_interval=None,  # Set your desired schedule interval here, e.g., '0 0 * * *' for daily
    catchup=False,
    tags=['bigquery', 'migration', 'isbert'],
) as dag:
    # Example for deriving current date in DDMMYYYY format for p_Stichtag
    # This can be adjusted based on desired date for the job
    stichtag_formatted = "{{ ds_nodash }}" # Airflow macro for 'YYYYMMDD'
    # If the stored procedure expects DDMMYYYY, you might need a custom macro or PythonOperator
    # For now, let's assume ds_nodash can be adapted or parsed within the procedure.
    # If not, you'd convert: "{{ execution_date.strftime('%d%m%Y') }}"

    call_procedure = BigQueryExecuteStoredProcedureOperator(
        task_id='execute_r_ausd_bp_ta_rn_vertrag',
        project_id='your_gcp_project',
        dataset_id='your_bq_dataset',
        procedure_id='r_ausd_bp_ta_rn_vertrag',
        parameters=[
            {"name": "p_JobKennung", "parameterType": {"type": "STRING"}, "parameterValue": {"value": "AIRFLOW_JOB"}},
            {"name": "p_EintragsNr", "parameterType": {"type": "STRING"}, "parameterValue": {"value": "AIRFLOW_ENTRY_123"}},
            {"name": "p_Stichtag", "parameterType": {"type": "STRING"}, "parameterValue": {"value": "{{ execution_date.strftime('%d%m%Y') }}"}}, # DDMMYYYY format
            {"name": "p_wiederanlaufWert", "parameterType": {"type": "STRING"}, "parameterValue": {"value": "0"}},
        ],
    )