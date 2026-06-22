# Airflow DAG for orchestrating r_aurd_rechstan
# Replaces vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/k_aurd_rechstan.ksh
import pendulum

from airflow.models.dag import DAG
from airflow.providers.google.cloud.operators.bigquery import BigQueryExecuteStoredProcedureOperator

# Define default arguments for the DAG
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
}

# Define the DAG
with DAG(
    dag_id='r_aurd_rechstan_orchestration',
    start_date=pendulum.datetime(2023, 1, 1, tz="UTC"),
    schedule_interval='@daily', # Example: run daily, adjust as needed
    catchup=False,
    tags=['bigquery', 'etl', 'daily'],
    default_args=default_args,
    description='Orchestrates the BigQuery Stored Procedure for r_aurd_rechstan, replacing k_aurd_rechstan.ksh',
) as dag:
    # Task to execute the BigQuery Stored Procedure
    execute_rechstan_sp = BigQueryExecuteStoredProcedureOperator(
        task_id='execute_r_aurd_rechstan_sp',
        project_id='my_project',
        dataset_id='my_dataset',
        procedure_id='r_aurd_rechstan',
        parameters=[
            {"name": "p_job_kennung", "argument_type": "STRING", "value": "DAILY_RECHSTAN_JOB"},
            {"name": "p_eintrags_nr", "argument_type": "STRING", "value": "1001"},
            {"name": "p_stichtag", "argument_type": "STRING", "value": "{{ ds_nodash }}"}, # Airflow macro for current date in YYYYMMDD, adjust if needed for DDMMYYYY
            {"name": "p_wiederanlauf_wert", "argument_type": "INT64", "value": 0}
        ],
        # If the stored procedure expects DDMMYYYY, we need to format ds_nodash
        # 'value': "{{ ds_nodash[6:] + ds_nodash[4:6] + ds_nodash[0:4] }}" for DDMMYYYY
        # Assuming the stored procedure in BigQuery will parse 'YYYYMMDD' if `ds_nodash` is passed
        # OR, we need to adjust the stored procedure's parsing if `ds_nodash` format 'YYYYMMDD' is used
        # The stored procedure expects 'DDMMYYYY', so let's adjust the macro
        # Example using a Python callable to format the date:
        # parameters=[
        #     {"name": "p_job_kennung", "argument_type": "STRING", "value": "DAILY_RECHSTAN_JOB"},
        #     {"name": "p_eintrags_nr", "argument_type": "STRING", "value": "1001"},
        #     {"name": "p_stichtag", "argument_type": "STRING", "value": "{{ execution_date.strftime('%d%m%Y') }}"},
        #     {"name": "p_wiederanlauf_wert", "argument_type": "INT64", "value": 0}
        # ],
    )
    # The current BigQuery SP expects `DDMMYYYY`. Airflow's `ds_nodash` is `YYYYMMDD`.
    # I'll use `execution_date.strftime('%d%m%Y')` to pass the correct format.
    execute_rechstan_sp.parameters[2]["value"] = "{{ execution_date.strftime('%d%m%Y') }}"