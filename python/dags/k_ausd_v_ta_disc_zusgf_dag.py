#
# Airflow DAG for k_ausd_v_ta_disc_zusgf.ksh migration
# Replaces: vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/k_ausd_v_ta_disc_zusgf.ksh
#
from airflow import DAG
from airflow.providers.google.cloud.operators.bigquery import BigQueryExecuteStoredProcedureOperator
from airflow.utils.dates import days_ago
from datetime import timedelta

# Define project and dataset for BigQuery resources
# These should be configured in your Airflow environment variables or connections
# For demonstration, placeholders are used.
PROJECT_ID = 'my_project'
DATASET_ID = 'my_dataset'

with DAG(
    dag_id='k_ausd_v_ta_disc_zusgf_dag',
    start_date=days_ago(1),
    schedule_interval=None, # Define your schedule here, e.g., '0 0 * * *' for daily
    catchup=False,
    dagrun_timeout=timedelta(hours=1),
    tags=['isrpt', 'isbert', 'bigquery'],
    params={
        'job_kennung': {'type': 'string', 'default': 'DEFAULT_JOB_KENNUNG', 'title': 'Job Identifier'},
        'eintrags_nr': {'type': 'string', 'default': 'DEFAULT_EINTRAGS_NR', 'title': 'Entry Number'},
    },
    default_args={
        'owner': 'airflow',
        'depends_on_past': False,
        'email_on_failure': False,
        'email_on_retry': False,
        'retries': 1,
        'retry_delay': timedelta(minutes=5),
    }
) as dag:
    call_control_procedure = BigQueryExecuteStoredProcedureOperator(
        task_id='call_r_ausd_vertrag_control',
        project_id=PROJECT_ID,
        dataset_id=DATASET_ID,
        procedure_id='r_ausd_vertrag_control',
        gcp_conn_id='google_cloud_default', # Ensure this connection exists in Airflow
        parameters=[
            {'name': 'p_JobKennung', 'parameterType': {'type': 'STRING'}, 'value': '{{ params.job_kennung }}'},
            {'name': 'p_EintragsNr', 'parameterType': {'type': 'STRING'}, 'value': '{{ params.eintrags_nr }}'},
            # p_records_processed is an OUT parameter, it will be captured by BigQuery but not directly
            # used as an input parameter for the operator. If you need to use its value in downstream
            # tasks, you might need to query the job_run_log table.
        ]
    )

    # Optional: Add a task to check the job_run_log for success/metrics
    # This would involve a BigQueryGetDataOperator or similar.
    # For now, we rely on the procedure to log its outcome.