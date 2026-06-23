# Apache Airflow DAG for k_ausd_bp_ta_msisdn.ksh migration
# Legacy Source: vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/k_ausd_bp_ta_msisdn.ksh
# Job: vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/k_ausd_bp_ta_msisdn.ksh

from airflow import DAG
from airflow.providers.google.cloud.operators.bigquery import BigQueryInsertJobOperator
from airflow.utils.dates import days_ago
from datetime import timedelta

# Define project and dataset for BigQuery resources
# Replace with your actual GCP Project ID and BigQuery Dataset ID
GCP_PROJECT_ID = 'your_gcp_project'
BQ_DATASET_ID = 'your_bq_dataset'

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    dag_id='k_ausd_bp_ta_msisdn_orchestration_dag',
    default_args=default_args,
    description='Orchestrates BigQuery stored procedure for MSISDN data preparation',
    schedule_interval=None, # Define your schedule, e.g., '0 0 * * *' for daily
    start_date=days_ago(1),
    tags=['bigquery', 'etl', 'msisdn'],
    catchup=False,
) as dag:
    # Task to call the main orchestration BigQuery Stored Procedure
    call_main_sp = BigQueryInsertJobOperator(
        task_id='call_r_ausd_bp_ta_msisdn_sp',
        configuration={
            "query": {
                "query": """
                    CALL `{}.{}.r_ausd_bp_ta_msisdn`(
                        p_job_kennung => @job_kennung,
                        p_eintrags_nr => @eintrags_nr,
                        p_stichtag => @stichtag,
                        p_wiederanlauf_wert => @wiederanlauf_wert
                    );
                """.format(GCP_PROJECT_ID, BQ_DATASET_ID),
                "useLegacySql": False,
                "queryParameters": [
                    {
                        "name": "job_kennung",
                        "parameterType": {"type": "STRING"},
                        "parameterValue": {"value": "{{ dag_run.conf.get('job_kennung', 'DEFAULT_JOB_KENNUNG') }}"}
                    },
                    {
                        "name": "eintrags_nr",
                        "parameterType": {"type": "STRING"},
                        "parameterValue": {"value": "{{ dag_run.conf.get('eintrags_nr', 'DEFAULT_EINTRAGS_NR') }}"}
                    },
                    {
                        "name": "stichtag",
                        "parameterType": {"type": "STRING"},
                        # Expects DDMMYYYY format. Example: 31122023
                        # Default to yesterday's date in DDMMYYYY format if not provided
                        "parameterValue": {"value": "{{ dag_run.conf.get('stichtag', ds_nodash[6:] + ds_nodash[4:6] + ds_nodash[0:4]) }}"}
                    },
                    {
                        "name": "wiederanlauf_wert",
                        "parameterType": {"type": "STRING"},
                        "parameterValue": {"value": "{{ dag_run.conf.get('wiederanlauf_wert', '') }}"}
                    }
                ],
            }
        },
        gcp_conn_id='google_cloud_default', # Ensure this connection is configured in Airflow
    )

    # Optional task to call the post-processing SP if needed
    # Uncomment and configure if the commented post-processing logic is to be implemented.
    # call_postprocess_sp = BigQueryInsertJobOperator(
    #     task_id='call_postprocess_cibasis_sp',
    #     configuration={
    #         "query": {
    #             "query": """
    #                 CALL `{}.{}.postprocess_cibasis`(
    #                     p_stichtag_date => PARSE_DATE('%d%m%Y', @stichtag_raw)
    #                 );
    #             """.format(GCP_PROJECT_ID, BQ_DATASET_ID),
    #             "useLegacySql": False,
    #             "queryParameters": [
    #                 {
    #                     "name": "stichtag_raw",
    #                     "parameterType": {"type": "STRING"},
    #                     "parameterValue": {"value": "{{ dag_run.conf.get('stichtag', ds_nodash[6:] + ds_nodash[4:6] + ds_nodash[0:4]) }}"}
    #                 }
    #             ],
    #         }
    #     },
    #     gcp_conn_id='google_cloud_default',
    # )

    # Define task dependencies
    # If post-processing is enabled, add it after the main SP call.
    # call_main_sp >> call_postprocess_sp
    call_main_sp