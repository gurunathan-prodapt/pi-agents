from __future__ import annotations

import pendulum

from airflow.models.dag import DAG
from airflow.providers.google.cloud.operators.bigquery import BigQueryExecuteQueryOperator

# Define default arguments for the DAG
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
}

with DAG(
    dag_id='vertragsdatenabgleich_ta_apn_ve',
    start_date=pendulum.datetime(2023, 1, 1, tz="UTC"),
    schedule=None,  # Set your desired schedule here, e.g., '@daily' or '0 5 * * *'
    catchup=False,
    tags=['bigquery', 'transformation', 'wrapper'],
    default_args=default_args,
    description='Airflow DAG to orchestrate the BigQuery contract data reconciliation wrapper procedure.',
) as dag:
    # Legacy Source: vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/r_ausd_v_ta_apn_ve.ksh
    # Job: vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/r_ausd_v_ta_apn_ve.ksh

    # Define parameters for the BigQuery stored procedure call
    # These can be dynamic via Airflow macros or XComs in a real scenario
    job_kennung_val = 'TA_APN_VE_DAILY'
    # Use execution_date as Stichtag, formatted as YYYY-MM-DD.
    # 'ds' macro provides the execution date as YYYY-MM-DD.
    stichtag_val = "{{ ds }}"

    call_wrapper_procedure = BigQueryExecuteQueryOperator(
        task_id='call_vertragsdatenabgleich_wrapper',
        gcp_conn_id='google_cloud_default', # Ensure you have a GCP connection configured in Airflow
        sql=f"""
            CALL `my-gcp-project.my_dataset.vertragsdatenabgleich_wrapper`(
                p_job_kennung_param => '{job_kennung_val}',
                p_stichtag_param => PARSE_DATE('%Y-%m-%d', '{stichtag_val}'),
                p_show_help => FALSE
            );
        """,
        use_legacy_sql=False,
        # If the stored procedure raises an error, BigQueryExecuteQueryOperator will fail the task.
        # Ensure that job_control.status is updated appropriately within the BQ SP.
    )