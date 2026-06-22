#
# Target: Airflow DAG for BigQuery Stored Procedure orchestration
# Legacy Source: vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/r_ausd_bp_ta_bpr_opt_text.ksh
# Job: vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/r_ausd_bp_ta_bpr_opt_text.ksh
#

from __future__ import annotations

import pendulum

from airflow.models.dag import DAG
from airflow.providers.google.cloud.operators.bigquery import BigQueryInsertJobOperator

PROJECT_ID = 'project'
DATASET_ID = 'dataset'
WRAPPER_PROCEDURE = f'{PROJECT_ID}.{DATASET_ID}.ausd_bp_ta_bpr_opt_text_wrapper'

with DAG(
    dag_id='ausd_bp_ta_bpr_opt_text_dag',
    start_date=pendulum.datetime(2023, 1, 1, tz="UTC"),
    schedule=None,  # Or set a schedule, e.g., '@daily'
    catchup=False,
    tags=['bigquery', 'etl', 'bert'],
    description='Orchestrates the BigQuery stored procedure for BERT basic product provisioning.',
) as dag:
    call_wrapper_sp = BigQueryInsertJobOperator(
        task_id='call_ausd_bp_ta_bpr_opt_text_wrapper',
        configuration={
            'query': {
                'query': f'CALL {WRAPPER_PROCEDURE}(@p_stichtag, @p_wiederanlaufWert)',
                'useLegacySql': False,
                'queryParameters': [
                    {
                        'name': 'p_stichtag',
                        'parameterType': {'type': 'STRING'},
                        'parameterValue': {'value': '{{ ds_nodash }}'} # Passes execution date in YYYYMMDD format
                                                                       # (or 'None' if procedure handles it)
                    },
                    {
                        'name': 'p_wiederanlaufWert',
                        'parameterType': {'type': 'INT64'},
                        'parameterValue': {'value': '0'} # Default restart value
                    }
                ]
            }
        },
        gcp_conn_id='google_cloud_default', # Ensure this connection ID is configured in Airflow
    )