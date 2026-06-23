# Airflow DAG for BigQuery Stored Procedure Orchestration
# Legacy Source: vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/k_ausd_v_ta_vvl_dwh.ksh
# Job: vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/k_ausd_v_ta_vvl_dwh.ksh

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
    dag_id='r_ausd_vertrag_control_dag',
    default_args=default_args,
    description='DAG to invoke r_ausd_vertrag_control BigQuery Stored Procedure',
    schedule_interval='@daily',
    catchup=False,
    tags=['bigquery', 'etl', 'daily'],
) as dag:
    call_bq_procedure = BigQueryExecuteStoredProcedureOperator(
        task_id='call_r_ausd_vertrag_control_sp',
        project_id='your_gcp_project_id',
        dataset_id='your_bq_dataset_id',
        procedure_id='r_ausd_vertrag_control',
        gcp_conn_id='google_cloud_default', # Ensure this connection is configured in Airflow
        parameters=[
            {"name": "p_JobKennung", "parameter_type": {"type": "STRING"}, "value": "TA_VVL_DWH_DAILY"},
            {"name": "p_EintragsNr", "parameter_type": {"type": "STRING"}, "value": "{{ ds_nodash }}"}
        ]
    )