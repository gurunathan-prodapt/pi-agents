"""
Airflow DAG for DW.DWH_ABTN_SMART_KUBI
"""
from airflow import DAG
from airflow.models import Variable
from airflow.providers.google.cloud.operators.bigquery import BigQueryInsertJobOperator
from datetime import datetime, timedelta

# Retain legacy scheduler-set variable
DWH_JOB_KENNUNG = 'ABTN_SMART_KUBI'

default_args = {
    'owner': 'dwh_operator',
    'depends_on_past': False,
    'start_date': datetime(2015, 9, 18),
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
}

# The job is monthly, as seen from l_monats_id and the design document.
with DAG(
    dag_id='dwh_abtn_smart_kubi_dag',
    default_args=default_args,
    schedule_interval='@monthly',
    catchup=False,
    max_active_runs=1,
    template_searchpath=['/home/airflow/gcs/dags', '/'],
    tags=['dwh', 'abtn', 'smart', 'kubi'],
) as dag:

    # Retrieve GLOBAL environment variables cleanly via Airflow Variable.get
    gcp_project = Variable.get("GCP_PROJECT")
    bq_dataset = Variable.get("BQ_DATASET")

    # Define query parameters to inject into the SQL script
    query_params = [
        {
            "name": "p_monats_id",
            "parameterType": {"type": "INT64"},
            "parameterValue": {
                # Shifts the execution date to the previous month in YYYYMM format matching the legacy MONATSID calculation
                "value": "{{ (execution_date - macros.dateutil.relativedelta.relativedelta(months=1)).strftime('%Y%m') }}"
            }
        },
        {
            "name": "p_eintragsnr",
            "parameterType": {"type": "INT64"},
            "parameterValue": {
                "value": "{{ task_instance.try_number }}"
            }
        }
    ]

    execute_smart_kubi = BigQueryInsertJobOperator(
        task_id='execute_d_abtn_x_smart_kubi',
        configuration={
            "query": {
                "query": "{% include 'local/home/gurunathan_t/kubi/d_abtn_x_smart_kubi.sql' %}",
                "useLegacySql": False,
                "parameterMode": "NAMED",
                "queryParameters": query_params,
                "defaultDataset": {
                    "projectId": gcp_project,
                    "datasetId": bq_dataset
                }
            }
        },
        gcp_conn_id='google_cloud_default',
    )

    execute_smart_kubi