from datetime import datetime, timedelta
from airflow import DAG
from airflow.providers.google.cloud.operators.bigquery import BigQueryInsertJobOperator
from airflow.models import Variable

# Global environment-wide variables
GCP_PROJECT = Variable.get("GCP_PROJECT")
BQ_DATASET = Variable.get("BQ_DATASET")

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2015, 9, 18),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'dw_dwh_abtn_smart_kubi',
    default_args=default_args,
    description='Aggregation Job to load data into DWH$TA_T_SMART_KUBI table',
    schedule_interval='@monthly',
    catchup=False,
    template_searchpath=['/home/airflow/gcs/dags/sql/', '/home/airflow/gcs/dags/'],
)

execute_sql = BigQueryInsertJobOperator(
    task_id='execute_d_abtn_x_smart_kubi',
    configuration={
        "query": {
            "query": "d_abtn_x_smart_kubi.sql",
            "useLegacySql": False,
            "defaultDataset": {
                "datasetId": BQ_DATASET,
                "projectId": GCP_PROJECT
            },
            "queryParameters": [
                {
                    "name": "param_monats_id",
                    "parameterType": {"type": "INT64"},
                    "parameterValue": {"value": "{{ (execution_date - macros.dateutil.relativedelta.relativedelta(months=1)).strftime('%Y%m') }}"}
                },
                {
                    "name": "param_eintrags_nr",
                    "parameterType": {"type": "INT64"},
                    "parameterValue": {"value": "{{ ti.try_number }}"}
                }
            ]
        }
    },
    gcp_conn_id='google_cloud_default',
    dag=dag,
)