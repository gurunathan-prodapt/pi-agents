"""
Airflow DAG: bereitstellung_basisprodukte_bert
Orchestrates MSISDN history preparation in BigQuery by resolving dynamic watermarks
and rebuilding target subscriber number records.
"""

from datetime import timedelta
from airflow import DAG
from airflow.providers.google.cloud.operators.bigquery import BigQueryInsertJobOperator
from airflow.utils.dates import days_ago

default_args = {
    "owner": "airflow",
    "depends_on_past": False, 
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    dag_id="bereitstellung_basisprodukte_bert",
    default_args=default_args,
    description="Orchestrates MSISDN history preparation in BigQuery",
    schedule_interval=None,
    start_date=days_ago(1),
    catchup=False,
    max_active_runs=1,
    template_searchpath=["/home/airflow/gcs/dags/queries"],
    tags=["bigquery", "bert", "msisdn"],
) as dag:

    # Execute the BigQuery job using the templated SQL script.
    # The wiederanlaufwert parameter is safely passed via queryParameters.
    execute_msisdn_history = BigQueryInsertJobOperator(
        task_id="execute_msisdn_history",
        configuration={
            "query": {
                "query": "{% include 'd_ausd_bp_ta_msisdn_his.sql' %}",
                "useLegacySql": False,
                "parameterMode": "NAMED",
                "queryParameters": [
                    {
                        "name": "wiederanlaufwert",
                        "parameterType": {"type": "INT64"},
                        "parameterValue": {
                            "value": "{{ dag_run.conf.get('wiederanlaufwert', 0) }}"
                        },
                    }
                ],
            }
        },
        gcp_conn_id="google_cloud_default",
    )

    execute_msisdn_history