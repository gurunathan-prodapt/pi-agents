"""
Airflow DAG orchestrating k_aurd_rechstan.ksh control flow.
Invokes BigQuery Stored Procedure r_aurd_rechstan_control with parameters.
"""

from datetime import datetime, timedelta
from airflow import DAG
from airflow.providers.google.cloud.operators.bigquery import BigQueryInsertJobOperator
from airflow.utils.dates import days_ago

# Target environment-specific settings
PROJECT_ID = "gcp-isbert-prod"
DATASET_ID = "isbert_aufbereitung"
CONNECTION_ID = "google_cloud_default"

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    dag_id="k_aurd_rechstan_dag",
    default_args=default_args,
    description="Orchestrates date validation, parameter checks and r_aurd_rechstan_control procedure execution",
    schedule_interval=None,
    start_date=days_ago(1),
    catchup=False,
    render_template_as_native_obj=True,
    tags=["migration", "bigquery", "isbert"],
) as dag:

    # Task to invoke the consolidated stored procedure in BigQuery
    run_control_proc = BigQueryInsertJobOperator(
        task_id="run_control_procedure",
        configuration={
            "query": {
                "query": f"""
                CALL `{PROJECT_ID}.{DATASET_ID}.r_aurd_rechstan_control`(
                  @job_kennung,
                  @eintrags_nr,
                  @stichtag,
                  @restart_value
                );
                """,
                "useLegacySql": False,
                "parameterMode": "NAMED",
                "queryParameters": [
                    {
                        "name": "job_kennung", 
                        "parameterType": {"type": "STRING"}, 
                        "parameterValue": {"value": "{{ dag_run.conf.get('job_kennung', 'JOB_DEFAULT') }}"}
                    },
                    {
                        "name": "eintrags_nr", 
                        "parameterType": {"type": "STRING"}, 
                        "parameterValue": {"value": "{{ dag_run.conf.get('eintrags_nr', '0000000') }}"}
                    },
                    {
                        "name": "stichtag", 
                        "parameterType": {"type": "STRING"}, 
                        "parameterValue": {"value": "{{ dag_run.conf.get('stichtag', ds_format(ds, '%Y-%m-%d', '%d%m%Y')) }}"}
                    },
                    {
                        "name": "restart_value", 
                        "parameterType": {"type": "STRING"}, 
                        "parameterValue": {"value": "{{ dag_run.conf.get('restart_value', '0') }}"}
                    },
                ],
            }
        },
        gcp_conn_id=CONNECTION_ID,
    )

    run_control_proc