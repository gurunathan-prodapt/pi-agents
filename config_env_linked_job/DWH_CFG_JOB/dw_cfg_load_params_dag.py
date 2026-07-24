import os
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.providers.google.cloud.operators.bigquery import BigQueryInsertJobOperator
from airflow.models import Variable

default_args = {
    'owner': 'dwh',
    'start_date': datetime(2023, 1, 1),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# Retrieve global environment variables
GCP_PROJECT = Variable.get("GCP_PROJECT")
GCS_CONFIG_BUCKET = Variable.get("GCS_CONFIG_BUCKET")

with DAG(
    dag_id='dw_cfg_load_params_dag',
    default_args=default_args,
    schedule_interval='@daily',
    catchup=False,
    max_active_runs=1,
) as dag:

    # Task 1: Run Python logic to read parameters from dwh_env.properties and write to DWH_STG.PARAM_LOAD
    parse_and_stage_parameters = BashOperator(
        task_id='parse_and_stage_parameters',
        bash_command='python3 /home/airflow/gcs/dags/config_env_linked_job/iscfg/bin/r_load_params.py',
        env={
            'GCP_PROJECT': GCP_PROJECT,
            'GCS_CONFIG_BUCKET': GCS_CONFIG_BUCKET,
        }
    )

    # Task 2: Merge staged parameters into target table using native BigQuery execution
    target_table = f"`{GCP_PROJECT}.DWH_ADM.JOB_PARAMS`" if GCP_PROJECT else "`DWH_ADM.JOB_PARAMS`"
    source_table = f"`{GCP_PROJECT}.DWH_STG.PARAM_LOAD`" if GCP_PROJECT else "`DWH_STG.PARAM_LOAD`"

    merge_query = f"""
    BEGIN TRANSACTION;

    MERGE INTO {target_table} tgt
    USING (
        SELECT param_key, param_value, loaded_at
        FROM {source_table}
    ) src
    ON (tgt.param_key = src.param_key)
    WHEN MATCHED THEN UPDATE SET
        tgt.param_value = src.param_value,
        tgt.updated_at  = src.loaded_at
    WHEN NOT MATCHED THEN INSERT (param_key, param_value, updated_at)
    VALUES (src.param_key, src.param_value, src.loaded_at);

    COMMIT TRANSACTION;
    """

    merge_parameters = BigQueryInsertJobOperator(
        task_id='merge_parameters',
        configuration={
            "query": {
                "query": merge_query,
                "useLegacySql": False,
            }
        }
    )

    parse_and_stage_parameters >> merge_parameters