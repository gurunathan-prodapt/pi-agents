from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.models import Variable

# Retrieve environment-wide global settings using Airflow Variable
gcp_project = Variable.get("GCP_PROJECT", default_var=None)
bq_dataset = Variable.get("BQ_DATASET", default_var="DWH_STG")

default_args = {
    'owner': 'dwh_admin',
    'depends_on_past': False,
    'start_date': datetime(2026, 1, 1),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    'dw_cfg_load_params',
    default_args=default_args,
    description='Orchestrate the loading of DWH parameters into BigQuery',
    schedule_interval=None,
    catchup=False,
) as dag:

    # Execute Python script replicating legacy KSH script
    execute_r_load_params = BashOperator(
        task_id='execute_r_load_params',
        bash_command='python3 /opt/dwh/config_env_linked_job/iscfg/bin/r_load_params.py',
        env={
            'DWH_HOME': '/opt/dwh',
            'DWH_LOG_DIR': '/opt/dwh/logs',
            'GCP_PROJECT': gcp_project or '',
            'BQ_DATASET': bq_dataset,
        }
    )