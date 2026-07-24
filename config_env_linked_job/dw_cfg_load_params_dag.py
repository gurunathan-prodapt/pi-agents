from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.models import Variable

# Retrieve environment-wide variables from Airflow variables config store
GCP_PROJECT = Variable.get("GCP_PROJECT")
GCS_BUCKET = Variable.get("GCS_BUCKET")
DWH_HOME = Variable.get("DWH_HOME", default_var="/home/gurunathan_t/tool_mapping_samples")
DWH_LOG_DIR = Variable.get("DWH_LOG_DIR", default_var="/var/log/dwh")

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "start_date": datetime(2026, 1, 1),
    "email_on_failure": True,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    "dw_cfg_load_params",
    default_args=default_args,
    description="Loads environment configurations and merges them into Job Parameters in BigQuery",
    schedule_interval=None,  # Triggered manually or by upstream task
    catchup=False,
    tags=["dwh", "config"],
) as dag:

    # Executes the unified Python script mimicking the original KornShell logic flow
    execute_load_params = BashOperator(
        task_id="r_load_params",
        bash_command=f"python3 {DWH_HOME}/config_env_linked_job/iscfg/bin/r_load_params.py",
        env={
            "GCP_PROJECT": GCP_PROJECT,
            "GCS_BUCKET": GCS_BUCKET,
            "DWH_HOME": DWH_HOME,
            "DWH_LOG_DIR": DWH_LOG_DIR,
            "BQ_DATASET": "DW_STG",
            "BQ_LOCATION": "EU"
        }
    )

    execute_load_params