from datetime import datetime
from airflow import DAG
from airflow.models import Variable
from airflow.operators.python import PythonOperator
from airflow.operators.empty import EmptyOperator

# Sourcing global infrastructure identifiers via Airflow Variable
GCP_PROJECT = Variable.get("GCP_PROJECT")
GCS_BUCKET = Variable.get("GCS_BUCKET")
BQ_DATASET = Variable.get("BQ_DATASET", default_var="DWH_ADM")

default_args = {
    "owner": "dw",
    "depends_on_past": False,
    "start_date": datetime(2026, 4, 21),
    "retries": 0,
}

dag = DAG(
    dag_id="dw_cfg_load_params_dag",
    default_args=default_args,
    description="Load DWH parameter file into staging - Migrated from DW.CFG_LOAD_PARAMS",
    schedule_interval=None,
    catchup=False,
    max_active_runs=1,
)

def run_parameter_load(**kwargs):
    # Dynamic runtime import of consolidated parameter loading module
    from config_env_linked_job.iscfg.bin import r_load_params
    r_load_params.load_parameters(
        job_kennung="AUSD_V_TA_PERIOD", 
        project_id=GCP_PROJECT, 
        bucket_name=GCS_BUCKET,
        dataset_name=BQ_DATASET
    )

start = EmptyOperator(task_id="start", dag=dag)

load_task = PythonOperator(
    task_id="r_load_params",
    python_callable=run_parameter_load,
    dag=dag,
)

end = EmptyOperator(task_id="end", dag=dag)

start >> load_task >> end