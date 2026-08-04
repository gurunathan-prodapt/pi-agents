import datetime
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.models import Variable

# Global Config - Sourced from Airflow Variables with legacy-aligned fallback
CRM_HOME = Variable.get("CRM_HOME", default_var="/opt/etl/customer")

default_args = {
    "owner": "airflow",
    "start_date": datetime.datetime(2023, 1, 1),
    "retries": 1,
}

with DAG(
    dag_id="CUSTOMER_HISTORIZATION_LOAD_dag",
    default_args=default_args,
    schedule_interval=None,  # Modelled as callable unit with no standalone schedule
    catchup=False,
    tags=["customer"],
) as dag:

    # Task to run customer/r_historization_load.py wrapper script
    run_wrapper = BashOperator(
        task_id="run_historization_load_wrapper",
        bash_command=f"python3 {CRM_HOME}/customer/r_historization_load.py",
        env={
            "CRM_HOME": CRM_HOME,
            "RUN_DATE": "{{ ds }}",
        },
    )

    run_wrapper