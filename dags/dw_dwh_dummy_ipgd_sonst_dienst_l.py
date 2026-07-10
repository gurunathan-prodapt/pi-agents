from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.empty import EmptyOperator


def build_default_args() -> dict:
    """
    Build reusable default arguments for the DAG.
    """
    return {
        "owner": "uc4_migration",
        "retries": 0,
        "retry_delay": timedelta(minutes=0),
        "start_date": datetime(2023, 1, 1),
    }


def build_dag_doc() -> str:
    """
    Return operational documentation for the migrated UC4 job.
    """
    return """
    ### Operational Note
    Legacy Job: DW.DWH_DUMMY_IPGD_SONST_DIENST_L

    Documentation:
    "kann nicht ohne weitere Arbeiten erneut ausgefuehrt werden"
    (Cannot be executed again without further manual work).

    This DAG intentionally uses a lightweight task because the source job
    only prints a dummy message (`:print nix`) and does not represent a
    production workload.
    """


def create_dummy_task() -> BashOperator:
    """
    Create the lightweight task that mirrors the dummy UC4 behavior.
    """
    return BashOperator( 
        task_id="run_dw_dwh_dummy_ipgd_sonst_dienst_l",
        bash_command="echo 'nix'",
    )


def create_start_task() -> EmptyOperator:
    """
    Create a reusable start marker task.
    """
    return EmptyOperator(task_id="start")


def create_end_task() -> EmptyOperator:
    """
    Create a reusable end marker task.
    """
    return EmptyOperator(task_id="end")


default_args = build_default_args()

with DAG(
    dag_id="dw_dwh_dummy_ipgd_sonst_dienst_l",
    schedule=None,
    catchup=False,
    max_active_runs=1,
    is_paused_upon_creation=False,
    default_args=default_args,
    doc_md=build_dag_doc(),
    tags=["uc4_migration", "dummy", "legacy"],
) as dag:
    start = create_start_task()
    run_dw_dwh_dummy_ipgd_sonst_dienst_l = create_dummy_task()
    end = create_end_task()

    start >> run_dw_dwh_dummy_ipgd_sonst_dienst_l >> end