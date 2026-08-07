"""
DAG: dw_dwh_dummy_absd_plato_tarife_dag
Overview:
    This wrapper DAG represents the migrated UC4 UNIX job 'DW.DWH_DUMMY_ABSD_PLATO_TARIFE'.
    In the source UC4 system, this job performed no functional shell actions and only executed 
    an internal script statement (:print Doing nothinig). It is migrated here as an EmptyOperator 
    to preserve downstream synchronization readiness within the Airflow orchestrator.
"""

from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator

# Default arguments applied to the DAG and inherited by tasks
DEFAULT_ARGS = {
    "owner": "airflow",
    "depends_on_past": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

with DAG( 
    dag_id="dw_dwh_dummy_absd_plato_tarife_dag",
    default_args=DEFAULT_ARGS,
    description="Wrapper DAG for migrated UC4 object DW.DWH_DUMMY_ABSD_PLATO_TARIFE",
    schedule=None,
    start_date=datetime(2023, 1, 1),
    catchup=False,
    max_active_runs=1,
    is_paused_upon_creation=False,
    tags=["migrated_uc4", "dummy"],
) as dag:

    # REVIEW-STRUCT: launcher command not recognised during conversion — implement the correct operator once its behaviour is confirmed
    # The original UC4 command was: ':print Doing nothinig'
    dw_dwh_dummy_absd_plato_tarife = BashOperator(
        task_id="dw_dwh_dummy_absd_plato_tarife",
        bash_command="echo 'Doing nothinig'"
    )

    # Trivial execution flow for single-task DAG
    dw_dwh_dummy_absd_plato_tarife