"""
DAG: dw_dwh_dummy_absd_plato_tarife

This DAG is migrated from the UC4 JOBS_UNIX object 'DW.DWH_DUMMY_ABSD_PLATO_TARIFE'.
In UC4, this represents a utility task with a title of 'dummy' and script body ':print Doing nothinig'.
Because no parent JOBP workflow or trigger schedule was included in the source extraction,
it is implemented here as a standalone single-task DAG, default-configured to run manually (schedule=None).
"""

from datetime import datetime, timedelta
from airflow import DAG
from airflow.models import Variable
from airflow.operators.bash import BashOperator

# GCP Configuration (retrieved from environment-wide configuration variables, no hardcoded placeholders)
GCP_PROJECT_ID = Variable.get("GCP_PROJECT", default_var=None)
GCP_REGION = Variable.get("GCP_REGION", default_var=None)

DEFAULT_ARGS = {
    "owner": "airflow",
    "depends_on_past": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    dag_id="dw_dwh_dummy_absd_plato_tarife",
    default_args=DEFAULT_ARGS,
    description="Converted dummy task from UC4 DW.DWH_DUMMY_ABSD_PLATO_TARIFE",
    schedule=None,
    start_date=datetime(2023, 1, 1),
    catchup=False,
    max_active_runs=1,
    is_paused_upon_creation=False,
) as dag:

    # dw_dwh_dummy_absd_plato_tarife_task represents the original JOBS_UNIX object.
    # It performs no functional work other than acting as an orchestration placeholder.
    # Original legacy script printed: "Doing nothinig"
    dw_dwh_dummy_absd_plato_tarife_task = BashOperator(
        task_id="dw_dwh_dummy_absd_plato_tarife_task",
        bash_command="echo 'Doing nothinig'",
    )

    # Single task DAG; no dependency wiring needed
    dw_dwh_dummy_absd_plato_tarife_task