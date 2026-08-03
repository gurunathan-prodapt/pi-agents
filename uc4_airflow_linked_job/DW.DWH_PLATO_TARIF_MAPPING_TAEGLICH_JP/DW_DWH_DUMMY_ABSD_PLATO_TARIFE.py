"""
DAG: dw_dwh_dummy_absd_plato_tarife
Converted from UC4 JOBS_UNIX object DW.DWH_DUMMY_ABSD_PLATO_TARIFE.

This DAG represents a dummy synchronization or placeholder workflow. The original
UC4 task performed no OS-level script execution, using only a native UC4 scripting 
command (:print Doing nothinig). It is converted here to a single-task DAG containing 
a BashOperator to explicitly print the literal string to preserve the source output.
"""

from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator

# Default arguments mapped from UC4 parameters
DEFAULT_ARGS = { 
    'owner': 'dw.unix.istns',
    'depends_on_past': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    dag_id='dw_dwh_dummy_absd_plato_tarife',
    default_args=DEFAULT_ARGS,
    description='Converted from UC4 JOBS_UNIX object DW.DWH_DUMMY_ABSD_PLATO_TARIFE (dummy)',
    schedule=None,
    start_date=datetime(2023, 1, 1),
    catchup=False,
    max_active_runs=1,
    is_paused_upon_creation=False,
) as dag:

    # ── Task: dw_dwh_dummy_absd_plato_tarife ───────────────────────────────
    # Converted to BashOperator to explicitly print the literal string 'Doing nothinig' to preserve the source script's output.
    dw_dwh_dummy_absd_plato_tarife = BashOperator(
        task_id='dw_dwh_dummy_absd_plato_tarife',
        bash_command='echo "Doing nothinig"',
    )

    dw_dwh_dummy_absd_plato_tarife