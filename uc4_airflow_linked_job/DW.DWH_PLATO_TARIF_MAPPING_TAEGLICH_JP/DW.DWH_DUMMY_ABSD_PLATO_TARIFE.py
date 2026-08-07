"""
Migration wrapper for standalone UC4 dummy job DW.DWH_DUMMY_ABSD_PLATO_TARIFE.

This DAG represents a dummy utility or placeholder job converted from UC4.
Because it uses an unrecognized native UC4 scripting command (:print Doing nothinig),
it is implemented using a BashOperator to print the literal string.

Legacy documentation:
Wiederanlauf ohne weitere Maßnahmen möglich
"""

from datetime import datetime, timedelta
from airflow import DAG
from airflow.models import Variable
from airflow.operators.bash import BashOperator

# ─── GCP CONFIGURATION ────────────────────────────────────────────────────────
# Sourced dynamically from Airflow Variables to avoid hardcoded placeholders.
GCP_PROJECT = Variable.get("GCP_PROJECT")
GCP_REGION = Variable.get("GCP_REGION")

# ─── DEFAULT ARGUMENTS ────────────────────────────────────────────────────────
DEFAULT_ARGS = {
    'owner': 'airflow',
    'depends_on_past': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# ─── DAG DEFINITION ──────────────────────────────────────────────────────────
with DAG( 
    dag_id='dw_dwh_dummy_absd_plato_tarife',
    default_args=DEFAULT_ARGS,
    description='Migration wrapper for standalone UC4 dummy job DW.DWH_DUMMY_ABSD_PLATO_TARIFE',
    schedule=None,  # Externally triggered, no schedule defined in source UC4 object
    start_date=datetime(2023, 1, 1),
    catchup=False,
    max_active_runs=1,
    is_paused_upon_creation=False,  # Active (1) in UC4 export
    tags=['migrated_uc4', 'dummy_task'],
) as dag:

    # ─── TASKS ────────────────────────────────────────────────────────────────

    # dwh_dummy_absd_plato_tarife_task
    # Represents the legacy UC4 JOBS_UNIX object DW.DWH_DUMMY_ABSD_PLATO_TARIFE.
    # Originally executed the script statement: :print Doing nothinig
    # Preserves the exact print statement via BashOperator echo command.
    dwh_dummy_absd_plato_tarife_task = BashOperator(
        task_id='dwh_dummy_absd_plato_tarife_task',
        bash_command="echo 'Doing nothinig'",
    )

    # ─── DEPENDENCIES ─────────────────────────────────────────────────────────
    # Standalone single-task workflow. Downstream dependencies to 
    # DW.DWH_PLATO_TARIF_MAPPING_TAEGLICH_JP are currently unmigrated.
    dwh_dummy_absd_plato_tarife_task