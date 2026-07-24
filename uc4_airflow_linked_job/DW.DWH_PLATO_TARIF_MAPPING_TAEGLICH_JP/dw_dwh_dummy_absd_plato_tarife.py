"""
DAG: dw_dwh_dummy_absd_plato_tarife
Description: Migration of UC4 Job `DW.DWH_DUMMY_ABSD_PLATO_TARIFE`.
             Executes the legacy placeholder step by printing the exact verbatim message.
Pattern: UC4_ONLY
"""

import logging
from datetime import datetime
from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator
from airflow.models import Variable

# ==============================================================================
# GLOBAL ENVIRONMENT VARIABLE SOURCING
# ==============================================================================
# All variables are retrieved dynamically from Airflow's variable store.
# No prose placeholders are utilized.
GCP_PROJECT = Variable.get("GCP_PROJECT", default_var=None)
GCP_REGION = Variable.get("GCP_REGION", default_var=None)

# ==============================================================================
# DEFAULT ARGUMENTS
# ==============================================================================
# Retries set to 0 to mirror the legacy behavior where failure is unrecovered.
DEFAULT_ARGS = {
    "owner": "airflow",
    "depends_on_past": False,
    "start_date": datetime(2026, 3, 30),
    "retries": 0,
}

# ==============================================================================
# LEGACY PRINT EXECUTION (PRESERVED VERBATIM)
# ==============================================================================
def execute_legacy_print():
    """
    OUTPUT/PRINT LITERAL RULE COMPLIANCE:
    Legacy UC4 object printed: "Doing nothinig" (including typo).
    This text is preserved verbatim without alteration.
    """
    logging.info("Doing nothinig")

# ==============================================================================
# DAG DEFINITION
# ==============================================================================
with DAG(
    dag_id="dw_dwh_dummy_absd_plato_tarife",
    default_args=DEFAULT_ARGS,
    schedule=None,  # No calendar schedule is provided in legacy file
    catchup=False,
    max_active_runs=1,
    is_paused_upon_creation=False,
    doc_md="""
    ### Workflow Description
    Migration of UC4 Job `DW.DWH_DUMMY_ABSD_PLATO_TARIFE`.
    This job is a dummy/no-op sync job which prints "Doing nothinig" (verbatim from legacy).
    """
) as dag:

    start_boundary = EmptyOperator(
        task_id="start"
    )

    dw_dwh_dummy_absd_plato_tarife = PythonOperator(
        task_id="dw_dwh_dummy_absd_plato_tarife",
        python_callable=execute_legacy_print
    )

    end_boundary = EmptyOperator(
        task_id="end"
    )

    # Task Execution Sequence
    start_boundary >> dw_dwh_dummy_absd_plato_tarife >> end_boundary